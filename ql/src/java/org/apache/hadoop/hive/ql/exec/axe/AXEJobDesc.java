package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;

import com.google.common.base.Preconditions;

import jline.internal.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

class AXEJobDesc {
  final public Output output;
  protected final Logger LOG = LoggerFactory.getLogger(AXETask.class);
  private final Map<String, Integer> taskNameToId;
  private final Map<String, Integer> tableNameToId;
  String currentStageName;
  private int counter;

  AXEJobDesc() {
    output = new Output();
    taskNameToId = new HashMap<>();
    tableNameToId = new HashMap<>();
    counter = 0;
  }

  AXEOperator addMapTask(String taskName, Map.Entry<String, Operator<? extends OperatorDesc>> aliasOp,
      final Map<Path, PartitionDesc> pathPartitionDescMap) {
    // TODO(tatiana): check for bucket pruning
    TableScanOperator ts = (TableScanOperator) aliasOp.getValue();
    Map<String, Integer> inputColIndex = new HashMap<>();
    int tableId = addSrcTable(ts, inputColIndex, pathPartitionDescMap, aliasOp.getKey());
    AXETableScanOperator tsOp = addTableScanOperator(ts, tableId);
    List<Operator<? extends OperatorDesc>> next = tryPushChildrenIntoTableScan(tsOp, ts.getChildOperators(),
                                                                               inputColIndex);
    addNext(tsOp, next, inputColIndex);
    taskNameToId.put(taskName, tsOp.id);
    return tsOp;
  }

  private List<Operator<? extends OperatorDesc>> tryPushChildrenIntoTableScan(final AXETableScanOperator tsOp,
      final List<Operator<? extends OperatorDesc>> children, final Map<String, Integer> inputColIndex) {
    // TODO(tatiana): check for case like
    //          TS
    //         /  \
    //        FIL  \
    //         \   /
    //         JOIN
    //  Will the children size be 1 but TS output is consumed in the next task?
    if (children.size() == 1) {
      Operator<? extends OperatorDesc> operator = children.get(0);
      if (operator instanceof FilterOperator) {
        tsOp.addFilterDesc(new AXEExpression(((FilterOperator) operator).getConf().getPredicate()));
      } else if (operator instanceof SelectOperator) {
        addSelect(tsOp, (SelectOperator) operator);
        int projectSize = tsOp.projectCols.size();
        final List<String> outputColumnNames = ((SelectOperator) operator).getConf().getOutputColumnNames();
        Preconditions.checkArgument(outputColumnNames.size() == projectSize,
                                    "Output column size does not match project column size");
      } else {
        return children;
      }
      return tryPushChildrenIntoTableScan(tsOp, operator.getChildOperators(), inputColIndex);
    } else if (children.size() > 1) {
      LOG.warn("MapWork not a chain, with " + children.size() + " branches!");
    }
    return children;
  }

  private AXEReduceSinkOperator addReduceSinkOperator(final ReduceSinkOperator operator,
      final Map<String, Integer> inputColIndex) {
    AXEReduceSinkOperator rsOp = new AXEReduceSinkOperator(addTask(operator.getName()));

    rsOp.reduceOutputName = operator.getReduceOutputName();
    rsOp.reduceKeyColumnNames = operator.getConf().getOutputKeyColumnNames();
    rsOp.reduceValueColumnNames = operator.getConf().getOutputValueColumnNames(); // e.g. [_col0, _col1]
    // TODO(tatiana): do not understand what this is: int[] valueIdx = operator.getValueIndex();
    rsOp.numReducers = operator.getConf().getNumReducers();
    rsOp.setReduceKey(operator.getConf().getKeyCols(), inputColIndex);
    rsOp.setReduceValue(operator.getConf().getValueCols(), inputColIndex);
    rsOp.setSortOrder(operator.getConf().getOrder());
    rsOp.setHasOrderBy(operator.getConf().hasOrderBy());
    rsOp.setTopN(operator.getConf().getTopN());
    if (!operator.getConf().getPartitionCols().equals(operator.getConf().getKeyCols())) {
      rsOp.setPartitionKey(operator.getConf().getPartitionCols(), inputColIndex);
    }

    output.reduceSinkOperators.add(rsOp);
    return rsOp;
  }

  // TODO(tatiana): handle vectorization
  private void addSelect(final AXESelectOperator op, final SelectOperator operator) {
    op.setProjectCols(operator.getConf().getColList());
    op.outputColumnNames = operator.getConf().getOutputColumnNames();
  }

  AXEOperator addReduceTask(final String taskName, final Operator<?> reducer) {
    Map<String, Integer> inputColIndex = new HashMap<>();
    AXEOperator reduceStart = addOperator(reducer, inputColIndex);
    addNext(reduceStart, reducer.getChildOperators(), inputColIndex);
    taskNameToId.put(taskName, reduceStart.id);
    return reduceStart;
  }

  private void addNext(AXEOperator op, final List<Operator<? extends OperatorDesc>> childOperators,
      final Map<String, Integer> inputColIndex) {
    if (childOperators == null || childOperators.isEmpty()) {
      if (!((op instanceof AXEReduceSinkOperator) || (op instanceof AXEFileSinkOperator))) {
        LOG.warn("The tail of work is not RS or FS: " + op.getClass().getName());
      }
      return;
    } else if (childOperators.size() > 1) {
      LOG.warn("addNext: current work not a chain, with " + childOperators.size() + " branches!");
    }
    for (Operator<? extends OperatorDesc> childOperator : childOperators) {
      AXEOperator childOp = addOperator(childOperator, inputColIndex);
      addNext(childOp, childOperator.getChildOperators(), inputColIndex);
      op.addImmediateChild(childOp.id);
      childOp.addImmediateParent(op.id);
    }
  }

  private AXEOperator addOperator(final Operator<? extends OperatorDesc> operator, Map<String, Integer> inputColIndex) {
    AXEOperator op;
    if (operator instanceof LimitOperator) {
      op = addLimitOperator((LimitOperator) operator);
    } else if (operator instanceof FileSinkOperator) {
      op = addFileSinkOperator((FileSinkOperator) operator);
    } else if (operator instanceof GroupByOperator) {
      op = addGroupByOperator((GroupByOperator) operator);
    } else if (operator instanceof SelectOperator) {
      op = addSelectOperator((SelectOperator) operator);
    } else if (operator instanceof ReduceSinkOperator) {
      op = addReduceSinkOperator((ReduceSinkOperator) operator, inputColIndex);
    } else if (operator instanceof JoinOperator) {
      op = addJoinOperator((JoinOperator) operator);
    } else if (operator instanceof FilterOperator) {
      op = addFilterOperator((FilterOperator) operator);
    } else if (operator instanceof AXEHashTableSinkOperator) {
      op = addHashTableSinkOperator((AXEHashTableSinkOperator) operator);
    } else if (operator instanceof MapJoinOperator) {
      op = addMapJoinOperator((MapJoinOperator) operator);
    } else if (operator instanceof PTFOperator) {
      op = addPTFOperator((PTFOperator) operator);
    } else {
      throw new IllegalStateException(
          "[AXEJobDesc.addOperator] Unsupported child operator " + operator.getClass().getName());
    }
    return op;
  }

  private AXEOperator addPTFOperator(final PTFOperator operator) {
    AXEPTFOperator ptfOp = new AXEPTFOperator(addTask(operator.getName()));
    ptfOp.initialize(operator.getConf());
    output.ptfOperators.add(ptfOp);
    return ptfOp;
  }

  private AXEOperator addMapJoinOperator(final MapJoinOperator operator) {
    AXEMapJoinOperator mjOp = new AXEMapJoinOperator(addTask(operator.getName()));
    mjOp.initialize(operator.getConf());
    output.mapJoinOperators.add(mjOp);
    return mjOp;
  }

  private AXEOperator addHashTableSinkOperator(final AXEHashTableSinkOperator operator) {
    AXEHTSOperator htsOp = new AXEHTSOperator(addTask(operator.getName()));
    htsOp.initialize(operator.getConf());
    htsOp.stageName = currentStageName;
    output.hashTableSinkOperators.add(htsOp);
    return htsOp;
  }

  private AXEOperator addFilterOperator(final FilterOperator operator) {
    AXEFilterOperator filterOp = new AXEFilterOperator(addTask(operator.getName()));
    filterOp.addFilterDesc(new AXEExpression(operator.getConf().getPredicate()));
    output.filterOperators.add(filterOp);
    return filterOp;
  }

  private AXEJoinOperator addJoinOperator(final JoinOperator operator) { //, final Map<String, Integer> inputColIndex
    AXEJoinOperator joinOp = new AXEJoinOperator(addTask(operator.getName()));
    joinOp.tagOrder = operator.getConf().getTagOrder();
    joinOp.outputColumnNames = operator.getConf().getOutputColumnNames();
    joinOp.setJoinValueExprs(operator.getConf().getExprs());
    joinOp.setJoinKeys(operator.getConf().getJoinKeys());
    joinOp.setJoinConditions(operator.getConf().getConds());
    output.joinOperators.add(joinOp);
    return joinOp;
  }

  private AXEGroupByOperator addGroupByOperator(final GroupByOperator groupByOperator) {
    AXEGroupByOperator gbOp = new AXEGroupByOperator(addTask(groupByOperator.getName()));
    GroupByDesc groupByDesc = groupByOperator.getConf();
    gbOp.outputColumnNames = groupByOperator.getConf().getOutputColumnNames();
    gbOp.setAggregatorKeys(groupByDesc.getKeys());
    gbOp.setAggregators(groupByDesc.getAggregators());
    for (AggregationDesc aggregationDesc : groupByDesc.getAggregators()) {
      if (aggregationDesc.getDistinct() && groupByDesc.getMode() == GroupByDesc.Mode.HASH) {
        // the aggregator parameters should also appear in aggregator key for hash mode
        boolean assumptionNotTrue = false;
        for (ExprNodeDesc parameterDesc : aggregationDesc.getParameters()) {
          boolean inKey = false;
          for (ExprNodeDesc keyDesc : groupByDesc.getKeys()) {
            if (keyDesc == parameterDesc) {
              inKey = true;
              break;
            }
          }
          if (!inKey) {
            assumptionNotTrue = true;
            break;
          }
        }
        if (assumptionNotTrue) {
          Log.error("Distinct agg parameter not in agg key for hash mode" + aggregationDesc.getExprString());
        }
      }
    }
    gbOp.setMode(groupByDesc.getMode().name());
    gbOp.setBucketGroup(groupByDesc.getBucketGroup());
    output.groupByOperators.add(gbOp);
    return gbOp;
  }

  private AXELimitOperator addLimitOperator(final LimitOperator limitOperator) {
    LimitDesc limitDesc = limitOperator.getConf();
    AXELimitOperator limitOp = new AXELimitOperator(addTask(limitOperator.getName()), limitDesc.getLimit(),
                                                    limitDesc.getLeastRows(), limitDesc.getOffset());
    output.limitOperators.add(limitOp);
    return limitOp;
  }

  private AXEFileSinkOperator addFileSinkOperator(final FileSinkOperator fileSinkOperator) {
    AXEFileSinkOperator fsOp = new AXEFileSinkOperator(addTask(fileSinkOperator.getName()));
    FileSinkDesc fileSinkDesc = fileSinkOperator.getConf();
    fsOp.destPath = fileSinkDesc.getDestPath().toString();
    // int destTableId = fileSinkDesc.getDestTableId();
    output.fileSinkOperators.add(fsOp);
    return fsOp;
  }

  private int addSrcTable(TableScanOperator ts, Map<String, Integer> inputColIndex,
      final Map<Path, PartitionDesc> pathPartitionDescMap, final String alias) {
    AXETable axeTable = new AXETable(alias);
    Table table = ts.getConf().getTableMetadata();
    if (pathPartitionDescMap.size() == 0) {
      LOG.warn("No partition given for table " + alias + ". Using the whole table now.");
      Preconditions.checkArgument(table != null, "No partition given for table, and null table metadata");
      axeTable.addPath(Objects.requireNonNull(table.getPath()));
      axeTable.setSchema(table.getAllCols());
      if (table.getInputFormatClass() == OrcInputFormat.class
          && table.getOutputFormatClass() == OrcOutputFormat.class) {
        axeTable.inputFormat = "ORC";
      } else {
        throw new IllegalArgumentException(
            "The inputFormat of table is expected to be ORC, but was " + table.getInputFormatClass());
      }
    } else {
      PartitionDesc pd = pathPartitionDescMap.entrySet().iterator().next().getValue();
      axeTable.addPaths(pathPartitionDescMap);
      if (table != null) {
        axeTable.setSchema(table.getAllCols());
      } else {
        RowSchema rowSchema = ts.getSchema();
        Preconditions.checkArgument(rowSchema != null, "Table metadata and row schema are both null");
        axeTable.setSchema(rowSchema);
      }
      if (pd.getInputFileFormatClass() == OrcInputFormat.class
          && pd.getOutputFileFormatClass() == OrcOutputFormat.class) {
        axeTable.inputFormat = "ORC";
      } else {
        throw new IllegalArgumentException(
            "The inputFormat of table is expected to be ORC, but was " + pd.getInputFileFormatClass());
      }
    }
    int nColumns = ts.getNeededColumnIDs().size();
    for (int i = 0; i < nColumns; ++i) {
      inputColIndex.put(ts.getNeededColumns().get(i), ts.getNeededColumnIDs().get(i));
    }

    // Add to Json output
    int tableId = output.srcTables.size();
    tableNameToId.put(alias, tableId);
    output.srcTables.add(axeTable);
    return tableId;
  }

  private int addTask(String taskName) {
    int taskId = counter++;
    taskNameToId.put(taskName, taskId);
    return taskId;
  }

  private AXESelectOperator addSelectOperator(SelectOperator op) {
    AXESelectOperator selectOp = new AXESelectOperator(addTask(op.getName()));
    addSelect(selectOp, op);
    output.selectOperators.add(selectOp);
    // TODO(tatiana): translate to index?
    return selectOp;
  }

  private AXETableScanOperator addTableScanOperator(TableScanOperator op, int tableId) {
    AXETableScanOperator tsOp = new AXETableScanOperator(addTask(op.getName()), op, tableId);
    if (op.getConf() != null) {
      tsOp.outputColumnNames = op.getConf().getOutputColumnNames();
    }
    output.tableScanOperators.add(tsOp);
    return tsOp;
  }

  public int getTaskId(final String name) {
    return taskNameToId.get(name);
  }

  static class Output {
    final List<AXEPTFOperator> ptfOperators = new ArrayList<>();
    final List<AXEFileSinkOperator> fileSinkOperators = new ArrayList<>();
    final List<AXEReduceSinkOperator> reduceSinkOperators = new ArrayList<>();
    final List<AXETable> srcTables = new ArrayList<>();
    final List<AXETableScanOperator> tableScanOperators = new ArrayList<>();
    final List<AXESelectOperator> selectOperators = new ArrayList<>();
    final List<AXELimitOperator> limitOperators = new ArrayList<>();
    final List<AXEGroupByOperator> groupByOperators = new ArrayList<>();
    final List<AXEJoinOperator> joinOperators = new ArrayList<>();
    final List<AXEFilterOperator> filterOperators = new ArrayList<>();
    final List<AXEHTSOperator> hashTableSinkOperators = new ArrayList<>();
    final List<AXEMapJoinOperator> mapJoinOperators = new ArrayList<>();

    Output() {}
  }

}
