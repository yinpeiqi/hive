package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

class AXEJobDesc {
  final public Output output;
  private final Map<String, Integer> taskNameToId;
  private final Map<String, Integer> tableNameToId;
  private int counter;

  AXEJobDesc() {
    output = new Output();
    taskNameToId = new HashMap<>();
    tableNameToId = new HashMap<>();
    counter = 0;
  }

  AXEOperator addMapTask(String taskName, TableScanOperator ts) {
    // TODO(tatiana): check for bucket pruning
    Map<String, Integer> inputColIndex = new HashMap<>();
    int tableId = addSrcTable(ts.getConf().getTableMetadata(), inputColIndex);
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
        tsOp.addFilterDesc(new AXEExpression(((FilterOperator) operator).getConf().getPredicate(), inputColIndex));
      } else if (operator instanceof SelectOperator) {
        addSelect(tsOp, (SelectOperator) operator);
        tsOp.translateColNameToIndex(inputColIndex);
        int projectSize = tsOp.projectCols.size();
        final List<String> outputColumnNames = ((SelectOperator) operator).getConf().getOutputColumnNames();
        Preconditions.checkArgument(outputColumnNames.size() == projectSize,
                                    "Output column size does not match project column size");
        for (int i = 0; i < projectSize; ++i) {
          inputColIndex.put(outputColumnNames.get(i), tsOp.projectCols.get(i));
        }
      } else {
        return children;
      }
      return tryPushChildrenIntoTableScan(tsOp, operator.getChildOperators(), inputColIndex);
    }
    return children;
  }

  private AXEReduceSinkOperator addReduceSinkOperator(final ReduceSinkOperator operator,
      final Map<String, Integer> inputColIndex) {
    AXEReduceSinkOperator rsOp = new AXEReduceSinkOperator(addTask(operator.getName()));

    // String reduceOutputName = operator.getReduceOutputName();
    // FIXME: match cols, e.g. [reducesinkkey0, reducesinkkey1]
    // List<String> keyColNames = operator.getConf().getOutputKeyColumnNames();
    // List<String> valueColNames = operator.getConf().getOutputValueColumnNames(); // e.g. [_col0, _col1]
    // TODO(tatiana): do not understand what this is: int[] valueIdx = operator.getValueIndex();
    rsOp.setReduceKey(operator.getConf().getKeyCols(), inputColIndex);
    rsOp.setReduceValue(operator.getConf().getValueCols(), inputColIndex);
    rsOp.setSortOrder(operator.getConf().getOrder());
    if (!operator.getConf().getPartitionCols().equals(operator.getConf().getKeyCols())) {
      rsOp.setPartitionKey(operator.getConf().getPartitionCols(), inputColIndex);
    }

    output.reduceSinkOperators.add(rsOp);
    return rsOp;
  }

  // TODO(tatiana): handle vectorization
  private void addSelect(final AXESelectOperator op, final SelectOperator operator) {
    op.setProjectCols(operator.getConf().getColList());
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
      return;
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
    } else {
      throw new IllegalStateException("Unsupported child operator " + operator.getClass().getName());
    }
    return op;
  }

  private AXEJoinOperator addJoinOperator(final JoinOperator operator) { //, final Map<String, Integer> inputColIndex
    AXEJoinOperator joinOp = new AXEJoinOperator(addTask(operator.getName()));
    joinOp.setJoinKeys(operator.getConf().getJoinKeys());
    joinOp.setJoinConditions(operator.getConf().getConds());
    output.joinOperators.add(joinOp);
    return joinOp;
  }

  private AXEGroupByOperator addGroupByOperator(final GroupByOperator groupByOperator) {
    AXEGroupByOperator gbOp = new AXEGroupByOperator(addTask(groupByOperator.getName()));
    GroupByDesc groupByDesc = groupByOperator.getConf();
    gbOp.setAggregatorKeys(groupByDesc.getKeys());
    gbOp.setAggregators(groupByDesc.getAggregators());
    gbOp.setMode(groupByDesc.getMode().name());
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

  private int addSrcTable(Table table, Map<String, Integer> inputColIndex) {
    // Construct AXETable
    AXETable axeTable = new AXETable(table.getTableName());
    axeTable.hdfsUrl = Objects.requireNonNull(table.getPath()).toString();
    if (table.getInputFormatClass() == OrcInputFormat.class && table.getOutputFormatClass() == OrcOutputFormat.class) {
      axeTable.inputFormat = "ORC";
    } else {
      throw new IllegalArgumentException(
          "The inputFormat of table is expected to be ORC, but was " + table.getInputFormatClass());
    }
    axeTable.setSchema(table.getAllCols());

    // Update column map
    int i = 0;
    for (AXETable.Field col : axeTable.schema) {
      inputColIndex.put(col.name, i++);
    }

    // Add to Json output
    int tableId = output.srcTables.size();
    tableNameToId.put(table.getTableName(), tableId);
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
    output.tableScanOperators.add(tsOp);
    return tsOp;
  }

  public int getTaskId(final String name) {
    return taskNameToId.get(name);
  }

  public class Output {
    List<AXEFileSinkOperator> fileSinkOperators;
    List<AXEReduceSinkOperator> reduceSinkOperators;
    List<AXETable> srcTables;
    List<AXETableScanOperator> tableScanOperators;
    List<AXESelectOperator> selectOperators;
    List<AXELimitOperator> limitOperators;
    List<AXEGroupByOperator> groupByOperators;
    List<AXEJoinOperator> joinOperators;

    Output() {
      srcTables = new ArrayList<>();
      tableScanOperators = new ArrayList<>();
      selectOperators = new ArrayList<>();
      limitOperators = new ArrayList<>();
      fileSinkOperators = new ArrayList<>();
      groupByOperators = new ArrayList<>();
      joinOperators = new ArrayList<>();
      reduceSinkOperators = new ArrayList<>();
    }
  }

}
