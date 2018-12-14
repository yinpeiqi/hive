package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Table;
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

  void addMapTask(String taskName, TableScanOperator ts) {
    Map<String, Integer> inputColIndex = new HashMap<>();
    int tableId = addSrcTable(ts.getConf().getTableMetadata(), inputColIndex);
    AXETableScanOperator tsOp = addTableScanOperator(ts, tableId);
    List<Operator<? extends OperatorDesc>> next = tryPushChildrenIntoTableScan(tsOp, ts.getChildOperators(),
                                                                               inputColIndex);
    if (next != null && !next.isEmpty()) {
      for (Operator<? extends OperatorDesc> child : next) {
        tsOp.addChild(addTask(child.getName()));
        // TODO(tatiana): add child tasks to json
      }
      throw new IllegalStateException("Multi-branch map task is not supported yet");
    }
    taskNameToId.put(taskName, tsOp.id);
  }

  private List<Operator<? extends OperatorDesc>> tryPushChildrenIntoTableScan(final AXETableScanOperator tsOp,
      final List<Operator<? extends OperatorDesc>> children, final Map<String, Integer> inputColIndex) {
    // TODO(tatiana): check for case like
    //          TS
    //         /  \
    //        FIL  \
    //         \   /
    //         JOIN
    // Will the children size be 1 but TS output is consumed in the next task?
    if (children.size() == 1) {
      Operator<? extends OperatorDesc> operator = children.get(0);
      if (operator instanceof FilterOperator) {
        pushFilterIntoTableScan(tsOp, (FilterOperator) operator, inputColIndex);
      } else if (operator instanceof SelectOperator) {
        pushSelectIntoTableScan(tsOp, (SelectOperator) operator, inputColIndex);
      } else if (operator instanceof ReduceSinkOperator) {
        pushReduceSinkIntoTableScan(tsOp, (ReduceSinkOperator) operator, inputColIndex);
      } else {
        return children;
      }
      return tryPushChildrenIntoTableScan(tsOp, operator.getChildOperators(), inputColIndex);
    }
    return children;
  }

  private void pushReduceSinkIntoTableScan(final AXETableScanOperator tsOp, final ReduceSinkOperator operator,
      final Map<String, Integer> inputColIndex) {
    String reduceOutputName = operator.getReduceOutputName(); // FIXME(tatiana): e.g. Reducer 2, could be used for resolving dependencies
    // TODO(tatiana): do not understand what this is: int[] valueIdx = operator.getValueIndex();
    tsOp.setReduceKey(operator.getConf().getKeyCols(), inputColIndex);
    tsOp.setReduceValue(operator.getConf().getValueCols(), inputColIndex);
    tsOp.setSortOrder(operator.getConf().getOrder());
    if (!operator.getConf().getPartitionCols().equals(operator.getConf().getKeyCols())) {
      tsOp.setPartitionKey(operator.getConf().getPartitionCols(), inputColIndex);
    }
  }

  private void pushSelectIntoTableScan(final AXETableScanOperator tsOp, final SelectOperator operator,
      final Map<String, Integer> inputColIndex) {
    if (!operator.getConf().isSelectStar()) {
      tsOp.setProjectCols(operator.getConf().getColList(), inputColIndex);
    }
    int projectSize = tsOp.projectCols.size();
    final List<String> outputColumnNames = operator.getConf().getOutputColumnNames();
    Preconditions.checkArgument(outputColumnNames.size() == projectSize,
                                "Output column size does not match project column size");
    for (int i = 0; i < projectSize; ++i) {
      inputColIndex.put(outputColumnNames.get(i), tsOp.projectCols.get(i));
    }
    // TODO: handle vectorization: operator.getConf().getSelectVectorization();
  }

  private void pushFilterIntoTableScan(final AXETableScanOperator tsOp, final FilterOperator operator,
      final Map<String, Integer> inputColIndex) {
    tsOp.addFilterDesc(new AXEExpression(operator.getConf().getPredicate(), inputColIndex));
  }

  void addReduceTask(final String taskName, final Operator<?> reducer) {
    addTask(taskName);
    System.out.println(taskName + "starts with " + reducer.getClass().getName());
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

  private AXETableScanOperator addTableScanOperator(TableScanOperator op, int tableId) {
    AXETableScanOperator tsOp = new AXETableScanOperator(addTask(op.getName()), op, tableId);
    output.tableScanOperators.add(tsOp);
    return tsOp;
  }

  public class Output {
    List<AXETable> srcTables;
    List<AXETableScanOperator> tableScanOperators;

    Output() {
      srcTables = new ArrayList<>();
      tableScanOperators = new ArrayList<>();
    }
  }

}
