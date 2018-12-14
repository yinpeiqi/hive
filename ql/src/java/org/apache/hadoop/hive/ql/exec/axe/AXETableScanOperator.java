package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"unused", "FieldCanBeLocal"})
class AXETableScanOperator {
  List<Integer> projectCols;
  int id;
  private int tableId;
  private List<Integer> neededCols;
  private AXEExpression filterDesc;
  private List<Integer> childOps;
  private List<AXEExpression> partitionKey;
  private List<Integer> reduceKeyColumns;
  private List<Integer> reduceValueColumns;
  private String reduceSortOrder;

  AXETableScanOperator(int id, final TableScanOperator op, int tableId) {
    this.id = id;
    this.tableId = tableId;
    neededCols = op.getNeededColumnIDs();
    projectCols = new ArrayList<>();
  }

  void setProjectCols(final List<ExprNodeDesc> colList,
      final Map<String, Integer> inputColIndex) {
    for (ExprNodeDesc desc : colList) {
      if (desc instanceof ExprNodeColumnDesc) {
        ExprNodeColumnDesc columnDesc = (ExprNodeColumnDesc) desc;
        projectCols.add(inputColIndex.get(columnDesc.getColumn()));
      }
    }
  }

  void addFilterDesc(final AXEExpression filterDesc) {
    this.filterDesc = filterDesc;
  }

  void addChild(final int child) {
    if (childOps == null) {
      childOps = new ArrayList<>();
    }
    childOps.add(child);
  }

  void setReduceKey(final ArrayList<ExprNodeDesc> keyCols,
      final Map<String, Integer> inputColIndex) {
    if (keyCols == null || keyCols.isEmpty()) {
      return;
    }
    reduceKeyColumns = new ArrayList<>();
    // TODO(tatiana): support algorithmic operations in reduce key?
    for (ExprNodeDesc expr : keyCols) {
      if (expr instanceof ExprNodeColumnDesc) {
        reduceKeyColumns.add(inputColIndex.get(((ExprNodeColumnDesc) expr).getColumn()));
      } else {
        throw new IllegalStateException("Reduce key of type " + expr.getClass().getName() + " not supported yet");
      }
    }
  }

  void setReduceValue(final ArrayList<ExprNodeDesc> valueCols,
      final Map<String, Integer> inputColIndex) {
    if (valueCols == null || valueCols.isEmpty()) {
      return;
    }
    reduceValueColumns = new ArrayList<>();
    // TODO(tatiana): support algorithmic operations in reduce key?
    for (ExprNodeDesc expr : valueCols) {
      if (expr instanceof ExprNodeColumnDesc) {
        reduceValueColumns.add(inputColIndex.get(((ExprNodeColumnDesc) expr).getColumn()));
      } else {
        throw new IllegalStateException("Reduce value of type " + expr.getClass().getName() + " not supported yet");
      }
    }
  }

  void setPartitionKey(final ArrayList<ExprNodeDesc> partitionCols,
      final Map<String, Integer> inputColIndex) { // TODO(tatiana): can it be simplified?
    if (partitionCols == null || partitionCols.isEmpty()) {
      return;
    }
    partitionKey = new ArrayList<>();
    for (ExprNodeDesc expr : partitionCols) {
      partitionKey.add(new AXEExpression(expr, inputColIndex));
    }
  }

  public void setSortOrder(final String order) {
    this.reduceSortOrder = order;
  }
}
