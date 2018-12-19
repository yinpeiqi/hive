package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"FieldCanBeLocal", "unused"})
public class AXEReduceSinkOperator extends AXEOperator {

  private List<AXEExpression> partitionKey;
  private List<String> reduceKeyColumns;
  private List<String> reduceValueColumns;
  private String reduceSortOrder;

  AXEReduceSinkOperator(int id) {
    super(id);
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
        reduceKeyColumns.add(((ExprNodeColumnDesc) expr).getColumn());
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
        reduceValueColumns.add(((ExprNodeColumnDesc) expr).getColumn());
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
      partitionKey.add(new AXEExpression(expr));
    }
  }

  public void setSortOrder(final String order) {
    this.reduceSortOrder = order;
  }
}
