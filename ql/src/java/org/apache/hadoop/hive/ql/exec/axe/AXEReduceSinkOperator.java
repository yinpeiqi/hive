package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"FieldCanBeLocal", "unused"})
public class AXEReduceSinkOperator extends AXEOperator {

  private List<AXEExpression> partitionKey;
  private List<AXEExpression> reduceKeyColumns;
  private List<AXEExpression> reduceValueColumns;
  private String reduceSortOrder;
  List<String> reduceKeyColumnNames;
  List<String> reduceValueColumnNames;
  String reduceOutputName;
  boolean hasOrderBy;
  int topN = -1;
  int numReducers = -1;

  AXEReduceSinkOperator(int id) {
    super(id);
  }

  void setReduceKey(final ArrayList<ExprNodeDesc> keyCols,
      final Map<String, Integer> inputColIndex) {
    if (keyCols == null || keyCols.isEmpty()) {
      return;
    }
    reduceKeyColumns = new ArrayList<>();
    for (ExprNodeDesc expr : keyCols) {
      reduceKeyColumns.add(new AXEExpression(expr));
    }
  }

  void setReduceValue(final ArrayList<ExprNodeDesc> valueCols,
      final Map<String, Integer> inputColIndex) {
    if (valueCols == null || valueCols.isEmpty()) {
      return;
    }
    reduceValueColumns = new ArrayList<>();
    for (ExprNodeDesc expr : valueCols) {
      reduceValueColumns.add(new AXEExpression(expr));
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

  public void setHasOrderBy(final Boolean hasOrderBy) {
    this.hasOrderBy = hasOrderBy;
  }
  public void setTopN(int topN) {
    this.topN = topN;
  }
}
