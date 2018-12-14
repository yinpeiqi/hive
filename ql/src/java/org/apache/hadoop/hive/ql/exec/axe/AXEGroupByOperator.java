package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"FieldCanBeLocal", "unused"})
class AXEGroupByOperator extends AXEOperator {

  private List<AggregatorKey> aggregatorKeys;
  private List<Aggregator> aggregators;
  private String mode;

  AXEGroupByOperator(int id) {
    super(id);
  }

  void setAggregators(List<AggregationDesc> aggregators) {
    this.aggregators = new ArrayList<>();
    for (AggregationDesc desc : aggregators) {
      boolean distinct = desc.getDistinct();
      String funcName = desc.getGenericUDAFName();
      List<ExprNodeDesc> parameters = desc.getParameters();
      List<AXEExpression> parameterDesc = new ArrayList<>();
      for (ExprNodeDesc parameter : parameters) {
        parameterDesc.add(new AXEExpression(parameter));
      }
      this.aggregators.add(new Aggregator(funcName, parameterDesc, distinct));
    }
  }

  void setAggregatorKeys(List<ExprNodeDesc> aggregatorKeys) {
    this.aggregatorKeys = new ArrayList<>();
    for (ExprNodeDesc desc : aggregatorKeys) {
      if (desc instanceof ExprNodeColumnDesc) {
        ExprNodeColumnDesc columnDesc = (ExprNodeColumnDesc) desc;
        this.aggregatorKeys.add(new AggregatorKey(columnDesc.getColumn(), columnDesc.getTabAlias()));
      } else {
        throw new IllegalStateException(
            "Expected ExprNodeColumnDesc for aggregator keys, but got " + desc.getClass().getName());
      }
    }
  }

  public void setMode(final String mode) {
    this.mode = mode;
  }

  @SuppressWarnings("unused")
  class AggregatorKey {
    private String column;
    private String table;

    AggregatorKey(String column, String table) {
      this.table = table;
      this.column = column;
    }
  }

  @SuppressWarnings("unused")
  class Aggregator {
    private final boolean distinct;
    private final List<AXEExpression> parameters;
    private final String func;

    Aggregator(String func, List<AXEExpression> parameters, final boolean distinct) {
      this.func = func;
      this.parameters = parameters;
      this.distinct = distinct;
    }
  }
}
