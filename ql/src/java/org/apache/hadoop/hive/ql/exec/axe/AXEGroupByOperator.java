package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"FieldCanBeLocal", "unused"})
class AXEGroupByOperator extends AXEOperator {

  List<String> outputColumnNames;
  private List<AXEExpression> aggregatorKeys;
  private List<Aggregator> aggregators;
  private String mode;
  private boolean bucketGroup;

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
      this.aggregators.add(new Aggregator(funcName, parameterDesc, distinct, desc.getMode()));
    }
  }

  void setAggregatorKeys(List<ExprNodeDesc> aggregatorKeys) {
    this.aggregatorKeys = new ArrayList<>();
    for (ExprNodeDesc desc : aggregatorKeys) {
      this.aggregatorKeys.add(new AXEExpression(desc));
    }
  }

  public void setMode(final String mode) {
    this.mode = mode;
  }

  void setBucketGroup(final boolean bucketGroup) { this.bucketGroup = bucketGroup; }

  @SuppressWarnings("unused")
  static class Aggregator {
    private final boolean distinct;
    private final List<AXEExpression> parameters;
    private final String func;
    private int mode;

    Aggregator(String func, List<AXEExpression> parameters, final boolean distinct,
        final GenericUDAFEvaluator.Mode mode) {
      this.func = func;
      this.parameters = parameters;
      this.distinct = distinct;
      this.mode = mode.ordinal();
    }
  }
}
