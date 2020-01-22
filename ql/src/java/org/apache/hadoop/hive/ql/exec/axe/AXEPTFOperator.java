package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.ptf.OrderDef;
import org.apache.hadoop.hive.ql.plan.ptf.OrderExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFQueryInputDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowTableFunctionDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

class AXEPTFOperator extends AXEOperator {
  private final List<Order> orderExprs = new ArrayList<>();
  private final List<AXEExpression> partitionExprs = new ArrayList<>();
  private final List<WindowFunction> windowFunctions = new ArrayList<>();
  private List<String> outputColumnNames;

  AXEPTFOperator(final int id) {
    super(id);
  }

  void initialize(final PTFDesc conf) {
    PartitionedTableFunctionDef funcDef = conf.getFuncDef();
    Preconditions.checkArgument(funcDef != null, "Get null funcDef for PTF operation");
    Preconditions.checkArgument(funcDef instanceof WindowTableFunctionDef,
                                "Now assuming class is WindowTableFunctionDef, but got " + funcDef);
    WindowTableFunctionDef windowTableFunctionDef = (WindowTableFunctionDef) funcDef;

    // input
    Preconditions.checkArgument(funcDef.getInput() instanceof PTFQueryInputDef,
                                "Now assuming the input to the window function is PTFQueryInputDef, but got "
                                    + funcDef.getInput());
    setInput((PTFQueryInputDef) funcDef.getInput());

    // window functions
    setWindowFunction(windowTableFunctionDef.getWindowFunctions());

    // order, partition and rank limit
    setOrder(windowTableFunctionDef.getOrder());
    setPartition(windowTableFunctionDef.getPartition());
    Preconditions.checkArgument(windowTableFunctionDef.getRankLimit() == -1, "Not supporting rank limit for now");

  }

  private void setPartition(final PartitionDef partitionDef) {
    if (partitionDef.getExpressions() == null) {
      return;
    }
    for (PTFExpressionDef ptfExpressionDef : partitionDef.getExpressions()) {
      partitionExprs.add(new AXEExpression(ptfExpressionDef.getExprNode()));
    }
  }

  private void setOrder(final OrderDef orderDef) {
    for (OrderExpressionDef orderExpressionDef : orderDef.getExpressions()) {
      orderExprs.add(new Order(orderExpressionDef.getOrder().ordinal(), orderExpressionDef.getNullOrder().ordinal(),
                               new AXEExpression(orderExpressionDef.getExprNode())));
    }
  }

  private void setWindowFunction(List<WindowFunctionDef> windowFunctionDefs) {
    // always complete mode
    for (WindowFunctionDef windowFunctionDef : windowFunctionDefs) {
      windowFunctions.add(new WindowFunction(windowFunctionDef));
      Preconditions.checkArgument(windowFunctionDef.getWindowFrame().getWindowSize() == -1,
                                  "Does not support windowing yet");
    }
  }

  private void setInput(PTFQueryInputDef ptfQueryInputDef) {
    Preconditions.checkArgument(ptfQueryInputDef.getType() == PTFInvocationSpec.PTFQueryInputType.WINDOWING,
                                "Supporting only windowing now");
    // FIXME(tatiana)
    outputColumnNames = ptfQueryInputDef.getOutputShape().getColumnNames();
  }

  @SuppressWarnings("FieldCanBeLocal")
  static class WindowFunction {
    private final String name;
    private final String alias; // output column name
    private final List<AXEExpression> args = new ArrayList<>();
    private final boolean isStar;
    private final boolean isDistinct;

    WindowFunction(WindowFunctionDef windowFunctionDef) {
      this.alias = windowFunctionDef.getAlias();
      this.name = windowFunctionDef.getName();
      this.isDistinct = windowFunctionDef.isDistinct();
      this.isStar = windowFunctionDef.isStar();
      if (windowFunctionDef.getArgs() != null) {
        for (PTFExpressionDef arg : windowFunctionDef.getArgs()) {
          this.args.add(new AXEExpression(arg.getExprNode()));
        }
      }
    }
  }

  @SuppressWarnings("FieldCanBeLocal")
  static class Order {
    @SuppressWarnings("MismatchedReadAndWriteOfArray")
    private final int[] orders = new int[2];
    private final AXEExpression expr;

    Order(int order, int nullOrder, AXEExpression expr) {
      orders[0] = order;
      orders[1] = nullOrder;
      this.expr = expr;
    }
  }
}
