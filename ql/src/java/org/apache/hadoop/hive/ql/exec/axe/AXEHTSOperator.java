package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.axe.AXEHashTableSinkDesc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"FieldCanBeLocal", "unused"})
public class AXEHTSOperator extends AXEOperator {
  String stageName;
  private Byte tag;
  private Map<Byte, List<AXEExpression>> filters;
  private Map<Byte, List<AXEExpression>> exprs;
  private Map<Byte, List<AXEExpression>> keys;
  private int posBigTable;
  private boolean isNoOuterJoin;
  private String dumpFilePrefix;
  List<String> outputColumnNames;

  public AXEHTSOperator(int id) {
    super(id);
  }

  static void SetExprs(Map<Byte, List<AXEExpression>> output, Map<Byte, List<ExprNodeDesc>> input) {
    for (Map.Entry<Byte, List<ExprNodeDesc>> entry: input.entrySet()) {
      List<AXEExpression> axeExprs = new ArrayList<>();
      output.put(entry.getKey(), axeExprs);
      for (ExprNodeDesc expr : entry.getValue()) {
        axeExprs.add(new AXEExpression(expr));
      }
    }
  }

  public void initialize(final AXEHashTableSinkDesc desc) {
    outputColumnNames = desc.getOutputColumnNames();
    dumpFilePrefix = desc.getDumpFilePrefix();
    tag = desc.getTag();
    filters = new HashMap<>();
    SetExprs(filters, desc.getFilters());
    exprs = new HashMap<>();
    SetExprs(exprs, desc.getExprs());
    keys =  new HashMap<>();
    SetExprs(keys, desc.getKeys());
    posBigTable = desc.getPosBigTable();
    isNoOuterJoin = desc.isNoOuterJoin();
  }
}
