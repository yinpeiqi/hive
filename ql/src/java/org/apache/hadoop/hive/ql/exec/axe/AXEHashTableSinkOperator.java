package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.HashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.TerminalOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.plan.axe.AXEHashTableSinkDesc;

import java.io.Serializable;

public class AXEHashTableSinkOperator
    extends TerminalOperator<AXEHashTableSinkDesc> implements Serializable {

  private final HashTableSinkOperator htsOperator;

  public AXEHashTableSinkOperator(CompilationOpContext ctx) {
    super(ctx);
    this.htsOperator = new HashTableSinkOperator (ctx);
  }

  @Override public void process(final Object row, final int tag) throws HiveException {
    htsOperator.process(row, conf.getTag());
  }

  @Override public OperatorType getType() {
    return null;
  }
}
