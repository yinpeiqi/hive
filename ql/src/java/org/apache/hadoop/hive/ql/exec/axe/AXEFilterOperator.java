package org.apache.hadoop.hive.ql.exec.axe;

class AXEFilterOperator extends AXEOperator {
  private AXEExpression filterDesc;

  void addFilterDesc(final AXEExpression filterDesc) {
    this.filterDesc = filterDesc;
  }

  AXEFilterOperator(final int id) {
    super(id);
  }
}
