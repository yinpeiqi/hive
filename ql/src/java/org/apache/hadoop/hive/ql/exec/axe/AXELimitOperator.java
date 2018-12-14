package org.apache.hadoop.hive.ql.exec.axe;

@SuppressWarnings({"FieldCanBeLocal", "unused"})
class AXELimitOperator extends AXEOperator {
  private int limit;
  private int leastRows;
  private Integer offset;

  AXELimitOperator(int id, int limit, int leastRows, Integer offset) {
    super(id);
    this.limit = limit;
    this.leastRows = leastRows;
    this.offset = offset;
  }
}
