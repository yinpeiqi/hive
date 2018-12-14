package org.apache.hadoop.hive.ql.exec.axe;

class AXEFileSinkOperator extends AXEOperator {
  String destPath;
  AXEFileSinkOperator(int id) {
    super(id);
  }
}
