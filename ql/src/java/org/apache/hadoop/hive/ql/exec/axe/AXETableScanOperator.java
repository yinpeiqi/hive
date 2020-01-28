package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.ql.exec.TableScanOperator;

import java.util.List;

@SuppressWarnings({"FieldCanBeLocal", "unused"})
class AXETableScanOperator extends AXESelectOperator {
  private int tableId;
  private List<Integer> neededCols;
  private List<String> referencedColumns;
  private AXEExpression filterDesc;

  AXETableScanOperator(int id, final TableScanOperator op, int tableId) {
    super(id);
    this.tableId = tableId;
    neededCols = op.getNeededColumnIDs();
    if (op.getReferencedColumns() != null && neededCols.size() != op.getReferencedColumns().size()) {
      referencedColumns = op.getReferencedColumns();
    }
  }

  void addFilterDesc(final AXEExpression filterDesc) {
    this.filterDesc = filterDesc;
  }
}
