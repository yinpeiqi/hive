package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import java.util.ArrayList;
import java.util.List;

class AXESelectOperator extends AXEOperator {
  List<AXEExpression> projectCols;
  List<String> outputColumnNames;

  AXESelectOperator(int id) {
    super(id);
  }

  void setProjectCols(final List<ExprNodeDesc> colList) {
    projectCols = new ArrayList<>();
    for (ExprNodeDesc desc : colList) {
      projectCols.add(new AXEExpression(desc));
    }
  }
}
