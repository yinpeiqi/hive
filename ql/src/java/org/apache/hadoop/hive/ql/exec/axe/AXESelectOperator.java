package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class AXESelectOperator extends AXEOperator {
  List<Integer> projectCols;
  List<String> projectColNames;

  AXESelectOperator(int id) {
    super(id);
  }

  void setProjectCols(final List<ExprNodeDesc> colList) {
    projectColNames = new ArrayList<>();
    for (ExprNodeDesc desc : colList) {
      if (desc instanceof ExprNodeColumnDesc) {
        ExprNodeColumnDesc columnDesc = (ExprNodeColumnDesc) desc;
        projectColNames.add(columnDesc.getColumn());
      }
    }
  }

  void translateColNameToIndex(Map<String, Integer> colMap) {
    projectCols = new ArrayList<>();
    for (String colName : projectColNames) {
      projectCols.add(colMap.get(colName));
    }
    projectColNames = null;
  }

}
