package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class AXEJoinOperator extends AXEOperator {
  Byte[] tagOrder;
  Map<Byte, List<AXEExpression>> joinValueExprs;
  List<String> outputColumnNames;
  private List<List<AXEExpression>> joinKeyExprs;
  private List<JoinCondition> joinConditions;

  AXEJoinOperator(int id) {
    super(id);
    joinKeyExprs = new ArrayList<>();
    joinConditions = new ArrayList<>();
  }

  void setJoinKeys(final ExprNodeDesc[][] joinKeys) {
    if (joinKeys == null) {
      return;
    }
    for (int keyIdx = 0; keyIdx < joinKeys.length; ++keyIdx) {
      joinKeyExprs.add(new ArrayList<AXEExpression>());
      for (int k = 0; k < joinKeys[keyIdx].length; ++k) {
        joinKeyExprs.get(keyIdx).add(new AXEExpression(joinKeys[keyIdx][k]));
      }
    }
  }

  void setJoinValueExprs(final Map<Byte, List<ExprNodeDesc>> joinValueExprs) {
    this.joinValueExprs = new HashMap<>();
    for (Map.Entry<Byte, List<ExprNodeDesc>> entry : joinValueExprs.entrySet()) {
      List<AXEExpression> axeExprs = new ArrayList<>();
      this.joinValueExprs.put(entry.getKey(), axeExprs);
      for (ExprNodeDesc expr : entry.getValue()) {
        axeExprs.add(new AXEExpression(expr));
      }
    }
  }

  /* In JoinCondDesc
   * public static final int INNER_JOIN = 0;
   * public static final int LEFT_OUTER_JOIN = 1;
   * public static final int RIGHT_OUTER_JOIN = 2;
   * public static final int FULL_OUTER_JOIN = 3;
   * public static final int UNIQUE_JOIN = 4;
   * public static final int LEFT_SEMI_JOIN = 5;
   */
  void setJoinConditions(final JoinCondDesc[] joinConds) {
    for (int condIdx = 0; condIdx < joinConds.length; ++condIdx) {
      joinConditions.add(
          new JoinCondition(joinConds[condIdx].getLeft(), joinConds[condIdx].getRight(), joinConds[condIdx].getType()));
    }
  }

  static class JoinCondition {
    final int left;
    final int right;
    final int type;

    JoinCondition(int left, int right, int type) {
      this.left = left;
      this.right = right;
      this.type = type;
    }
  }

  class JoinColumn {
    String table;
    String column;
    String type;

    JoinColumn(String table, String column, String type) {
      this.table = table;
      this.column = column;
      this.type = type;
    }
  }
}
