package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;

import java.util.ArrayList;
import java.util.List;

class AXEJoinOperator extends AXEOperator {
  private List<List<JoinColumn>> joinTableColumns;
  private List<JoinCondition> joinConditions;

  AXEJoinOperator(int id) {
    super(id);
    joinTableColumns = new ArrayList<>();
    joinConditions = new ArrayList<>();
  }

  void setJoinKeys(final ExprNodeDesc[][] joinKeys) {
    for (int keyIdx = 0; keyIdx < joinKeys.length; ++keyIdx) {
      joinTableColumns.add(new ArrayList<JoinColumn>());
      for (int k = 0; k < joinKeys[keyIdx].length; ++k) {
        if (joinKeys[keyIdx][k] instanceof ExprNodeColumnDesc) {
          ExprNodeColumnDesc columnDesc = (ExprNodeColumnDesc) joinKeys[keyIdx][k];
          joinTableColumns.get(keyIdx).add(
              new JoinColumn(columnDesc.getTabAlias(), columnDesc.getColumn(), columnDesc.getTypeInfo().getTypeName()));
        } else {
          throw new IllegalStateException(
              "Expected only column desc in join keys, but got " + joinKeys[keyIdx][k].getClass().getName());
        }
      }
    }

    // check
    for (List<JoinColumn> tableJoinKey : joinTableColumns) {
      String table = tableJoinKey.get(0).table;
      for (JoinColumn column : tableJoinKey) {
        if (!table.equals(column.table)) {
          throw new IllegalStateException("The columns within one table is not associated with the same table alias : "
                                              + table + " vs " + column.table);
        }
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

  class JoinCondition {
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
