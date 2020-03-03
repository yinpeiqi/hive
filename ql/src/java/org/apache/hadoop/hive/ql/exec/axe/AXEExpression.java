package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

class AXEExpression {

  private List<AXEExprNode> internalNodeInfos;
  private List<AXEExprNode> constLeafNodeInfos;
  private List<AXEExprNode> tableFieldNodeInfos;

  AXEExpression(final ExprNodeDesc exprNodeDesc) {
    int nodeCounter = 0;
    internalNodeInfos = new ArrayList<>();
    constLeafNodeInfos = new ArrayList<>();
    tableFieldNodeInfos = new ArrayList<>();

    Deque<ObjectPair<ExprNodeDesc, Integer>> queue = new LinkedList<>();
    int nodeId = nodeCounter++;
    nodeCounter = processExpr(nodeId, exprNodeDesc, queue, nodeCounter);
    while (!queue.isEmpty()) {
      ObjectPair<ExprNodeDesc, Integer> pair = queue.pop();
      nodeCounter = processExpr(pair.getSecond(), pair.getFirst(), queue, nodeCounter);
    }
  }

  private static String getFuncText(final ExprNodeGenericFuncDesc funcDesc) {
    if (funcDesc.getFuncText() != null) {
      return funcDesc.getFuncText();
    }
    if (funcDesc.getGenericUDF() instanceof GenericUDFOPAnd) {
      return "And";
    } else if (funcDesc.getGenericUDF() instanceof GenericUDFOPOr) {
      return "Or";
    } else if (funcDesc.getGenericUDF() instanceof GenericUDFBridge) {
      return funcDesc.getGenericUDF().getUdfName();
    }
    return funcDesc.getGenericUDF().getClass().getSimpleName().substring(10).toLowerCase();
  }

  private int processExpr(int nodeId, ExprNodeDesc exprNodeDesc, Deque<ObjectPair<ExprNodeDesc, Integer>> queue,
      int nodeCounter) {
    AXEExprNode node = new AXEExprNode(nodeId);
    if (exprNodeDesc instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc) exprNodeDesc;
      node.type = funcDesc.getTypeString();
      node.func = getFuncText(funcDesc);
      node.children = new ArrayList<>();
      for (ExprNodeDesc child : funcDesc.getChildren()) {
        node.children.add(nodeCounter);
        queue.add(new ObjectPair<>(child, nodeCounter++));
      }
      internalNodeInfos.add(node);
    } else if (exprNodeDesc instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc columnDesc = (ExprNodeColumnDesc) exprNodeDesc;
      node.type = columnDesc.getTypeString();
      node.value = columnDesc.getColumn();
      node.tableName = columnDesc.getTabAlias();
      tableFieldNodeInfos.add(node);
    } else if (exprNodeDesc instanceof ExprNodeConstantDesc) {
      ExprNodeConstantDesc constantDesc = (ExprNodeConstantDesc) exprNodeDesc;
      node.type = constantDesc.getTypeString();
      if (constantDesc.getTypeString().equals("string")) {
        if (constantDesc.getValue() != null) {
          node.value = constantDesc.getValue().toString();
        }
      } else {
        node.value = constantDesc.getExprString();
      }
      constLeafNodeInfos.add(node);
    } else {
      throw new IllegalStateException("Unexpected ExprNodeDesc type " + exprNodeDesc.getClass().getName());
    }
    return nodeCounter;
  }

  @SuppressWarnings("unused")
  class AXEExprNode {
    String func; // for internal node
    List<Integer> children; // for internal node
    String type; // for leaf
    String value; // for const literal
    String tableName; // for column ref
    Integer col; // for column ref
    private int id;

    AXEExprNode(int id) {
      this.id = id;
    }
  }
}
