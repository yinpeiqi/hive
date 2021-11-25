package org.apache.hadoop.hive.ql.parse.axe;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 * Walks the operator tree in DFS fashion.
 *
 * Cloned from GenSparkWorkWalker.
 */
public class GenAXEWorkWalker extends DefaultGraphWalker {
  private final GenAXEProcContext ctx;

  GenAXEWorkWalker(Dispatcher dispatcher, GenAXEProcContext procCtx) {
    super(dispatcher);
    this.ctx = procCtx;
  }

  private void setRoot(Node nd) {
    ctx.currentRootOperator = (Operator<? extends OperatorDesc>) nd;
    ctx.precedingWork = null;
    ctx.parentOfRoot = null;
  }

  @Override
  public void startWalking(Collection<Node> startNodes, HashMap<Node, Object> nodeOutput)
      throws SemanticException {
    toWalk.addAll(startNodes);
    while (toWalk.size() > 0) {
      Node nd = toWalk.remove(0);
      setRoot(nd);
      walk(nd);
      if (nodeOutput != null) {
        nodeOutput.put(nd, retMap.get(nd));
      }
    }
  }

  @Override
  protected void walk(Node nd) throws SemanticException {
    List<? extends Node> children = nd.getChildren();

    // maintain the stack of operators encountered
    opStack.push(nd);
    Boolean skip = dispatchAndReturn(nd, opStack);

    // save some positional state
    Operator<? extends OperatorDesc> currentRoot = ctx.currentRootOperator;
    Operator<? extends OperatorDesc> parentOfRoot = ctx.parentOfRoot;
    BaseWork precedingWork = ctx.precedingWork;

    if (skip == null || !skip) {
      // move all the children to the front of queue
      for (Node ch : children) {

        // and restore the state before walking each child
        ctx.currentRootOperator = currentRoot;
        ctx.parentOfRoot = parentOfRoot;
        ctx.precedingWork = precedingWork;

        walk(ch);
      }
    }

    // done with this operator
    opStack.pop();
  }
}
