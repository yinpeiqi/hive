package org.apache.hadoop.hive.ql.parse.axe;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.Stack;

public class AXEFileSinkProcessor implements NodeProcessor {

  @Override public Object process(final Node nd, final Stack<Node> stack, final NodeProcessorCtx procCtx,
      final Object... nodeOutputs)
      throws SemanticException {
    GenAXEProcContext context = (GenAXEProcContext) procCtx;
    FileSinkOperator fileSink = (FileSinkOperator) nd;

    context.fileSinkSet.add(fileSink);
    return true;
  }
}
