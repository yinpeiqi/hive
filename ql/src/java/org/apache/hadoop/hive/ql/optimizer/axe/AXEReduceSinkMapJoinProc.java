package org.apache.hadoop.hive.ql.optimizer.axe;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.axe.AXEHashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.axe.AXEWork;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.axe.GenAXEProcContext;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.HashTableDummyDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.SparkEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.axe.AXEHashTableSinkDesc;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

public class AXEReduceSinkMapJoinProc implements org.apache.hadoop.hive.ql.lib.NodeProcessor {
  public static final Logger LOG = LoggerFactory.getLogger(AXEReduceSinkMapJoinProc.class.getName());

  private boolean hasGroupBy(Operator<? extends OperatorDesc> mapjoinOp,
      GenAXEProcContext context) throws SemanticException {
    List<Operator<? extends OperatorDesc>> childOps = mapjoinOp.getChildOperators();
    Map<Rule, NodeProcessor> rules = new LinkedHashMap<>();
    AXEMapJoinFollowedByGroupByProcessor processor = new AXEMapJoinFollowedByGroupByProcessor();
    rules.put(new RuleRegExp("GBY", GroupByOperator.getOperatorName() + "%"), processor);
    Dispatcher disp = new DefaultRuleDispatcher(null, rules, context);
    GraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>(childOps);
    ogw.startWalking(topNodes, null);
    return processor.getHasGroupBy();
  }

  @Override
  public Object process(final Node nd, final Stack<Node> stack, final NodeProcessorCtx procCtx,
      final Object... nodeOutputs)
      throws SemanticException {
    GenAXEProcContext context = (GenAXEProcContext) procCtx;

    if (!nd.getClass().equals(MapJoinOperator.class)) {
      return null;
    }

    MapJoinOperator mapJoinOp = (MapJoinOperator) nd;

    if (stack.size() < 2 || !(stack.get(stack.size() - 2) instanceof ReduceSinkOperator)) {
      context.currentMapJoinOperators.add(mapJoinOp);
      return null;
    }

    context.precedingWork = null;
    context.currentRootOperator = null;

    ReduceSinkOperator parentRS = (ReduceSinkOperator) stack.get(stack.size() - 2);
    // remove the tag for in-memory side of mapjoin
    parentRS.getConf().setSkipTag(true);
    parentRS.setSkipTag(true);
    // remember the original parent list before we start modifying it.
    if (!context.mapJoinParentMap.containsKey(mapJoinOp)) {
      List<Operator<?>> parents = new ArrayList<>(mapJoinOp.getParentOperators());
      context.mapJoinParentMap.put(mapJoinOp, parents);
    }

    List<BaseWork> mapJoinWork;

    /*
     *  If there was a pre-existing work generated for the big-table mapjoin side,
     *  we need to hook the work generated for the RS (associated with the RS-MJ pattern)
     *  with the pre-existing work.
     *
     *  Otherwise, we need to associate that the mapjoin op
     *  to be linked to the RS work (associated with the RS-MJ pattern).
     *
     */
    mapJoinWork = context.mapJoinWorkMap.get(mapJoinOp);
    int workMapSize = context.childToWorkMap.get(parentRS).size();
    Preconditions.checkArgument(workMapSize == 1,
                                "AssertionError: expected context.childToWorkMap.get(parentRS).size() to be 1, but was "
                                    + workMapSize);
    BaseWork parentWork = context.childToWorkMap.get(parentRS).get(0);

    // set the link between mapjoin and parent vertex
    int pos = context.mapJoinParentMap.get(mapJoinOp).indexOf(parentRS);
    if (pos == -1) {
      throw new SemanticException("Cannot find position of parent in mapjoin");
    }
    LOG.debug("Mapjoin " + mapJoinOp + ", pos: " + pos + " --> " + parentWork.getName());
    mapJoinOp.getConf().getParentToInput().put(pos, parentWork.getName());

    SparkEdgeProperty edgeProp = new SparkEdgeProperty(SparkEdgeProperty.SHUFFLE_NONE);

    if (mapJoinWork != null) {
      for (BaseWork myWork : mapJoinWork) {
        // link the work with the work associated with the reduce sink that triggered this rule
        AXEWork axeWork = context.currentTask.getWork();
        LOG.debug("connecting " + parentWork.getName() + " with " + myWork.getName());
        axeWork.connect(parentWork, myWork, edgeProp);
      }
    }

    // remember in case we need to connect additional work later
    Map<BaseWork, SparkEdgeProperty> linkWorkMap;
    if (context.linkOpWithWorkMap.containsKey(mapJoinOp)) {
      linkWorkMap = context.linkOpWithWorkMap.get(mapJoinOp);
    } else {
      linkWorkMap = new HashMap<>();
    }
    linkWorkMap.put(parentWork, edgeProp);
    context.linkOpWithWorkMap.put(mapJoinOp, linkWorkMap);

    List<ReduceSinkOperator> reduceSinks
        = context.linkWorkWithReduceSinkMap.get(parentWork);
    if (reduceSinks == null) {
      reduceSinks = new ArrayList<>();
    }
    reduceSinks.add(parentRS);
    context.linkWorkWithReduceSinkMap.put(parentWork, reduceSinks);

    // create the dummy operators
    List<Operator<?>> dummyOperators = new ArrayList<>();

    // create an new operator: HashTableDummyOperator, which share the table desc
    HashTableDummyDesc desc = new HashTableDummyDesc();
    HashTableDummyOperator dummyOp = (HashTableDummyOperator) OperatorFactory.get(
        mapJoinOp.getCompilationOpContext(), desc);
    TableDesc tbl;

    // need to create the correct table descriptor for key/value
    RowSchema rowSchema = parentRS.getParentOperators().get(0).getSchema();
    tbl = PlanUtils.getReduceValueTableDesc(PlanUtils.getFieldSchemasFromRowSchema(rowSchema, ""));
    dummyOp.getConf().setTbl(tbl);

    Map<Byte, List<ExprNodeDesc>> keyExprMap = mapJoinOp.getConf().getKeys();
    List<ExprNodeDesc> keyCols = keyExprMap.get((byte) 0);
    StringBuilder keyOrder = new StringBuilder();
    StringBuilder keyNullOrder = new StringBuilder();
    for (int i = 0; i < keyCols.size(); i++) {
      keyOrder.append("+");
      keyNullOrder.append("a");
    }
    TableDesc keyTableDesc = PlanUtils.getReduceKeyTableDesc(PlanUtils
                                                                 .getFieldSchemasFromColumnList(keyCols, "mapjoinkey"),
                                                             keyOrder.toString(),
                                                             keyNullOrder.toString());
    mapJoinOp.getConf().setKeyTableDesc(keyTableDesc);

    // let the dummy op be the parent of mapjoin op
    mapJoinOp.replaceParent(parentRS, dummyOp);
    List<Operator<? extends OperatorDesc>> dummyChildren = new ArrayList<>();
    dummyChildren.add(mapJoinOp);
    dummyOp.setChildOperators(dummyChildren);
    dummyOperators.add(dummyOp);

    // cut the operator tree so as to not retain connections from the parent RS downstream
    List<Operator<? extends OperatorDesc>> childOperators = parentRS.getChildOperators();
    childOperators.remove(mapJoinOp);

    // the "work" needs to know about the dummy operators. They have to be separately initialized
    // at task startup
    if (mapJoinWork != null) {
      for (BaseWork myWork : mapJoinWork) {
        myWork.addDummyOp(dummyOp);
      }
    }
    if (context.linkChildOpWithDummyOp.containsKey(mapJoinOp)) {
      dummyOperators.addAll(context.linkChildOpWithDummyOp.get(mapJoinOp));
    }
    context.linkChildOpWithDummyOp.put(mapJoinOp, dummyOperators);

    // replace ReduceSinkOp with HashTableSinkOp for the RSops which are parents of MJop
    MapJoinDesc mjDesc = mapJoinOp.getConf();
    HiveConf conf = context.conf;

    // Unlike in MR, we may call this method multiple times, for each
    // small table HTS. But, since it's idempotent, it should be OK.
    mjDesc.resetOrder();

    float hashTableMemoryUsage;
    if (hasGroupBy(mapJoinOp, context)) {
      hashTableMemoryUsage = conf.getFloatVar(
          HiveConf.ConfVars.HIVEHASHTABLEFOLLOWBYGBYMAXMEMORYUSAGE);
    } else {
      hashTableMemoryUsage = conf.getFloatVar(
          HiveConf.ConfVars.HIVEHASHTABLEMAXMEMORYUSAGE);
    }
    mjDesc.setHashTableMemoryUsage(hashTableMemoryUsage);

    AXEHashTableSinkDesc hashTableSinkDesc = new AXEHashTableSinkDesc(mjDesc);
    AXEHashTableSinkOperator hashTableSinkOp = (AXEHashTableSinkOperator) OperatorFactory.get(
        mapJoinOp.getCompilationOpContext(), hashTableSinkDesc);

    byte tag = (byte) pos;
    int[] valueIndex = mjDesc.getValueIndex(tag);
    if (valueIndex != null) {
      List<ExprNodeDesc> newValues = new ArrayList<>();
      List<ExprNodeDesc> values = hashTableSinkDesc.getExprs().get(tag);
      for (int index = 0; index < values.size(); index++) {
        if (valueIndex[index] < 0) {
          newValues.add(values.get(index));
        }
      }
      hashTableSinkDesc.getExprs().put(tag, newValues);
    }

    //get all parents of reduce sink
    List<Operator<? extends OperatorDesc>> rsParentOps = parentRS.getParentOperators();
    for (Operator<? extends OperatorDesc> parent : rsParentOps) {
      parent.replaceChild(parentRS, hashTableSinkOp);
    }
    hashTableSinkOp.setParentOperators(rsParentOps);
    hashTableSinkOp.getConf().setTag(tag);
    return true;
  }

  public static class AXEMapJoinFollowedByGroupByProcessor implements NodeProcessor {
    private boolean hasGroupBy = false;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      GenAXEProcContext context = (GenAXEProcContext) procCtx;
      hasGroupBy = true;
      GroupByOperator op = (GroupByOperator) nd;
      float groupByMemoryUsage = context.conf.getFloatVar(
          HiveConf.ConfVars.HIVEMAPJOINFOLLOWEDBYMAPAGGRHASHMEMORY);
      op.getConf().setGroupByMemoryUsage(groupByMemoryUsage);
      return null;
    }

    boolean getHasGroupBy() {
      return hasGroupBy;
    }
  }
}
