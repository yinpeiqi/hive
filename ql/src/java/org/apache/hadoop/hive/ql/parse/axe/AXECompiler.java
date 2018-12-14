package org.apache.hadoop.hive.ql.parse.axe;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.DummyStoreOperator;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.axe.AXETask;
import org.apache.hadoop.hive.ql.exec.axe.AXEWork;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.CompositeProcessor;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.lib.TypeRule;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.optimizer.axe.AXEReduceSinkMapJoinProc;
import org.apache.hadoop.hive.ql.parse.GlobalLimitCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TaskCompiler;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.BucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public class AXECompiler extends TaskCompiler {

  private static final String CLASS_NAME = AXECompiler.class.getName();
  private static final PerfLogger PERF_LOGGER = SessionState.getPerfLogger();
  protected final Logger LOG = LoggerFactory.getLogger(AXECompiler.class);

  private static void annotateMapWork(GenAXEProcContext context, MapWork mapWork,
      SMBMapJoinOperator smbMapJoinOp, TableScanOperator ts, boolean local)
      throws SemanticException {
    String currAliasId = findAliasId(context, ts);
    GenMapRedUtils.setMapWork(mapWork, context.parseContext,
                              context.inputs, null, ts, currAliasId, context.conf, local);
    setupBucketMapJoinInfo(mapWork, smbMapJoinOp);
  }

  private static void setupBucketMapJoinInfo(MapWork plan, SMBMapJoinOperator currMapJoinOp) {

    if (currMapJoinOp != null) {
      Map<String, Map<String, List<String>>> aliasBucketFileNameMapping =
          currMapJoinOp.getConf().getAliasBucketFileNameMapping();
      if (aliasBucketFileNameMapping != null) {
        MapredLocalWork localPlan = plan.getMapRedLocalWork();
        if (localPlan == null) {
          localPlan = currMapJoinOp.getConf().getLocalWork();
        } else {
          // local plan is not null, we want to merge it into SMBMapJoinOperator's local work
          MapredLocalWork smbLocalWork = currMapJoinOp.getConf().getLocalWork();
          if (smbLocalWork != null) {
            localPlan.getAliasToFetchWork().putAll(smbLocalWork.getAliasToFetchWork());
            localPlan.getAliasToWork().putAll(smbLocalWork.getAliasToWork());
          }
        }

        if (localPlan == null) {
          return;
        }
        plan.setMapRedLocalWork(null);
        currMapJoinOp.getConf().setLocalWork(localPlan);

        BucketMapJoinContext bucketMJCxt = new BucketMapJoinContext();
        localPlan.setBucketMapjoinContext(bucketMJCxt);
        bucketMJCxt.setAliasBucketFileNameMapping(aliasBucketFileNameMapping);
        bucketMJCxt.setBucketFileNameMapping(
            currMapJoinOp.getConf().getBigTableBucketNumMapping());
        localPlan.setInputFileChangeSensitive(true);
        bucketMJCxt.setMapJoinBigTableAlias(currMapJoinOp.getConf().getBigTableAlias());
        bucketMJCxt
            .setBucketMatcherClass(org.apache.hadoop.hive.ql.exec.DefaultBucketMatcher.class);
        bucketMJCxt.setBigTablePartSpecToFileMapping(
            currMapJoinOp.getConf().getBigTablePartSpecToFileMapping());

        plan.setUseBucketizedHiveInputFormat(true);

      }
    }
  }

  private static String findAliasId(GenAXEProcContext opProcCtx, TableScanOperator ts) {
    for (String alias : opProcCtx.topOps.keySet()) {
      if (opProcCtx.topOps.get(alias) == ts) {
        return alias;
      }
    }
    return null;
  }

  @Override
  public void compile(final ParseContext pCtx, final List<Task<? extends Serializable>> rootTasks,
      final HashSet<ReadEntity> inputs, final HashSet<WriteEntity> outputs) throws SemanticException {
    super.compile(pCtx, rootTasks, inputs, outputs);
  }

  @Override
  protected void decideExecMode(List<Task<? extends Serializable>> rootTasks, Context ctx,
      GlobalLimitCtx globalLimitCtx) {
    // currently all AXE work is on the AXE instance
  }

  @Override
  protected void optimizeTaskPlan(List<Task<? extends Serializable>> rootTasks,
      ParseContext pCtx, Context ctx) {
    // Currently we do not optimize the task plan
    // Consider parallelism setting & running stats annotation
  }

  @Override
  protected void setInputFormat(Task<? extends Serializable> task) {
    if (task instanceof AXETask) {
      AXEWork work = ((AXETask) task).getWork();
      List<BaseWork> all = work.getAllWork();
      for (BaseWork w : all) {
        if (w instanceof MapWork) {
          MapWork mapWork = (MapWork) w;
          HashMap<String, Operator<? extends OperatorDesc>> opMap = mapWork.getAliasToWork();
          if (!opMap.isEmpty()) {
            for (Operator<? extends OperatorDesc> op : opMap.values()) {
              setInputFormat(mapWork, op);
            }
          }
        }
      }
    } else if (task instanceof ConditionalTask) {
      List<Task<? extends Serializable>> listTasks
          = ((ConditionalTask) task).getListTasks();
      for (Task<? extends Serializable> tsk : listTasks) {
        setInputFormat(tsk);
      }
    }

    if (task.getChildTasks() != null) {
      for (Task<? extends Serializable> childTask : task.getChildTasks()) {
        setInputFormat(childTask);
      }
    }
  }

  private void setInputFormat(MapWork work, Operator<? extends OperatorDesc> op) {
    if (op.isUseBucketizedHiveInputFormat()) {
      work.setUseBucketizedHiveInputFormat(true);
      return;
    }

    if (op.getChildOperators() != null) {
      for (Operator<? extends OperatorDesc> childOp : op.getChildOperators()) {
        setInputFormat(work, childOp);
      }
    }
  }

  private void generateTaskTree(GenAXEProcContext procCtx) throws SemanticException {
    GenAXEWork genAXEWork = new GenAXEWork();
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<>();
    opRules.put(new RuleRegExp("Split Work - ReduceSink",
                               ReduceSinkOperator.getOperatorName() + "%"), genAXEWork);
    opRules.put(new TypeRule(MapJoinOperator.class), new AXEReduceSinkMapJoinProc());
    opRules.put(new RuleRegExp("Split Work + Move/Merge - FileSink",
                               FileSinkOperator.getOperatorName() + "%"),
                new CompositeProcessor(new AXEFileSinkProcessor(), genAXEWork));
    // opRules.put(new RuleRegExp("Handle Analyze Command",
    //             TableScanOperator.getOperatorName() + "%"), new AXEProcessAnalyzeTable());
    opRules.put(new RuleRegExp("Remember union", UnionOperator.getOperatorName() + "%"),
                new NodeProcessor() {
                  @Override
                  public Object process(Node n, Stack<Node> s,
                      NodeProcessorCtx procCtx, Object... os) {
                    GenAXEProcContext context = (GenAXEProcContext) procCtx;
                    UnionOperator union = (UnionOperator) n;

                    // simply need to remember that we've seen a union.
                    context.currentUnionOperators.add(union);
                    return null;
                  }
                }
    );
    opRules.put(new TypeRule(SMBMapJoinOperator.class),
                new NodeProcessor() {
                  @Override
                  public Object process(Node currNode, Stack<Node> stack,
                      NodeProcessorCtx procCtx, Object... os) {
                    GenAXEProcContext context = (GenAXEProcContext) procCtx;
                    SMBMapJoinOperator currSmbNode = (SMBMapJoinOperator) currNode;
                    AXESMBMapJoinInfo smbMapJoinCtx = context.smbMapJoinCtxMap.get(currSmbNode);
                    if (smbMapJoinCtx == null) {
                      smbMapJoinCtx = new AXESMBMapJoinInfo();
                      context.smbMapJoinCtxMap.put(currSmbNode, smbMapJoinCtx);
                    }

                    for (Node stackNode : stack) {
                      if (stackNode instanceof DummyStoreOperator) {
                        //If coming from small-table side, do some book-keeping, and skip traversal.
                        smbMapJoinCtx.smallTableRootOps.add(context.currentRootOperator);
                        return true;
                      }
                    }
                    //If coming from big-table side, do some book-keeping, and continue traversal
                    smbMapJoinCtx.bigTableRootOp = context.currentRootOperator;
                    return false;
                  }
                }
    );


    List<Node> topNodes = new ArrayList<Node>(procCtx.topOps.values());

    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    GraphWalker ogw = new GenAXEWorkWalker(disp, procCtx);
    ogw.startWalking(topNodes, null);
  }

  @Override
  protected void generateTaskTree(List<Task<? extends Serializable>> rootTasks, ParseContext pCtx,
      List<Task<MoveWork>> mvTask, Set<ReadEntity> inputs, Set<WriteEntity> outputs) throws SemanticException {
    PERF_LOGGER.PerfLogBegin(CLASS_NAME, PerfLogger.AXE_GENERATE_TASK_TREE);

    ParseContext tempParseContext = getParseContext(pCtx, rootTasks);
    GenAXEProcContext procCtx = new GenAXEProcContext(conf, tempParseContext, mvTask, rootTasks, inputs, outputs,
                                                      pCtx.getTopOps());
    generateTaskTree(procCtx);

    // -------------------------------- Post Pass ---------------------------------- //
    for (BaseWork w : procCtx.workWithUnionOperators) {
      removeUnionOperators(procCtx, w);
    }
    annotateMapWork(procCtx);
    for (FileSinkOperator fileSink : procCtx.fileSinkSet) {
      processFileSink(procCtx, fileSink);
    }

    PERF_LOGGER.PerfLogEnd(CLASS_NAME, PerfLogger.AXE_GENERATE_TASK_TREE);
  }


  private void processFileSink(GenAXEProcContext context, FileSinkOperator fileSink)
      throws SemanticException {

    ParseContext parseContext = context.parseContext;

    // Is INSERT OVERWRITE TABLE
    boolean isInsertTable = GenMapRedUtils.isInsertInto(parseContext, fileSink);
    HiveConf hconf = parseContext.getConf();

    boolean chDir = GenMapRedUtils.isMergeRequired(context.moveTask,
                                                   hconf, fileSink, context.currentTask, isInsertTable);
    // Set stats config for FileSinkOperators which are cloned from the fileSink
    List<FileSinkOperator> fileSinkList = context.fileSinkMap.get(fileSink);
    if (fileSinkList != null) {
      for (FileSinkOperator fsOp : fileSinkList) {
        fsOp.getConf().setGatherStats(fileSink.getConf().isGatherStats());
        fsOp.getConf().setStatsReliable(fileSink.getConf().isStatsReliable());
      }
    }

    Path finalName = GenMapRedUtils.createMoveTask(context.currentTask,
                                                   chDir, fileSink, parseContext, context.moveTask, hconf,
                                                   context.dependencyTask);

    if (chDir) {
      // Merge the files in the destination table/partitions by creating Map-only merge job
      // If underlying data is RCFile a RCFileBlockMerge task would be created.
      LOG.info("using CombineHiveInputFormat for the merge job");
      GenMapRedUtils.createMRWorkForMergingFiles(fileSink, finalName,
                                                 context.dependencyTask, context.moveTask,
                                                 hconf, context.currentTask);
    }

    FetchTask fetchTask = parseContext.getFetchTask();
    if (fetchTask != null && context.currentTask.getNumChild() == 0) {
      if (fetchTask.isFetchFrom(fileSink.getConf())) {
        context.currentTask.setFetchSource(true);
      }
    }
  }

  private void collectOperators(Operator<?> op, List<Operator<?>> opList) {
    opList.add(op);
    for (Object child : op.getChildOperators()) {
      if (child != null) {
        collectOperators((Operator<?>) child, opList);
      }
    }
  }

  private void removeUnionOperators(GenAXEProcContext context, BaseWork work) throws SemanticException {

    List<Operator<?>> roots = new ArrayList<>();

    // For MapWork, getAllRootOperators is not suitable, since it checks
    // getPathToAliases, and will return null if this is empty. Here we are
    // replacing getAliasToWork, so should use that information instead.
    if (work instanceof MapWork) {
      roots.addAll(((MapWork) work).getAliasToWork().values());
    } else {
      roots.addAll(work.getAllRootOperators());
    }
    if (work.getDummyOps() != null) {
      roots.addAll(work.getDummyOps());
    }

    // need to clone the plan.
    List<Operator<?>> newRoots = SerializationUtilities.cloneOperatorTree(roots);

    // Build a map to map the original FileSinkOperator and the cloned FileSinkOperators
    // This map is used for set the stats flag for the cloned FileSinkOperators in later process
    Iterator<Operator<?>> newRootsIt = newRoots.iterator();
    for (Operator<?> root : roots) {
      Operator<?> newRoot = newRootsIt.next();
      List<Operator<?>> newOpQueue = new LinkedList<>();
      collectOperators(newRoot, newOpQueue);
      List<Operator<?>> opQueue = new LinkedList<>();
      collectOperators(root, opQueue);
      Iterator<Operator<?>> newOpQueueIt = newOpQueue.iterator();
      for (Operator<?> op : opQueue) {
        Operator<?> newOp = newOpQueueIt.next();

        // We need to update rootToWorkMap in case the op is a key, since even
        // though we clone the op tree, we're still using the same MapWork/ReduceWork.
        if (context.rootToWorkMap.containsKey(op)) {
          context.rootToWorkMap.put(newOp, context.rootToWorkMap.get(op));
        }

        if (op instanceof FileSinkOperator) {
          List<FileSinkOperator> fileSinkList = context.fileSinkMap.get(op);
          if (fileSinkList == null) {
            fileSinkList = new LinkedList<>();
          }
          fileSinkList.add((FileSinkOperator) newOp);
          context.fileSinkMap.put((FileSinkOperator) op, fileSinkList);
        }
      }
    }

    // we're cloning the operator plan but we're retaining the original work. That means
    // that root operators have to be replaced with the cloned ops. The replacement map
    // tells you what that mapping is.
    Map<Operator<?>, Operator<?>> replacementMap = new HashMap<>();

    // there's some special handling for dummyOps required. Mapjoins won't be properly
    // initialized if their dummy parents aren't initialized. Since we cloned the plan
    // we need to replace the dummy operators in the work with the cloned ones.
    List<HashTableDummyOperator> dummyOps = new LinkedList<>();

    Iterator<Operator<?>> it = newRoots.iterator();
    for (Operator<?> orig : roots) {
      Operator<?> newRoot = it.next();
      if (newRoot instanceof HashTableDummyOperator) {
        dummyOps.add((HashTableDummyOperator) newRoot);
        it.remove();
      } else {
        replacementMap.put(orig, newRoot);
      }
    }

    // now we remove all the unions. we throw away any branch that's not reachable from
    // the current set of roots. The reason is that those branches will be handled in
    // different tasks.
    Deque<Operator<?>> operators = new LinkedList<>(newRoots);

    Set<Operator<?>> seen = new HashSet<>();

    while (!operators.isEmpty()) {
      Operator<?> current = operators.pop();
      seen.add(current);

      if (current instanceof UnionOperator) {
        Operator<?> parent = null;
        int count = 0;

        for (Operator<?> op : current.getParentOperators()) {
          if (seen.contains(op)) {
            ++count;
            parent = op;
          }
        }

        // we should have been able to reach the union from only one side.
        Preconditions.checkArgument(count <= 1,
                                    "AssertionError: expected count to be <= 1, but was " + count);

        if (parent == null) {
          // root operator is union (can happen in reducers)
          replacementMap.put(current, current.getChildOperators().get(0));
        } else {
          parent.removeChildAndAdoptItsChildren(current);
        }
      }

      if (current instanceof FileSinkOperator
          || current instanceof ReduceSinkOperator) {
        current.setChildOperators(null);
      } else {
        operators.addAll(current.getChildOperators());
      }
    }
    work.setDummyOps(dummyOps);
    work.replaceRoots(replacementMap);
  }

  private void annotateMapWork(GenAXEProcContext context) throws SemanticException {
    for (SMBMapJoinOperator smbMapJoinOp : context.smbMapJoinCtxMap.keySet()) {
      //initialize mapWork with smbMapJoin information.
      AXESMBMapJoinInfo smbMapJoinInfo = context.smbMapJoinCtxMap.get(smbMapJoinOp);
      MapWork work = smbMapJoinInfo.mapWork;
      annotateMapWork(context, work, smbMapJoinOp, (TableScanOperator) smbMapJoinInfo.bigTableRootOp, false);
      for (Operator<?> smallTableRootOp : smbMapJoinInfo.smallTableRootOps) {
        annotateMapWork(context, work, smbMapJoinOp, (TableScanOperator) smallTableRootOp, true);
      }
    }
  }
}
