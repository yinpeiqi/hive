package org.apache.hadoop.hive.ql.parse.axe;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.axe.AXEWork;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.spark.GenSparkUtils;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SparkEdgeProperty;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import static org.apache.hadoop.hive.ql.parse.spark.GenSparkUtils.getEdgeProperty;

/**
 * GenAXEWork separates the operator tree into AXE tasks.
 * It is called once per leaf operator (operator that forces a new execution unit.)
 * and break the operators into work and tasks along the way.
 *
 * Cloned from GenTezWork.
 */
public class GenAXEWork implements NodeProcessor {
  static final private Logger LOG = LoggerFactory.getLogger(GenAXEWork.class.getName());
  static private int sequenceNumber = 0;

  public MapWork createMapWork(GenAXEProcContext context, Operator<?> root,
      AXEWork sparkWork, PrunedPartitionList partitions, boolean deferSetup) throws SemanticException {
    Preconditions.checkArgument(root.getParentOperators().isEmpty(),
                                "AssertionError: expected root.getParentOperators() to be empty");
    MapWork mapWork = new MapWork("Map " + (++sequenceNumber));
    LOG.debug("Adding map work (" + mapWork.getName() + ") for " + root);

    // map work starts with table scan operators
    Preconditions.checkArgument(root instanceof TableScanOperator,
                                "AssertionError: expected root to be an instance of TableScanOperator, but was "
                                    + root.getClass().getName());

    if (!deferSetup) {
      String alias = ((TableScanOperator) root).getConf().getAlias();
      GenMapRedUtils.setMapWork(mapWork, context.parseContext, context.inputs, partitions, (TableScanOperator) root,
                                alias, context.conf, false);
    }

    // add new item to the Spark work
    sparkWork.add(mapWork);

    return mapWork;
  }

  private BaseWork createNewWork(GenAXEProcContext context, AXEWork axeWork, Operator<?> root)
      throws SemanticException {
    SMBMapJoinOperator smbOp = GenSparkUtils.getChildOperator(root, SMBMapJoinOperator.class);
    if (context.precedingWork == null) {
      if (smbOp == null) {
        return createMapWork(context, root, axeWork, null, false);
      } else {
        MapWork work = createMapWork(context, root, axeWork, null, true);
        context.smbMapJoinCtxMap.get(smbOp).mapWork = work;
        return work;
      }
    } else {
      Preconditions.checkArgument(!root.getParentOperators().isEmpty(),
                                  "AssertionError: expected root.getParentOperators() to be non-empty");

      ReduceWork reduceWork = new ReduceWork("Reducer " + (++sequenceNumber));
      reduceWork.setReducer(root);
      reduceWork.setNeedsTagging(GenMapRedUtils.needsTagging(reduceWork));

      // Pick the maximum # reducers across all parents as the # of reduce tasks.
      int maxExecutors = -1;
      for (Operator<? extends OperatorDesc> parentOfRoot : root.getParentOperators()) {
        Preconditions.checkArgument(parentOfRoot instanceof ReduceSinkOperator,
                                    "AssertionError: expected parentOfRoot to be an "
                                        + "instance of ReduceSinkOperator, but was "
                                        + parentOfRoot.getClass().getName());
        ReduceSinkOperator reduceSink = (ReduceSinkOperator) parentOfRoot;
        maxExecutors = Math.max(maxExecutors, reduceSink.getConf().getNumReducers());
      }
      reduceWork.setNumReduceTasks(maxExecutors);

      ReduceSinkOperator reduceSink = (ReduceSinkOperator) context.parentOfRoot;
      // need to fill in information about the key and value in the reducer
      GenMapRedUtils.setKeyAndValueDesc(reduceWork, reduceSink);

      // remember which parent belongs to which tag
      reduceWork.getTagToInput().put(reduceSink.getConf().getTag(),
                                     context.precedingWork.getName());

      // remember the output name of the reduce sink
      reduceSink.getConf().setOutputName(reduceWork.getName());
      axeWork.add(reduceWork);
      SparkEdgeProperty edgeProp = getEdgeProperty(reduceSink, reduceWork);

      axeWork.connect(context.precedingWork, reduceWork, edgeProp);
      return reduceWork;
    }
  }

  @Override public Object process(final Node nd, final Stack<Node> stack, final NodeProcessorCtx procCtx,
      final Object... nodeOutputs)
      throws SemanticException {
    GenAXEProcContext context = (GenAXEProcContext) procCtx;

    Preconditions.checkArgument(context != null,
                                "AssertionError: expected context to be not null");
    Preconditions.checkArgument(context.currentTask != null,
                                "AssertionError: expected context.currentTask to be not null");
    Preconditions.checkArgument(context.currentRootOperator != null,
                                "AssertionError: expected context.currentRootOperator to be not null");

    Operator<? extends OperatorDesc> operator = (Operator<? extends OperatorDesc>) nd;

    Operator<?> root = context.currentRootOperator;

    LOG.debug("Root operator: " + root);
    LOG.debug("Leaf operator: " + operator);

    AXEWork axeWork = context.currentTask.getWork();

    /* Get the AXE task that the current node belongs to */
    BaseWork work;

    // If there is no preceding work we have a root and will
    // generate a map vertex. If there is a preceding work we
    // will generate a reduce vertex
    if (context.rootToWorkMap.containsKey(root)) {
      work = context.rootToWorkMap.get(root);
    } else {
      work = createNewWork(context, axeWork, root);
      context.rootToWorkMap.put(root, work);
    }

    if (!context.childToWorkMap.containsKey(operator)) {
      List<BaseWork> workItems = new LinkedList<>();
      workItems.add(work);
      context.childToWorkMap.put(operator, workItems);
    } else {
      context.childToWorkMap.get(operator).add(work);
    }

    if (!context.currentMapJoinOperators.isEmpty()) {
      for (MapJoinOperator mj : context.currentMapJoinOperators) {
        // remember the mapping in case we scan another branch of the mapjoin later
        if (!context.mapJoinWorkMap.containsKey(mj)) {
          List<BaseWork> workItems = new LinkedList<>();
          workItems.add(work);
          context.mapJoinWorkMap.put(mj, workItems);
        } else {
          context.mapJoinWorkMap.get(mj).add(work);
        }

        /*
         * this happens in case of map join operations.
         * The tree looks like this:
         *
         *        RS <--- we are here perhaps
         *        |
         *     MapJoin
         *     /     \
         *   RS       TS
         *  /
         * TS
         *
         * If we are at the RS pointed above, and we may have already visited the
         * RS following the TS, we have already generated work for the TS-RS.
         * We need to hook the current work to this generated work.
         */
        if (context.linkOpWithWorkMap.containsKey(mj)) {
          Map<BaseWork, SparkEdgeProperty> linkWorkMap = context.linkOpWithWorkMap.get(mj);
          if (linkWorkMap != null) {
            if (context.linkChildOpWithDummyOp.containsKey(mj)) {
              for (Operator<?> dummy : context.linkChildOpWithDummyOp.get(mj)) {
                work.addDummyOp((HashTableDummyOperator) dummy);
              }
            }
            for (Map.Entry<BaseWork, SparkEdgeProperty> parentWorkMap : linkWorkMap.entrySet()) {
              BaseWork parentWork = parentWorkMap.getKey();
              LOG.debug("connecting " + parentWork.getName() + " with " + work.getName());
              SparkEdgeProperty edgeProp = parentWorkMap.getValue();
              axeWork.connect(parentWork, work, edgeProp);

              // need to set up output name for reduce sink now that we know the name
              // of the downstream work
              for (ReduceSinkOperator r : context.linkWorkWithReduceSinkMap.get(parentWork)) {
                if (r.getConf().getOutputName() != null) {
                  LOG.debug("Cloning reduce sink for multi-child broadcast edge");
                  // we've already set this one up. Need to clone for the next work.
                  r = (ReduceSinkOperator) OperatorFactory.getAndMakeChild(
                      r.getCompilationOpContext(), (ReduceSinkDesc) r.getConf().clone(),
                      r.getParentOperators());
                }
                r.getConf().setOutputName(work.getName());
              }
            }
          }
        }
      }
      // clear out the set. we don't need it anymore.
      context.currentMapJoinOperators.clear();
    }

    // Here we are disconnecting root with its parents. However, we need to save
    // a few information, since in future we may reach the parent operators via a
    // different path, and we may need to connect parent works with the work associated
    // with this root operator.
    if (root.getNumParent() > 0) {
      Preconditions.checkArgument(work instanceof ReduceWork,
                                  "AssertionError: expected work to be a ReduceWork, but was " + work.getClass()
                                                                                                     .getName());
      ReduceWork reduceWork = (ReduceWork) work;
      for (Operator<?> parent : new ArrayList<>(root.getParentOperators())) {
        Preconditions.checkArgument(parent instanceof ReduceSinkOperator,
                                    "AssertionError: expected operator to be a ReduceSinkOperator, but was "
                                        + parent.getClass().getName());
        ReduceSinkOperator rsOp = (ReduceSinkOperator) parent;
        SparkEdgeProperty edgeProp = getEdgeProperty(rsOp, reduceWork);

        rsOp.getConf().setOutputName(reduceWork.getName());
        GenMapRedUtils.setKeyAndValueDesc(reduceWork, rsOp);

        context.leafOpToFollowingWorkInfo.put(rsOp, ObjectPair.create(edgeProp, reduceWork));
        root.removeParent(parent);
      }
    }

    // If `currentUnionOperators` is not empty, it means we are creating
    // BaseWork whose operator tree contains union operators. In this case,
    // we need to save these BaseWorks, and remove the union operators
    // from the operator tree later.
    if (!context.currentUnionOperators.isEmpty()) {
      context.currentUnionOperators.clear();
      context.workWithUnionOperators.add(work);
    }

    // We're scanning a tree from roots to leaf (this is not technically
    // correct, demux and mux operators might form a diamond shape, but
    // we will only scan one path and ignore the others, because the
    // diamond shape is always contained in a single vertex). The scan
    // is depth first and because we remove parents when we pack a pipeline
    // into a vertex we will never visit any node twice. But because of that
    // we might have a situation where we need to connect 'work' that comes after
    // the 'work' we're currently looking at.
    //
    // Also note: the concept of leaf and root is reversed in hive for historical
    // reasons. Roots are data sources, leaves are data sinks. I know.
    if (context.leafOpToFollowingWorkInfo.containsKey(operator)) {
      ObjectPair<SparkEdgeProperty, ReduceWork> childWorkInfo = context.
          leafOpToFollowingWorkInfo.get(operator);
      SparkEdgeProperty edgeProp = childWorkInfo.getFirst();
      ReduceWork childWork = childWorkInfo.getSecond();

      LOG.debug("Second pass. Leaf operator: " + operator + " has common downstream work:" + childWork);

      // We may have already connected `work` with `childWork`, in case, for example, lateral view:
      //    TS
      //     |
      //    ...
      //     |
      //    LVF
      //     | \
      //    SEL SEL
      //     |    |
      //    LVJ-UDTF
      //     |
      //    SEL
      //     |
      //    RS
      // Here, RS can be reached from TS via two different paths. If there is any child work after RS,
      // we don't want to connect them with the work associated with TS more than once.
      if (axeWork.getEdgeProperty(work, childWork) == null) {
        axeWork.connect(work, childWork, edgeProp);
      } else {
        LOG.debug("work " + work.getName() + " is already connected to " + childWork.getName() + " before");
      }
    } else {
      LOG.debug("First pass. Leaf operator: " + operator);
    }

    // No children means we're at the bottom. If there are more operators to scan
    // the next item will be a new root.
    if (!operator.getChildOperators().isEmpty()) {
      Preconditions.checkArgument(operator.getChildOperators().size() == 1,
                                  "AssertionError: expected operator.getChildOperators().size() to be 1, but was "
                                      + operator.getChildOperators().size());
      context.parentOfRoot = operator;
      context.currentRootOperator = operator.getChildOperators().get(0);
      context.precedingWork = work;
    }
    return null;
  }
}
