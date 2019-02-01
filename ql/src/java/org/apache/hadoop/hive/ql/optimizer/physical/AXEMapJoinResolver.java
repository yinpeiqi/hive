package org.apache.hadoop.hive.ql.optimizer.physical;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.axe.AXEHashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.axe.AXETask;
import org.apache.hadoop.hive.ql.exec.axe.AXEWork;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.BucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.ConditionalResolver;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverSkewJoin;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SparkBucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.axe.AXEHashTableSinkDesc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public class AXEMapJoinResolver implements PhysicalPlanResolver {

  // prevents a task from being processed multiple times
  private final Set<Task<? extends Serializable>> visitedTasks = new HashSet<>();

  public static Set<Operator<?>> getOp(BaseWork work, Class<?> clazz) {
    Set<Operator<?>> ops = new HashSet<>();
    if (work instanceof MapWork) {
      Collection<Operator<?>> opSet = ((MapWork) work).getAliasToWork().values();
      Stack<Operator<?>> opStack = new Stack<>();
      opStack.addAll(opSet);

      while (!opStack.empty()) {
        Operator<?> op = opStack.pop();
        ops.add(op);
        if (op.getChildOperators() != null) {
          opStack.addAll(op.getChildOperators());
        }
      }
    } else {
      ops.addAll(work.getAllOperators());
    }

    Set<Operator<? extends OperatorDesc>> matchingOps = new HashSet<>();
    for (Operator<? extends OperatorDesc> op : ops) {
      if (clazz.isInstance(op)) {
        matchingOps.add(op);
      }
    }
    return matchingOps;
  }

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {

    Dispatcher dispatcher = new AXEMapJoinTaskDispatcher(pctx);
    TaskGraphWalker graphWalker = new TaskGraphWalker(dispatcher);

    ArrayList<Node> topNodes = new ArrayList<>();
    topNodes.addAll(pctx.getRootTasks());
    graphWalker.startWalking(topNodes, null);
    return pctx;
  }

  // Check whether the specified BaseWork's operator tree contains a operator
  // of the specified operator class
  private boolean containsOp(BaseWork work, Class<?> clazz) {
    Set<Operator<?>> matchingOps = getOp(work, clazz);
    return matchingOps != null && !matchingOps.isEmpty();
  }

  private boolean containsOp(AXEWork axeWork, Class<?> clazz) {
    for (BaseWork work : axeWork.getAllWorkUnsorted()) {
      if (containsOp(work, clazz)) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  class AXEMapJoinTaskDispatcher implements Dispatcher {

    private final PhysicalContext physicalContext;

    // For each BaseWork with MJ operator, we build a AXEWork for its small table BaseWorks
    // This map records such information
    private final Map<BaseWork, AXEWork> axeWorkMap;

    // AXEWork dependency graph - from a AXEWork with MJ operators to all
    // of its parent AXEWorks for the small tables
    private final Map<AXEWork, List<AXEWork>> dependencyGraph;

    public AXEMapJoinTaskDispatcher(PhysicalContext pc) {
      super();
      physicalContext = pc;
      axeWorkMap = new LinkedHashMap<BaseWork, AXEWork>();
      dependencyGraph = new LinkedHashMap<AXEWork, List<AXEWork>>();
    }

    // Move the specified work from the axeWork to the targetWork
    // Note that, in order not to break the graph (since we need it for the edges),
    // we don't remove the work from the axeWork here. The removal is done later.
    private void moveWork(AXEWork axeWork, BaseWork work, AXEWork targetWork) {
      List<BaseWork> parentWorks = axeWork.getParents(work);
      if (axeWork != targetWork) {
        targetWork.add(work);

        // If any child work for this work is already added to the targetWork earlier,
        // we should connect this work with it
        for (BaseWork childWork : axeWork.getChildren(work)) {
          if (targetWork.contains(childWork)) {
            targetWork.connect(work, childWork, axeWork.getEdgeProperty(work, childWork));
          }
        }
      }

      if (!containsOp(work, MapJoinOperator.class)) {
        for (BaseWork parent : parentWorks) {
          moveWork(axeWork, parent, targetWork);
        }
      } else {
        // Create a new AXEWork for all the small tables of this work
        AXEWork parentWork =
            new AXEWork(physicalContext.conf.getVar(HiveConf.ConfVars.HIVEQUERYID));
        // copy cloneToWork to ensure RDD cache still works
        parentWork.setCloneToWork(axeWork.getCloneToWork());

        dependencyGraph.get(targetWork).add(parentWork);
        dependencyGraph.put(parentWork, new ArrayList<AXEWork>());

        // this work is now moved to the parentWork, thus we should
        // update this information in axeWorkMap
        axeWorkMap.put(work, parentWork);
        for (BaseWork parent : parentWorks) {
          if (containsOp(parent, AXEHashTableSinkOperator.class)) {
            moveWork(axeWork, parent, parentWork);
          } else {
            moveWork(axeWork, parent, targetWork);
          }
        }
      }
    }

    private void generateLocalWork(AXETask originalTask) {
      AXEWork originalWork = originalTask.getWork();
      Collection<BaseWork> allBaseWorks = originalWork.getAllWork();
      Context ctx = physicalContext.getContext();

      for (BaseWork work : allBaseWorks) {
        if (work.getMapRedLocalWork() == null) {
          if (containsOp(work, AXEHashTableSinkOperator.class) ||
              containsOp(work, MapJoinOperator.class)) {
            work.setMapRedLocalWork(new MapredLocalWork());
          }
          Set<Operator<?>> ops = getOp(work, MapJoinOperator.class);
          if (ops == null || ops.isEmpty()) {
            continue;
          }
          Path tmpPath = Utilities.generateTmpPath(ctx.getMRTmpPath(), originalTask.getId());
          MapredLocalWork bigTableLocalWork = work.getMapRedLocalWork();
          List<Operator<? extends OperatorDesc>> dummyOps =
              new ArrayList<Operator<? extends OperatorDesc>>(work.getDummyOps());
          bigTableLocalWork.setDummyParentOp(dummyOps);
          bigTableLocalWork.setTmpPath(tmpPath);

          // In one work, only one map join operator can be bucketed
          SparkBucketMapJoinContext bucketMJCxt = null;
          for (Operator<? extends OperatorDesc> op : ops) {
            MapJoinOperator mapJoinOp = (MapJoinOperator) op;
            MapJoinDesc mapJoinDesc = mapJoinOp.getConf();
            if (mapJoinDesc.isBucketMapJoin()) {
              bucketMJCxt = new SparkBucketMapJoinContext(mapJoinDesc);
              bucketMJCxt.setBucketMatcherClass(
                  org.apache.hadoop.hive.ql.exec.DefaultBucketMatcher.class);
              bucketMJCxt.setPosToAliasMap(mapJoinOp.getPosToAliasMap());
              ((MapWork) work).setUseBucketizedHiveInputFormat(true);
              bigTableLocalWork.setBucketMapjoinContext(bucketMJCxt);
              bigTableLocalWork.setInputFileChangeSensitive(true);
              break;
            }
          }

          for (BaseWork parentWork : originalWork.getParents(work)) {
            Set<Operator<?>> hashTableSinkOps =
                getOp(parentWork, AXEHashTableSinkOperator.class);
            if (hashTableSinkOps == null || hashTableSinkOps.isEmpty()) {
              continue;
            }
            MapredLocalWork parentLocalWork = parentWork.getMapRedLocalWork();
            parentLocalWork.setTmpHDFSPath(tmpPath);
            if (bucketMJCxt != null) {
              // We only need to update the work with the hashtable
              // sink operator with the same mapjoin desc. We can tell
              // that by comparing the bucket file name mapping map
              // instance. They should be exactly the same one due to
              // the way how the bucket mapjoin context is constructed.
              for (Operator<? extends OperatorDesc> op : hashTableSinkOps) {
                AXEHashTableSinkOperator hashTableSinkOp = (AXEHashTableSinkOperator) op;
                AXEHashTableSinkDesc hashTableSinkDesc = hashTableSinkOp.getConf();
                BucketMapJoinContext original = hashTableSinkDesc.getBucketMapjoinContext();
                if (original != null && original.getBucketFileNameMapping()
                    == bucketMJCxt.getBucketFileNameMapping()) {
                  ((MapWork) parentWork).setUseBucketizedHiveInputFormat(true);
                  parentLocalWork.setBucketMapjoinContext(bucketMJCxt);
                  parentLocalWork.setInputFileChangeSensitive(true);
                  break;
                }
              }
            }
          }
        }
      }
    }

    // Create a new AXETask for the specified AXEWork, recursively compute
    // all the parent AXETasks that this new task is depend on, if they don't already exists.
    private AXETask createAXETask(AXETask originalTask,
        AXEWork axeWork,
        Map<AXEWork, AXETask> createdTaskMap,
        ConditionalTask conditionalTask) {
      if (createdTaskMap.containsKey(axeWork)) {
        return createdTaskMap.get(axeWork);
      }
      AXETask resultTask = originalTask.getWork() == axeWork ?
          originalTask : (AXETask) TaskFactory.get(axeWork, physicalContext.conf);
      if (!dependencyGraph.get(axeWork).isEmpty()) {
        for (AXEWork parentWork : dependencyGraph.get(axeWork)) {
          AXETask parentTask =
              createAXETask(originalTask, parentWork, createdTaskMap, conditionalTask);
          parentTask.addDependentTask(resultTask);
        }
      } else {
        if (originalTask != resultTask) {
          List<Task<? extends Serializable>> parentTasks = originalTask.getParentTasks();
          if (parentTasks != null && parentTasks.size() > 0) {
            // avoid concurrent modification
            originalTask.setParentTasks(new ArrayList<Task<? extends Serializable>>());
            for (Task<? extends Serializable> parentTask : parentTasks) {
              parentTask.addDependentTask(resultTask);
              parentTask.removeDependentTask(originalTask);
            }
          } else {
            if (conditionalTask == null) {
              physicalContext.addToRootTask(resultTask);
              physicalContext.removeFromRootTask(originalTask);
            } else {
              updateConditionalTask(conditionalTask, originalTask, resultTask);
            }
          }
        }
      }

      createdTaskMap.put(axeWork, resultTask);
      return resultTask;
    }

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nos)
        throws SemanticException {
      Task<? extends Serializable> currentTask = (Task<? extends Serializable>) nd;
      if (currentTask.isMapRedTask()) {
        if (currentTask instanceof ConditionalTask) {
          List<Task<? extends Serializable>> taskList =
              ((ConditionalTask) currentTask).getListTasks();
          for (Task<? extends Serializable> tsk : taskList) {
            if (tsk instanceof AXETask) {
              processCurrentTask((AXETask) tsk, (ConditionalTask) currentTask);
              visitedTasks.add(tsk);
            }
          }
        } else if (currentTask instanceof AXETask) {
          processCurrentTask((AXETask) currentTask, null);
          visitedTasks.add(currentTask);
        }
      }

      return null;
    }

    /**
     * @param axeTask The current axe task we're processing.
     * @param conditionalTask If conditional task is not null, it means the current task is
     *                        wrapped in its task list.
     */
    private void processCurrentTask(AXETask axeTask, ConditionalTask conditionalTask) {
      AXEWork axeWork = axeTask.getWork();
      if (!visitedTasks.contains(axeTask)) {
        dependencyGraph.clear();
        axeWorkMap.clear();

        // Generate MapredLocalWorks for MJ and HTS
        generateLocalWork(axeTask);

        dependencyGraph.put(axeWork, new ArrayList<AXEWork>());
        Set<BaseWork> leaves = axeWork.getLeaves();
        for (BaseWork leaf : leaves) {
          moveWork(axeWork, leaf, axeWork);
        }

        // Now remove all BaseWorks in all the childAXEWorks that we created
        // from the original AXEWork
        for (AXEWork newAXEWork : axeWorkMap.values()) {
          for (BaseWork work : newAXEWork.getAllWorkUnsorted()) {
            axeWork.remove(work);
          }
        }

        Map<AXEWork, AXETask> createdTaskMap = new LinkedHashMap<AXEWork, AXETask>();

        // Now create AXETasks from the AXEWorks, also set up dependency
        for (AXEWork work : dependencyGraph.keySet()) {
          createAXETask(axeTask, work, createdTaskMap, conditionalTask);
        }
      } else if (conditionalTask != null) {
        // We may need to update the conditional task's list. This happens when a common map join
        // task exists in the task list and has already been processed. In such a case,
        // the current task is the map join task and we need to replace it with
        // its parent, i.e. the small table task.
        if (axeTask.getParentTasks() != null && axeTask.getParentTasks().size() == 1 &&
            axeTask.getParentTasks().get(0) instanceof AXETask) {
          AXETask parent = (AXETask) axeTask.getParentTasks().get(0);
          if (containsOp(axeWork, MapJoinOperator.class) &&
              containsOp(parent.getWork(), AXEHashTableSinkOperator.class)) {
            updateConditionalTask(conditionalTask, axeTask, parent);
          }
        }
      }
    }

    /**
     * Update the task/work list of this conditional task to replace originalTask with newTask.
     * For runtime skew join, also update dirToTaskMap for the conditional resolver
     */
    private void updateConditionalTask(ConditionalTask conditionalTask,
        AXETask originalTask, AXETask newTask) {
      ConditionalWork conditionalWork = conditionalTask.getWork();
      AXEWork originWork = originalTask.getWork();
      AXEWork newWork = newTask.getWork();
      List<Task<? extends Serializable>> listTask = conditionalTask.getListTasks();
      List<Serializable> listWork = (List<Serializable>) conditionalWork.getListWorks();
      int taskIndex = listTask.indexOf(originalTask);
      int workIndex = listWork.indexOf(originWork);
      if (taskIndex < 0 || workIndex < 0) {
        return;
      }
      listTask.set(taskIndex, newTask);
      listWork.set(workIndex, newWork);
      ConditionalResolver resolver = conditionalTask.getResolver();
      if (resolver instanceof ConditionalResolverSkewJoin) {
        // get bigKeysDirToTaskMap
        ConditionalResolverSkewJoin.ConditionalResolverSkewJoinCtx context =
            (ConditionalResolverSkewJoin.ConditionalResolverSkewJoinCtx) conditionalTask
                .getResolverCtx();
        HashMap<Path, Task<? extends Serializable>> bigKeysDirToTaskMap = context
            .getDirToTaskMap();
        // to avoid concurrent modify the hashmap
        HashMap<Path, Task<? extends Serializable>> newbigKeysDirToTaskMap =
            new HashMap<Path, Task<? extends Serializable>>();
        // reset the resolver
        for (Map.Entry<Path, Task<? extends Serializable>> entry :
            bigKeysDirToTaskMap.entrySet()) {
          Task<? extends Serializable> task = entry.getValue();
          Path bigKeyDir = entry.getKey();
          if (task.equals(originalTask)) {
            newbigKeysDirToTaskMap.put(bigKeyDir, newTask);
          } else {
            newbigKeysDirToTaskMap.put(bigKeyDir, task);
          }
        }
        context.setDirToTaskMap(newbigKeysDirToTaskMap);
        // update no skew task
        if (context.getNoSkewTask() != null && context.getNoSkewTask().equals(originalTask)) {
          List<Task<? extends Serializable>> noSkewTask = new ArrayList<>();
          noSkewTask.add(newTask);
          context.setNoSkewTask(noSkewTask);
        }
      }
    }
  }
}
