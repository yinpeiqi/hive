package org.apache.hadoop.hive.ql.optimizer.axe;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.axe.AXETask;
import org.apache.hadoop.hive.ql.exec.axe.AXEWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalContext;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalPlanResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SparkEdgeProperty;

public class CombineEquivalentWorkResolver implements PhysicalPlanResolver {
  protected static transient Logger LOG = LoggerFactory.getLogger(CombineEquivalentWorkResolver.class);

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());
    TaskGraphWalker taskWalker = new TaskGraphWalker(new EquivalentWorkMatcher());
    HashMap<Node, Object> nodeOutput = Maps.newHashMap();
    taskWalker.startWalking(topNodes, nodeOutput);
    return pctx;
  }

  class EquivalentWorkMatcher implements Dispatcher {
    private Comparator<BaseWork> baseWorkComparator = new Comparator<BaseWork>() {
      @Override
      public int compare(BaseWork o1, BaseWork o2) {
        return o1.getName().compareTo(o2.getName());
      }
    };

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {
      if (nd instanceof AXETask) {
        AXETask axeTask = (AXETask) nd;
        AXEWork axeWork = axeTask.getWork();
        Set<BaseWork> roots = axeWork.getRoots();
        compareWorksRecursively(roots, axeWork);
      }
      return null;
    }

    private void compareWorksRecursively(Set<BaseWork> works, AXEWork axeWork) {
      // find out all equivalent works in the Set.
      Set<Set<BaseWork>> equivalentWorks = compareChildWorks(works, axeWork);
      // combine equivalent work into single one in AXEWork's work graph.
      Set<BaseWork> removedWorks = combineEquivalentWorks(equivalentWorks, axeWork);

      // try to combine next level works recursively.
      for (BaseWork work : works) {
        if (!removedWorks.contains(work)) {
          Set<BaseWork> children = Sets.newHashSet();
          children.addAll(axeWork.getChildren(work));
          if (children.size() > 0) {
            compareWorksRecursively(children, axeWork);
          }
        }
      }
    }

    private Set<Set<BaseWork>> compareChildWorks(Set<BaseWork> children, AXEWork axeWork) {
      Set<Set<BaseWork>> equivalentChildren = Sets.newHashSet();
      if (children.size() > 1) {
        for (BaseWork work : children) {
          boolean assigned = false;
          for (Set<BaseWork> set : equivalentChildren) {
            if (belongToSet(set, work, axeWork)) {
              set.add(work);
              assigned = true;
              break;
            }
          }
          if (!assigned) {
            // sort the works so that we get consistent query plan for multi executions(for test verification).
            Set<BaseWork> newSet = Sets.newTreeSet(baseWorkComparator);
            newSet.add(work);
            equivalentChildren.add(newSet);
          }
        }
      }
      return equivalentChildren;
    }

    private boolean belongToSet(Set<BaseWork> set, BaseWork work, AXEWork axeWork) {
      if (set.isEmpty()) {
        return true;
      } else if (compareWork(set.iterator().next(), work, axeWork)) {
        return true;
      }
      return false;
    }

    private Set<BaseWork> combineEquivalentWorks(Set<Set<BaseWork>> equivalentWorks, AXEWork axeWork) {
      Set<BaseWork> removedWorks = Sets.newHashSet();
      for (Set<BaseWork> workSet : equivalentWorks) {
        if (workSet.size() > 1) {
          Iterator<BaseWork> iterator = workSet.iterator();
          BaseWork first = iterator.next();
          while (iterator.hasNext()) {
            BaseWork next = iterator.next();
            replaceWork(next, first, axeWork);
            removedWorks.add(next);
          }
        }
      }
      return removedWorks;
    }

    private void replaceWork(BaseWork previous, BaseWork current, AXEWork axeWork) {
      updateReference(previous, current, axeWork);
      List<BaseWork> parents = axeWork.getParents(previous);
      List<BaseWork> children = axeWork.getChildren(previous);
      if (parents != null) {
        for (BaseWork parent : parents) {
          // we do not need to connect its parent to its counterpart, as they have the same parents.
          axeWork.disconnect(parent, previous);
        }
      }
      if (children != null) {
        for (BaseWork child : children) {
          SparkEdgeProperty edgeProperty = axeWork.getEdgeProperty(previous, child);
          axeWork.disconnect(previous, child);
          axeWork.connect(current, child, edgeProperty);
        }
      }
      axeWork.remove(previous);
    }

    /*
    * update the Work name which referred by Operators in following Works.
    */
    private void updateReference(BaseWork previous, BaseWork current, AXEWork axeWork) {
      String previousName = previous.getName();
      String currentName = current.getName();
      List<BaseWork> children = axeWork.getAllWork();
      for (BaseWork child : children) {
        Set<Operator<?>> allOperators = child.getAllOperators();
        for (Operator<?> operator : allOperators) {
          if (operator instanceof MapJoinOperator) {
            MapJoinDesc mapJoinDesc = ((MapJoinOperator) operator).getConf();
            Map<Integer, String> parentToInput = mapJoinDesc.getParentToInput();
            for (Integer id : parentToInput.keySet()) {
              String parent = parentToInput.get(id);
              if (parent.equals(previousName)) {
                parentToInput.put(id, currentName);
              }
            }
          }
        }
      }
    }

    private boolean compareWork(BaseWork first, BaseWork second, AXEWork axeWork) {

      if (!first.getClass().getName().equals(second.getClass().getName())) {
        return false;
      }

      if (!hasSameParent(first, second, axeWork)) {
        return false;
      }

      // leave work's output may be read in further AXEWork/FetchWork, we should not combine
      // leave works without notifying further AXEWork/FetchWork.
      if (axeWork.getLeaves().contains(first) && axeWork.getLeaves().contains(second)) {
        return false;
      }

      // need to check paths and partition desc for MapWorks
      if (first instanceof MapWork && !compareMapWork((MapWork) first, (MapWork) second)) {
        return false;
      }

      Set<Operator<?>> firstRootOperators = first.getAllRootOperators();
      Set<Operator<?>> secondRootOperators = second.getAllRootOperators();
      if (firstRootOperators.size() != secondRootOperators.size()) {
        return false;
      }

      Iterator<Operator<?>> firstIterator = firstRootOperators.iterator();
      Iterator<Operator<?>> secondIterator = secondRootOperators.iterator();
      while (firstIterator.hasNext()) {
        boolean result = compareOperatorChain(firstIterator.next(), secondIterator.next());
        if (!result) {
          return result;
        }
      }

      return true;
    }

    private boolean compareMapWork(MapWork first, MapWork second) {
      Map<Path, PartitionDesc> pathToPartition1 = first.getPathToPartitionInfo();
      Map<Path, PartitionDesc> pathToPartition2 = second.getPathToPartitionInfo();
      if (pathToPartition1.size() == pathToPartition2.size()) {
        for (Map.Entry<Path, PartitionDesc> entry : pathToPartition1.entrySet()) {
          Path path1 = entry.getKey();
          PartitionDesc partitionDesc1 = entry.getValue();
          PartitionDesc partitionDesc2 = pathToPartition2.get(path1);
          if (!partitionDesc1.equals(partitionDesc2)) {
            return false;
          }
        }
        return true;
      }
      return false;
    }

    private boolean hasSameParent(BaseWork first, BaseWork second, AXEWork axeWork) {
      boolean result = true;
      List<BaseWork> firstParents = axeWork.getParents(first);
      List<BaseWork> secondParents = axeWork.getParents(second);
      if (firstParents.size() != secondParents.size()) {
        result = false;
      }
      for (BaseWork parent : firstParents) {
        if (!secondParents.contains(parent)) {
          result = false;
          break;
        }
      }
      return result;
    }

    private boolean compareOperatorChain(Operator<?> firstOperator, Operator<?> secondOperator) {
      boolean result = compareCurrentOperator(firstOperator, secondOperator);
      if (!result) {
        return result;
      }

      List<Operator<? extends OperatorDesc>> firstOperatorChildOperators = firstOperator.getChildOperators();
      List<Operator<? extends OperatorDesc>> secondOperatorChildOperators = secondOperator.getChildOperators();
      if (firstOperatorChildOperators == null && secondOperatorChildOperators != null) {
        return false;
      } else if (firstOperatorChildOperators != null && secondOperatorChildOperators == null) {
        return false;
      } else if (firstOperatorChildOperators != null && secondOperatorChildOperators != null) {
        if (firstOperatorChildOperators.size() != secondOperatorChildOperators.size()) {
          return false;
        }
        int size = firstOperatorChildOperators.size();
        for (int i = 0; i < size; i++) {
          result = compareOperatorChain(firstOperatorChildOperators.get(i), secondOperatorChildOperators.get(i));
          if (!result) {
            return false;
          }
        }
      }

      return true;
    }

    /**
     * Compare Operators through their Explain output string.
     *
     * @param firstOperator
     * @param secondOperator
     * @return
     */
    private boolean compareCurrentOperator(Operator<?> firstOperator, Operator<?> secondOperator) {
      if (!firstOperator.getClass().getName().equals(secondOperator.getClass().getName())) {
        return false;
      }

      return firstOperator.logicalEquals(secondOperator);
    }
  }
}
