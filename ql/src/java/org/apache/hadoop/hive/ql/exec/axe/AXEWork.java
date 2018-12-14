package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.ql.plan.AbstractOperatorDesc;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.SparkEdgeProperty;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Explain(displayName = "AXE", vectorization = Explain.Vectorization.SUMMARY_PATH)
public class AXEWork extends AbstractOperatorDesc {
  private static int counter;
  private final Map<BaseWork, List<BaseWork>> workGraph = new HashMap<>();
  private final Map<BaseWork, List<BaseWork>> invertedWorkGraph = new HashMap<>();
  private final String name;
  private final Set<BaseWork> roots = new LinkedHashSet<>();
  private final Set<BaseWork> leaves = new LinkedHashSet<>();
  private final Map<Pair<BaseWork, BaseWork>, SparkEdgeProperty> edgeProperties = new HashMap<>();

  public AXEWork(final String name) {
    this.name = name + ":" + (++counter);
  }

  @Explain(displayName = "DagName")
  public String getName() {
    return name;
  }

  public Set<BaseWork> getRoots() {
    return new LinkedHashSet<>(roots);
  }

  /**
   * add creates a new node in the graph without any connections
   */
  public void add(BaseWork w) {
    if (workGraph.containsKey(w)) {
      return;
    }
    workGraph.put(w, new LinkedList<BaseWork>());
    invertedWorkGraph.put(w, new LinkedList<BaseWork>());
    roots.add(w);
    leaves.add(w);
  }

  public void connect(BaseWork a, BaseWork b, SparkEdgeProperty edgeProp) {
    workGraph.get(a).add(b);
    invertedWorkGraph.get(b).add(a);
    roots.remove(b);
    leaves.remove(a);
    ImmutablePair<BaseWork, BaseWork> workPair = new ImmutablePair<>(a, b);
    edgeProperties.put(workPair, edgeProp);
  }

  public SparkEdgeProperty getEdgeProperty(BaseWork a, BaseWork b) {
    return edgeProperties.get(new ImmutablePair<>(a, b));
  }

  /**
   * @return a map of "vertex name" to BaseWork
   */
  @Explain(displayName = "Vertices", vectorization = Explain.Vectorization.SUMMARY_PATH)
  public Map<String, BaseWork> getWorkMap() {
    Map<String, BaseWork> result = new LinkedHashMap<>();
    for (BaseWork w : getAllWork()) {
      result.put(w.getName(), w);
    }
    return result;
  }

  @Explain(displayName = "Edges")
  public Map<ComparableName, List<Dependency>> getDependencyMap() {
    Map<String, String> allDependencies = new HashMap<>();
    Map<ComparableName, List<Dependency>> result =
        new LinkedHashMap<>();
    for (BaseWork baseWork : getAllWork()) {
      if (invertedWorkGraph.get(baseWork) != null && invertedWorkGraph.get(baseWork).size() > 0) {
        List<Dependency> dependencies = new LinkedList<>();
        for (BaseWork d : invertedWorkGraph.get(baseWork)) {
          allDependencies.put(baseWork.getName(), d.getName());
          Dependency dependency = new Dependency();
          dependency.w = d;
          dependency.prop = getEdgeProperty(d, baseWork);
          dependencies.add(dependency);
        }
        if (!dependencies.isEmpty()) {
          Collections.sort(dependencies);
          result.put(new ComparableName(allDependencies,
                                        baseWork.getName()), dependencies);
        }
      }
    }
    return result;
  }

  public List<BaseWork> getAllWork() {
    List<BaseWork> result = new LinkedList<>();
    Set<BaseWork> seen = new HashSet<>();
    for (BaseWork leaf : leaves) {
      // make sure all leaves are visited at least once
      visit(leaf, seen, result);
    }
    return result;
  }

  private void visit(BaseWork child, Set<BaseWork> seen, List<BaseWork> result) {
    if (seen.contains(child)) {
      // don't visit multiple times
      return;
    }
    seen.add(child);
    for (BaseWork parent : getParents(child)) {
      if (!seen.contains(parent)) {
        visit(parent, seen, result);
      }
    }
    result.add(child);
  }

  public List<BaseWork> getParents(BaseWork work) {
    Preconditions.checkArgument(invertedWorkGraph.containsKey(work),
                                "AssertionError: expected invertedWorkGraph.containsKey(work) to be true");
    Preconditions.checkArgument(invertedWorkGraph.get(work) != null,
                                "AssertionError: expected invertedWorkGraph.get(work) to be not null");
    return new LinkedList<>(invertedWorkGraph.get(work));
  }

  public List<BaseWork> getChildren(BaseWork work) {
    Preconditions.checkArgument(workGraph.containsKey(work),
                                "AssertionError: expected workGraph.containsKey(work) to be true");
    Preconditions.checkArgument(workGraph.get(work) != null,
                                "AssertionError: expected workGraph.get(work) to be not null");
    return new LinkedList<>(workGraph.get(work));
  }

  private static class ComparableName implements Comparable<ComparableName> {
    private final Map<String, String> dependencies;
    private final String name;

    ComparableName(Map<String, String> dependencies, String name) {
      this.dependencies = dependencies;
      this.name = name;
    }

    /**
     * Check if task n1 depends on task n2
     */
    boolean dependsOn(String n1, String n2) {
      for (String p = dependencies.get(n1); p != null; p = dependencies.get(p)) {
        if (p.equals(n2)) {
          return true;
        }
      }
      return false;
    }

    /**
     * Get the number of parents of task n
     */
    int getDepth(String n) {
      int depth = 0;
      for (String p = dependencies.get(n); p != null; p = dependencies.get(p)) {
        depth++;
      }
      return depth;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public int compareTo(ComparableName o) {
      if (dependsOn(name, o.name)) {
        // this depends on o
        return 1;
      }
      if (dependsOn(o.name, name)) {
        // o depends on this
        return -1;
      }
      // No dependency, check depth
      int d1 = getDepth(name);
      int d2 = getDepth(o.name);
      if (d1 == d2) {
        // Same depth, using natural order
        return name.compareTo(o.name);
      }
      // Deep one is bigger, i.e. less to the top
      return d1 > d2 ? 1 : -1;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /*
   * Dependency is a class used for explain
   */
  public class Dependency implements Serializable, Comparable<Dependency> {
    public BaseWork w;
    public SparkEdgeProperty prop;

    @Explain(displayName = "Name")
    public String getName() {
      return w.getName();
    }

    @Explain(displayName = "Shuffle Type")
    public String getShuffleType() {
      return prop.getShuffleType();
    }

    @Explain(displayName = "Number of Partitions")
    public String getNumPartitions() {
      return Integer.toString(prop.getNumPartitions());
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public int compareTo(Dependency o) {
      int compare = getName().compareTo(o.getName());
      if (compare == 0) {
        compare = getShuffleType().compareTo(o.getShuffleType());
      }
      return compare;
    }
  }

}
