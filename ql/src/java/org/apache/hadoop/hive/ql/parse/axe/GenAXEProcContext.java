package org.apache.hadoop.hive.ql.parse.axe;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.DependencyCollectionTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.axe.AXETask;
import org.apache.hadoop.hive.ql.exec.axe.AXEWork;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.DependencyCollectionWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SparkEdgeProperty;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * GenAXEProcContext maintains information about the tasks and operators
 * as we walk the operator tree to break them into AXE tasks.
 *
 * Cloned from GenSparkProcContext
 *
 */
public class GenAXEProcContext implements NodeProcessorCtx {
  public final ParseContext parseContext;
  public final HiveConf conf;
  // rootTasks is the entry point for all generated tasks
  public final List<Task<? extends Serializable>> rootTasks;
  public final Set<ReadEntity> inputs;
  public final Set<WriteEntity> outputs;
  public final Map<Operator<?>, List<Operator<?>>> linkChildOpWithDummyOp;
  public final Map<BaseWork, List<ReduceSinkOperator>> linkWorkWithReduceSinkMap;
  public final Map<MapJoinOperator, List<BaseWork>> mapJoinWorkMap;
  public final Map<Operator<?>, List<BaseWork>> childToWorkMap;
  // Alias to operator map, from the semantic analyzer.
  // This is necessary as sometimes semantic analyzer's mapping is different than operator's own alias.
  public final Map<String, TableScanOperator> topOps;
  public final Map<MapJoinOperator, List<Operator<?>>> mapJoinParentMap;
  public final Set<MapJoinOperator> currentMapJoinOperators;
  public final Map<Operator<?>, Map<BaseWork, SparkEdgeProperty>> linkOpWithWorkMap;
  public final List<UnionOperator> currentUnionOperators;
  final DependencyCollectionTask dependencyTask;
  final List<Task<MoveWork>> moveTask;
  final Map<Operator<?>, BaseWork> rootToWorkMap;
  final Map<ReduceSinkOperator, ObjectPair<SparkEdgeProperty, ReduceWork>>
      leafOpToFollowingWorkInfo;
  // used to hook up unions
  final Set<BaseWork> workWithUnionOperators;
  final Set<FileSinkOperator> fileSinkSet;
  final Map<FileSinkOperator, List<FileSinkOperator>> fileSinkMap;
  final Map<SMBMapJoinOperator, AXESMBMapJoinInfo> smbMapJoinCtxMap;
  public Operator<? extends OperatorDesc> currentRootOperator;
  public Operator<? extends OperatorDesc> parentOfRoot;
  public AXETask currentTask;
  public BaseWork precedingWork;


  @SuppressWarnings("unchecked") GenAXEProcContext(HiveConf conf,
      ParseContext parseContext,
      List<Task<MoveWork>> moveTask,
      List<Task<? extends Serializable>> rootTasks,
      Set<ReadEntity> inputs,
      Set<WriteEntity> outputs,
      Map<String, TableScanOperator> topOps) {
    this.conf = conf;
    this.parseContext = parseContext;
    this.moveTask = moveTask;
    this.rootTasks = rootTasks;
    this.inputs = inputs;
    this.outputs = outputs;
    this.topOps = topOps;
    this.currentTask = (AXETask) TaskFactory.get(
        new AXEWork(conf.getVar(HiveConf.ConfVars.HIVEQUERYID)), conf);
    this.rootTasks.add(currentTask);
    this.rootToWorkMap = new LinkedHashMap<>();
    this.childToWorkMap = new LinkedHashMap<>();
    this.mapJoinParentMap = new LinkedHashMap<>();
    this.currentMapJoinOperators = new LinkedHashSet<>();
    this.mapJoinWorkMap = new LinkedHashMap<>();
    this.linkOpWithWorkMap = new LinkedHashMap<>();
    this.linkWorkWithReduceSinkMap = new LinkedHashMap<>();
    this.linkChildOpWithDummyOp = new LinkedHashMap<>();
    this.leafOpToFollowingWorkInfo = new LinkedHashMap<>();
    this.currentUnionOperators = new LinkedList<>();
    this.workWithUnionOperators = new LinkedHashSet<>();
    this.smbMapJoinCtxMap = new HashMap<>();
    this.fileSinkSet = new LinkedHashSet<>();
    this.fileSinkMap = new LinkedHashMap<>();
    this.dependencyTask = conf.getBoolVar(
        HiveConf.ConfVars.HIVE_MULTI_INSERT_MOVE_TASKS_SHARE_DEPENDENCIES)
        ? (DependencyCollectionTask) TaskFactory.get(new DependencyCollectionWork(), conf)
        : null;
  }
}
