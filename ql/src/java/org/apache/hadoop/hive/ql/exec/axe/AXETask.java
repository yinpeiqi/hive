package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AXETask extends Task<AXEWork> {
  protected final Logger LOG = LoggerFactory.getLogger(AXETask.class);
  private Gson gson = new GsonBuilder().disableHtmlEscaping().create();
  private String jsonPath;
  private String jobSubmit;

  @Override protected int execute(final DriverContext driverContext) {
    int rc = 0;
    jsonPath = this.conf.getVar(HiveConf.ConfVars.AXE_JOB_DESC_PATH);
    jobSubmit = conf.getVar(HiveConf.ConfVars.AXE_JOB_SUBMIT);
    generateJobSpecJson();
    System.out.println("JobDesc json is written to " + jsonPath);
    submitToAXE(driverContext);
    return rc;
  }

  private void submitToAXE(DriverContext driverContext) {
    try {
      AXEClient client = new AXEClient(driverContext);
      client.submitJob(jobSubmit, jsonPath);
    } catch (IOException e) {
      LOG.error(e.getMessage());
      e.printStackTrace();
    }
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void generateJobSpecJson() {
    AXEJobDesc jobDesc = new AXEJobDesc();

    Map<BaseWork, AXEOperator> workAXEOperatorMap = new HashMap<>();
    // Specify tasks
    for (BaseWork task : work.getAllWork()) {
      String taskName = task.getName();
      if (task instanceof MapWork) {
        MapWork mapWork = (MapWork) task;
        for (Map.Entry<String, Operator<? extends OperatorDesc>> ops : mapWork.getAliasToWork().entrySet()) {
          Preconditions.checkArgument(ops.getValue() instanceof TableScanOperator,
                                      "The root of MapWork is expected to be a TableScanOperator, but was "
                                          + ops.getValue().getClass().getName());
          AXEOperator operator = jobDesc.addMapTask(taskName, (TableScanOperator) ops.getValue());
          workAXEOperatorMap.put(mapWork, operator);
        }
      } else if (task instanceof ReduceWork) {
        ReduceWork reduceWork = (ReduceWork) task;
        workAXEOperatorMap.put(reduceWork, jobDesc.addReduceTask(taskName, reduceWork.getReducer()));
      } else {
        throw new IllegalStateException("AssertionError: expected either MapWork or ReduceWork, "
                                            + "but found " + work.getClass().getName());
      }
    }

    // Specify dependency
    Set<BaseWork> expandedWork = new HashSet<>();
    for (BaseWork task : work.getRoots()) {
      addDependenciesToJson(task, workAXEOperatorMap, expandedWork);
    }

    // Write to json file
    try {
      File jsonFile = new File(jsonPath);
      jsonFile.getParentFile().mkdirs();
      jsonFile.createNewFile();
      Writer writer = new FileWriter(jsonFile);
      gson.toJson(jobDesc.output, writer);
      writer.close();
    } catch (IOException e) {
      LOG.error("Error writing to json file " + jsonPath);
      e.printStackTrace();
    }
  }

  private void addDependenciesToJson(BaseWork task, Map<BaseWork, AXEOperator> workAXEOperatorMap,
      Set<BaseWork> expandedWork) {
    if (expandedWork.contains(task)) {
      return;
    }
    expandedWork.add(task);
    AXEOperator parentOp = workAXEOperatorMap.get(task);
    List<BaseWork> children = work.getChildren(task);
    for (BaseWork child : children) {
      AXEOperator childOp = workAXEOperatorMap.get(child);
      parentOp.addChild(childOp.id);
      childOp.addParent(parentOp.id);
      addDependenciesToJson(child, workAXEOperatorMap, expandedWork);
    }
  }

  @Override
  public void updateTaskMetrics(Metrics metrics) {
    metrics.incrementCounter("hive_axe_tasks");
  }

  @Override
  public boolean isMapRedTask() {
    return true;
  }

  @Override
  public StageType getType() {
    return StageType.MAPRED;
  }

  @Override public String getName() {
    return "AXE";
  }

  @Override
  public Collection<MapWork> getMapWork() {
    List<MapWork> result = Lists.newArrayList();
    for (BaseWork w : getWork().getRoots()) {
      result.add((MapWork) w);
    }
    return result;
  }

  @Override
  public Operator<? extends OperatorDesc> getReducer(MapWork mapWork) {
    List<BaseWork> children = getWork().getChildren(mapWork);
    if (children.size() != 1 || (!(children.get(0) instanceof ReduceWork))) {
      return null;
    }
    return ((ReduceWork) children.get(0)).getReducer();
  }


}
