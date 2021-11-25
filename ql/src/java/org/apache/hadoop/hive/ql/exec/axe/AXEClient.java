package org.apache.hadoop.hive.ql.exec.axe;

import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.StreamPrinter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

@SuppressWarnings("FieldCanBeLocal")
class AXEClient {
  private DriverContext driverContext;
  private int nJobProcesses = 5;
  private int parallelism = 10;

  AXEClient(final DriverContext driverContext) {
    this.driverContext = driverContext;
  }

  void submitJob(final String jobSubmit, final String jsonPath) throws IOException {
    String configFile = createConfigFile(jsonPath);
    String cmdLine = "python " + jobSubmit + "/submit-job.py --config " + jobSubmit + "/job.json";
    // Run ExecDriver in another JVM
    Process executor = Runtime.getRuntime().exec(cmdLine);
    // FIXME: job status monitor
  }

  @SuppressWarnings("StringBufferReplaceableByString")
  private String createConfigFile(final String jsonPath) throws IOException {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("n_job_processes=").append(nJobProcesses).append("\n");
    stringBuilder.append("parallelism=").append(parallelism).append("\n");
    stringBuilder.append("sql_json_file=").append(jsonPath).append("\n");

    File configFile = File.createTempFile(driverContext.getCtx().getLocalTmpPath().toString(), "job.json");
    FileWriter writer = new FileWriter(configFile);
    writer.write(stringBuilder.toString());
    return configFile.getAbsolutePath();
  }
}
