package dev.ds_co.flink.streaming.api.operators.savepoint;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.ExceptionUtils;

public final class SavepointTestCluster implements AutoCloseable {

  private final File tempFolder;
  private MiniClusterWithClientResource cluster;
  private StreamExecutionEnvironment env1;
  private StreamExecutionEnvironment env2;
  private ClusterClient<?> client;

  public SavepointTestCluster(File tempFolder) {
    this.tempFolder = tempFolder;
  }

  public void start() throws Exception {
    File checkpointDir = new File(tempFolder, "checkpoints");
    File savepointDir = new File(tempFolder, "savepoints");
    if (!checkpointDir.mkdirs() || !savepointDir.mkdirs()) {
      throw new IllegalStateException("Could not create checkpoint/savepoint directories");
    }
    Configuration config = new Configuration();
    config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
    config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());

    cluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setConfiguration(config)
                .setNumberSlotsPerTaskManager(2)
                .setNumberTaskManagers(1)
                .build());
    cluster.before();

    env1 = StreamExecutionEnvironment.getExecutionEnvironment();
    env1.setParallelism(2);
    env1.setRuntimeMode(RuntimeExecutionMode.STREAMING);
    env1.enableCheckpointing(100);

    env2 = StreamExecutionEnvironment.getExecutionEnvironment();
    env2.setParallelism(2);
    env2.setRuntimeMode(RuntimeExecutionMode.STREAMING);
    env2.enableCheckpointing(100);

    client = cluster.getClusterClient();
  }

  public void prepareForJob1() {
    SavepointReadinessSignal.reset();
  }

  public StreamExecutionEnvironment env1() {
    return env1;
  }

  public StreamExecutionEnvironment env2() {
    return env2;
  }

  public CompletableFuture<JobID> submitJob1() {
    JobGraph jobGraph = env1.getStreamGraph().getJobGraph();
    return client.submitJob(jobGraph);
  }

  public String waitForStateAndSavepoint(JobID jobId, String key1, String key2) throws Exception {
    long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
    while (System.currentTimeMillis() < deadline) {
      if (SavepointReadinessSignal.allKeysReady(key1, key2)) {
        String savepointPath = triggerSavepointWithRetry(jobId, deadline);
        assertNotNull(savepointPath);
        client.cancel(jobId).get();
        return savepointPath;
      }
      Thread.sleep(10);
    }
    throw new AssertionError("State for keys " + key1 + "," + key2 + " not ready in time");
  }

  private String triggerSavepointWithRetry(JobID jobId, long deadline) throws Exception {
    Exception lastException = null;
    while (System.currentTimeMillis() < deadline) {
      try {
        return client.triggerSavepoint(jobId, null, SavepointFormatType.CANONICAL).get();
      } catch (Exception e) {
        lastException = e;
        String message = ExceptionUtils.stringifyException(e);
        if (message.contains("Checkpoint was declined")
            || message.contains("Task local checkpoint failure")) {
          Thread.sleep(100);
          continue;
        }
        throw e;
      }
    }
    throw new AssertionError("Savepoint failed after retries", lastException);
  }

  public void submitJob2AndWait(String savepointPath) throws Exception {
    JobGraph jobGraph = env2.getStreamGraph().getJobGraph();
    jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath, true));
    JobID jobId2 = client.submitJob(jobGraph).get();

    JobResult result = client.requestJobResult(jobId2).get(60, TimeUnit.SECONDS);
    if (!result.isSuccess()) {
      throw new AssertionError(
          "Job 2 failed: "
              + result
                  .getSerializedThrowable()
                  .map(ExceptionUtils::stringifyException)
                  .orElse("unknown cause"));
    }
  }

  @Override
  public void close() {
    if (cluster != null) {
      cluster.after();
    }
  }
}
