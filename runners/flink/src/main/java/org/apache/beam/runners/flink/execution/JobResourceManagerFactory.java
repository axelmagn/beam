package org.apache.beam.runners.flink.execution;

import java.util.concurrent.ExecutorService;
import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.fnexecution.v1.ProvisionApi.ProvisionInfo;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;

/**
 * A Factory which creates {@link JobResourceManager JobResourceManagers}.
 */
public class JobResourceManagerFactory {
  public static JobResourceManagerFactory create() {
    return new JobResourceManagerFactory();
  }

  private JobResourceManagerFactory() {
  }

  public JobResourceManager create(
      ProvisionApi.ProvisionInfo jobInfo,
      RunnerApi.Environment environment,
      ArtifactSource artifactSource,
      ServerFactory serverFactory,
      ExecutorService executor) {
    return JobResourceManager.create(jobInfo, environment, artifactSource, serverFactory, executor);
  }
}