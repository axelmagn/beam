package org.apache.beam.runners.flink.execution;

import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.environment.EnvironmentManager;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;

/**
 * A class that manages the long-lived resources of an individual job.
 *
 * <p>Only one harness environment is currently supported per job.
 */
public class JobResourceManager {

  /** Create a new JobResourceManager. */
  public static JobResourceManager create(
      ProvisionApi.ProvisionInfo jobInfo,
      RunnerApi.Environment environment,
      ArtifactSource artifactSource,
      JobResourceFactory jobResourceFactory) {
    return new JobResourceManager(
        jobInfo,
        environment,
        artifactSource,
        jobResourceFactory);
  }

  // job resources
  private final ProvisionApi.ProvisionInfo jobInfo;
  private final ArtifactSource artifactSource;
  private final RunnerApi.Environment environment;
  private final JobResourceFactory jobResourceFactory;

  // environment resources (will eventually need to support multiple environments)
  @Nullable private RemoteEnvironment remoteEnvironment = null;
  @Nullable private EnvironmentManager containerManager = null;

  private JobResourceManager (
      ProvisionApi.ProvisionInfo jobInfo,
      RunnerApi.Environment environment,
      ArtifactSource artifactSource,
      JobResourceFactory jobResourceFactory) {
    this.jobInfo = jobInfo;
    this.environment = environment;
    this.artifactSource = artifactSource;
    this.jobResourceFactory = jobResourceFactory;
  }

  /** Get a new environment session using the manager's resources. */
  public EnvironmentSession getSession() {
    if (!isStarted()) {
      throw new IllegalStateException("JobResourceManager has not been properly initialized.");
    }
    return new JobResourceEnvironmentSession(
        remoteEnvironment.getEnvironment(),
        artifactSource,
        remoteEnvironment.getClient()
    );
  }

  /**
   * Start all JobResourceManager resources that have a lifecycle, such as gRPC services and remote
   * environments.
   * @throws Exception
   */
  public void start() throws Exception {
    containerManager = jobResourceFactory.containerManager(artifactSource, jobInfo);
    remoteEnvironment = containerManager.getEnvironment(environment);
  }

  /**
   * Check if job resources have been successfully started and set.
   * @return true if all resources are started.
   */
  public boolean isStarted() {
    return containerManager != null
        && remoteEnvironment != null;
  }


}