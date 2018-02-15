package org.apache.beam.runners.flink.execution;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;

/**
 * An environment session that is managed by a JobResourceManager.
 */
public class JobResourceEnvironmentSession implements EnvironmentSession {
  private final RunnerApi.Environment environment;
  private final ArtifactSource artifactSource;
  private final SdkHarnessClient client;

  public JobResourceEnvironmentSession(
      RunnerApi.Environment environment,
      ArtifactSource artifactSource,
      SdkHarnessClient client) {
    this.environment = environment;
    this.artifactSource = artifactSource;
    this.client = client;
  }

  @Override
  public RunnerApi.Environment getEnvironment() {
    return environment;
  }

  @Override
  public ArtifactSource getArtifactSource() {
    return artifactSource;
  }

  @Override
  public SdkHarnessClient getClient() {
    return client;
  }

  @Override
  public void close() throws Exception {
    // TODO: eventually use this for reference counting open sessions
  }
}
