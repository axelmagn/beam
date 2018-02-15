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
  private boolean isClosed = false;

  public JobResourceEnvironmentSession(
      RunnerApi.Environment environment,
      ArtifactSource artifactSource,
      SdkHarnessClient client) {
    this.environment = environment;
    this.artifactSource = artifactSource;
    this.client = client;
  }

  @Override
  public RunnerApi.Environment getEnvironment() throws EnvironmentSessionException {
    validateNotClosed();
    return environment;
  }

  @Override
  public ArtifactSource getArtifactSource() throws EnvironmentSessionException{
    validateNotClosed();
    return artifactSource;
  }

  @Override
  public SdkHarnessClient getClient() throws EnvironmentSessionException {
    validateNotClosed();
    return client;
  }

  private void validateNotClosed() throws EnvironmentSessionClosedException {
    if(this.isClosed) {
      throw new EnvironmentSessionClosedException();
    }
  }

  @Override
  public void close() throws Exception {
    // TODO: eventually use this for reference counting open sessions
    this.isClosed = true;
  }
}
