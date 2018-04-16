package org.apache.beam.runners.fnexecution.manager;

import com.google.auto.value.AutoValue;
import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages SDK harness containers and job resources on behalf of
 * worker operators.  It is responsible for initializing and storing
 * job resources for use.  It delegates instantiation tasks to the
 * factory classes passed into it.
 *
 * @TODO(axelmagn): extend to clean up unused containers and resources
 */
public class DelegatingSdkHarnessManager implements SdkHarnessManager {

  /**
   * Resources bound to the lifetime of a job.
   * @TODO(axelmagn): identify environment resources
   */
  @AutoValue
  abstract static class JobResources {
    abstract GrpcFnServer<GrpcLoggingService> loggingServer();
    abstract GrpcFnServer<StaticGrpcProvisionService> provisionServer();
    abstract GrpcFnServer<ArtifactRetrievalService> retrievalServer();
  }

  /**
   * Resources bound to the lifetime of a container environment.
   * @TODO(axelmagn): identify environment resources
   */
  @AutoValue
  abstract static class EnvironmentResources {
    abstract ControlClientPool controlClientPool();
    abstract RemoteEnvironment remoteEnvironment();
    abstract GrpcFnServer<FnApiControlClientPoolService> controlServer();
  }

  // TODO(axelmagn): figure out if this needs to be threadsafe
  // key: ProvisionApi.ProvisionInfo.job_id
  private final Map<String, JobResources> jobResourcesCache;

  // TODO(axelmagn): figure out if this needs to be threadsafe
  // key: RunnerApi.Environment.url
  private final Map<String, EnvironmentResources> environmentResourcesCache;

  // unbounded lifetime resources


  public DelegatingSdkHarnessManager() {
    jobResourcesCache = new HashMap<>();
    environmentResourcesCache = new HashMap<>();
  }

  @Override
  public EnvironmentSession getSession(
      ProvisionApi.ProvisionInfo jobInfo,
      RunnerApi.Environment environment,
      ArtifactSource artifactSource) throws Exception {
    // TODO(axelmagn): create session from resources
    JobResources jobResources = getOrCreateJobResources(jobInfo);
    EnvironmentResources environmentResources = getOrCreateEnvironmentResources(environment);
    return null;
  }

  private JobResources getOrCreateJobResources(ProvisionApi.ProvisionInfo jobInfo) {
    // TODO(axelmagn)
    return null;
  }

  private EnvironmentResources getOrCreateEnvironmentResources(RunnerApi.Environment environment) {
    // TODO(axelmagn)
    return null;
  }
}
