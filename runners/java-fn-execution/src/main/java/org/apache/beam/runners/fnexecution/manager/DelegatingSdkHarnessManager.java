package org.apache.beam.runners.fnexecution.manager;

import com.google.auto.value.AutoValue;
import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Manages SDK harness containers and job resources on behalf of
 * worker operators.  It is responsible for initializing and storing
 * job resources for use.  It delegates instantiation tasks to the
 * factory classes passed into it.
 *
 * @TODO(axelmagn): extend to clean up unused containers and resources
 */
public class DelegatingSdkHarnessManager implements SdkHarnessManager {
  Logger log = LoggerFactory.getLogger(DelegatingSdkHarnessManager.class);




  // key: ProvisionApi.ProvisionInfo.job_id
  private final Map<String, JobResources> jobResources;

  // key: RunnerApi.Environment.url
  private final Map<String, EnvironmentResources> environmentResources;

  // TODO(axelmagn): operator resources
  // private final Map<String, ???> operatorResources;

  // unbounded lifetime resources

  private DelegatingSdkHarnessManager() {
    jobResources = new HashMap<>();
    environmentResources = new HashMap<>();
  }

  @Override
  public <InputT> RemoteBundle<InputT> getBundle(
      ProvisionApi.ProvisionInfo jobInfo,
      ExecutableStage executableStage,
      ArtifactSource artifactSource,
      StateRequestHandler stateRequestHandler) throws Exception {
    // TODO(axelmagn): provision job resources
    // TODO(axelmagn): register operation resources
    // TODO(axelmagn): provision environment resources
    // TODO(axelmagn): create bundle
    return null;
  }

  @Override
  public void close() throws Exception {
  }

  private JobResources getOrCreateJobResources(
    ProvisionApi.ProvisionInfo jobInfo,
    ExecutableStage executableStage,
    ArtifactSource artifactSource,
    StateRequestHandler stateRequestHandler) throws Exception {
    String jobKey = jobInfo.getJobId();
    synchronized (jobResources) {
      if (jobResources.containsKey(jobKey)) {
        return jobResources.get(jobKey);
      } else {
        // TODO(axelmagn): create logging, provision, artifact servers
        return null;
      }
    }
  }

  private GrpcFnServer<GrpcLoggingService> createLoggingService() throws IOException {

  }

}
