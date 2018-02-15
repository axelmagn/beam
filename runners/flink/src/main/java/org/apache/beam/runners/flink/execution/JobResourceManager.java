package org.apache.beam.runners.flink.execution;

import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.artifact.GrpcArtifactProxyService;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClientControlService;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.ContainerManager;
import org.apache.beam.runners.fnexecution.environment.SingletonDockerContainerManager;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.logging.LogWriter;
import org.apache.beam.runners.fnexecution.logging.Slf4jLogWriter;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * A class that manages the long-lived resources of an individual job.
 */
public class JobResourceManager {

  public static JobResourceManager create(
      ProvisionApi.ProvisionInfo jobInfo,
      RunnerApi.Environment environment,
      ArtifactSource artifactSource,
      ServerFactory serverFactory,
      ExecutorService executor) {
    return new JobResourceManager(jobInfo, environment, artifactSource, serverFactory, executor);
  }

  // job resources
  private final ProvisionApi.ProvisionInfo jobInfo;
  private final ArtifactSource artifactSource;
  private final ServerFactory serverFactory;
  private final RunnerApi.Environment environment;
  private final ExecutorService executor;

  // environment resources (will eventually need to support multiple environments)
  @Nullable private RemoteEnvironment remoteEnvironment = null;
  @Nullable private ContainerManager containerManager = null;
  @Nullable private GrpcFnServer<GrpcLoggingService> loggingServiceServer = null;
  @Nullable private GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer = null;
  @Nullable private GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer = null;
  @Nullable private GrpcFnServer<SdkHarnessClientControlService> controlServiceServer = null;

  private JobResourceManager (
      ProvisionApi.ProvisionInfo jobInfo,
      RunnerApi.Environment environment,
      ArtifactSource artifactSource,
      ServerFactory serverFactory,
      ExecutorService executor) {
    this.jobInfo = jobInfo;
    this.environment = environment;
    this.artifactSource = artifactSource;
    this.serverFactory = serverFactory;
    this.executor = executor;
  }

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

  public void start() throws Exception {
    // logging service.
    LogWriter logWriter = Slf4jLogWriter.getDefault();
    GrpcLoggingService loggingService = GrpcLoggingService.forWriter(logWriter);
    loggingServiceServer = GrpcFnServer.allocatePortAndCreateFor(loggingService, serverFactory);

    // retrieval service.
    ArtifactRetrievalService retrievalService = GrpcArtifactProxyService.fromSource(artifactSource);
    retrievalServiceServer = GrpcFnServer.allocatePortAndCreateFor(retrievalService, serverFactory);

    // provisioning service
    StaticGrpcProvisionService provisioningService = StaticGrpcProvisionService.create(jobInfo);
    provisioningServiceServer =
        GrpcFnServer.allocatePortAndCreateFor(provisioningService, serverFactory);

    // control service
    FnDataService dataService = GrpcDataService.create(executor);
    SdkHarnessClientControlService controlService =
        SdkHarnessClientControlService.create(() -> dataService);
    controlServiceServer = GrpcFnServer.allocatePortAndCreateFor(controlService, serverFactory);

    // container manager
    containerManager =
        SingletonDockerContainerManager.forServers(
            controlServiceServer,
            loggingServiceServer,
            retrievalServiceServer,
            provisioningServiceServer);

    // remote environment
    remoteEnvironment = containerManager.getEnvironment(environment);
  }

  public boolean isStarted() {
    return this.loggingServiceServer != null
        && this.retrievalServiceServer != null
        && this.provisioningServiceServer != null
        && this.controlServiceServer != null
        && containerManager != null
        && remoteEnvironment != null;
  }


}
