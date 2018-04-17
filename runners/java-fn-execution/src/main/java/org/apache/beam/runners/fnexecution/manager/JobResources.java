package org.apache.beam.runners.fnexecution.manager;

import com.google.auto.value.AutoValue;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;

/**
 * Resources bound to the lifetime of a job.
 */
@AutoValue
abstract class JobResources {
  static JobResources create(
      GrpcFnServer<GrpcLoggingService> loggingServer,
      GrpcFnServer<StaticGrpcProvisionService> provisionServer,
      GrpcFnServer<ArtifactRetrievalService> artifactServer) {
    return new AutoValue_JobResources(
        loggingServer,
        provisionServer,
        artifactServer);
  }

  abstract GrpcFnServer<GrpcLoggingService> loggingServer();
  abstract GrpcFnServer<StaticGrpcProvisionService> provisionServer();
  abstract GrpcFnServer<ArtifactRetrievalService> artifactServer();
}
