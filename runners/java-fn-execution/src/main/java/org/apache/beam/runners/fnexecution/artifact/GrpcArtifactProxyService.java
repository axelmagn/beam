package org.apache.beam.runners.fnexecution.artifact;


import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.runners.fnexecution.FnService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * An {@link ArtifactRetrievalService} implemented via gRPC.
 *
 * <p>Relies on one or more {@link ArtifactSource} for artifact retrieval.  All public methods are
 * threadsafe.
 */
public class GrpcArtifactProxyService
    extends ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceImplBase
    implements FnService, ArtifactRetrievalService {


  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcArtifactProxyService.class);

  public static GrpcArtifactProxyService create() {
    return new GrpcArtifactProxyService();
  }

  private ConcurrentHashMap<UUID, ArtifactSource> artifactSources;

  private GrpcArtifactProxyService() {
  }

  public UUID registerArtifactSource(ArtifactSource artifactSource) {
    UUID key = UUID.randomUUID();
    synchronized (artifactSources) {
      artifactSources.put(key, artifactSource);
    }
  }

  @Override
  public void getManifest(
      ArtifactApi.GetManifestRequest request,
      StreamObserver<ArtifactApi.GetManifestResponse> responseObserver) {
    try {
      ArtifactApi.Manifest manifest = artifactSource.getManifest();
      ArtifactApi.GetManifestResponse response =
          ArtifactApi.GetManifestResponse.newBuilder().setManifest(manifest).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.warn("Error retrieving manifest", e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Could not retrieve manifest.")
              .withCause(e)
              .asException());
    }
  }

  @Override
  public void getArtifact(
      ArtifactApi.GetArtifactRequest request,
      StreamObserver<ArtifactApi.ArtifactChunk> responseObserver) {
    try {
      artifactSource.getArtifact(request.getName(), responseObserver);
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Could not retrieve artifact.")
              .withCause(e)
              .asException());
    }
  }

  @Override
  public void close() throws Exception {
    // TODO: wrap up any open streams
    LOGGER.warn("GrpcArtifactProxyService.close() was called but is not yet implemented.");
  }
}
