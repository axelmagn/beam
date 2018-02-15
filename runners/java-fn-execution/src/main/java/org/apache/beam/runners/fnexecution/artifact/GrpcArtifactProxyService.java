package org.apache.beam.runners.fnexecution.artifact;


import io.grpc.stub.StreamObserver;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.runners.fnexecution.FnService;

import java.util.stream.Stream;

/**
 * An {@link ArtifactRetrievalService} implemented via gRPC.
 *
 * Relies on one or more {@link ArtifactSource} for artifact retrieval.
 */
public class GrpcArtifactProxyService
    extends ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceImplBase
    implements FnService, ArtifactRetrievalService {

  public static GrpcArtifactProxyService fromSource(ArtifactSource artifactSource) {
    return new GrpcArtifactProxyService(artifactSource);
  }

  private final ArtifactSource artifactSource;

  private GrpcArtifactProxyService(ArtifactSource artifactSource) {
    this.artifactSource = artifactSource;
  }

  @Override
  public void getManifest(
      ArtifactApi.GetManifestRequest request,
      StreamObserver<ArtifactApi.GetManifestResponse> responseObserver) {
    ArtifactApi.Manifest manifest = artifactSource.getManifest();
    ArtifactApi.GetManifestResponse response =
        ArtifactApi.GetManifestResponse.newBuilder().setManifest(manifest).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getArtifact(
      ArtifactApi.GetArtifactRequest request,
      StreamObserver<ArtifactApi.ArtifactChunk> responseObserver) {
    Stream<ArtifactApi.ArtifactChunk> artifactChunkStream =
        artifactSource.getArtifact(request.getName());
    artifactChunkStream.forEach(responseObserver::onNext);
    responseObserver.onCompleted();
  }

  @Override
  public void close() throws Exception {
    // TODO: wrap up any open streams
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
