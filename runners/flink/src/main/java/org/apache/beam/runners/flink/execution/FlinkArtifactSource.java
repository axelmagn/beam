package org.apache.beam.runners.flink.execution;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.stream.Stream;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.Manifest;
import org.apache.beam.runners.flink.FlinkCachedArtifactPaths;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.flink.api.common.cache.DistributedCache;

/**
 * An {@link org.apache.beam.runners.fnexecution.artifact.ArtifactSource} that draws artifacts
 * from the Flink Distributed File Cache {@link org.apache.flink.api.common.cache.DistributedCache}.
 */
public class FlinkArtifactSource implements ArtifactSource {
  private static final int DEFAULT_CHUNK_SIZE = 2 * 1024 * 1024;

  public static FlinkArtifactSource createDefault(DistributedCache cache) {
    return new FlinkArtifactSource(cache, FlinkCachedArtifactPaths.createDefault());
  }

  public static FlinkArtifactSource forToken(DistributedCache cache, String artifactToken) {
    return new FlinkArtifactSource(cache, FlinkCachedArtifactPaths.forToken(artifactToken));
  }

  private final DistributedCache cache;
  private final FlinkCachedArtifactPaths paths;

  private FlinkArtifactSource(DistributedCache cache, FlinkCachedArtifactPaths paths) {
    this.cache = cache;
    this.paths = paths;
  }

  @Override
  public Manifest getManifest() throws IOException {
    String path = paths.getManifestPath();
    ByteString manifestBytes = ByteString.copyFrom(getFileBytes(path));
    return Manifest.parseFrom(manifestBytes);
  }

  @Override
  public void getArtifact(String name, StreamObserver<ArtifactChunk> responseObserver) {
    try {
      String path = paths.getArtifactPath(name);
      ByteBuffer artifact = getFileBytes(path);
      do {
        ByteString data =
            ByteString.copyFrom(artifact, Math.min(artifact.remaining(), DEFAULT_CHUNK_SIZE));
        responseObserver
            .onNext(
                ArtifactChunk
                    .newBuilder()
                    .setData(data)
                    .build());
      } while (artifact.hasRemaining());
      responseObserver.onCompleted();
    } catch (FileNotFoundException e) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(String.format("No such artifact %s", name))
              .withCause(e)
              .asException());
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  String.format("Could not retrieve artifact with name %s", name))
              .withCause(e)
              .asException());
    }
  }

  private ByteBuffer getFileBytes(String path) throws IOException {
    File artifact = cache.getFile(path);
    if (!artifact.exists()) {
      throw new FileNotFoundException(String.format("No such artifact %s", path));
    }
    FileChannel input = new FileInputStream(artifact).getChannel();
    return input.map(FileChannel.MapMode.READ_ONLY, 0L, input.size());
  }
}
