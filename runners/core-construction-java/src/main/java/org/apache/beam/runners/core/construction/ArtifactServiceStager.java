/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.runners.core.construction;

import com.google.auto.value.AutoValue;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactMetadata;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.Manifest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc.ArtifactStagingServiceBlockingStub;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc.ArtifactStagingServiceStub;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.sdk.util.ThrowingSupplier;

/** A client to stage files on an {@link ArtifactStagingServiceGrpc ArtifactService}. */
public class ArtifactServiceStager {
  // 2 MB per file-request
  private static final int DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024;

  public static ArtifactServiceStager overChannel(Channel channel) {
    return overChannel(channel, DEFAULT_BUFFER_SIZE);
  }

  /**
   * Create a new ArtifactServiceStager with the specified buffer size. Useful for testing
   * multi-part uploads.
   *
   * @param bufferSize the maximum size of the artifact chunk, in bytes.
   */
  static ArtifactServiceStager overChannel(Channel channel, int bufferSize) {
    return new ArtifactServiceStager(channel, bufferSize);
  }

  private final int bufferSize;
  private final ArtifactStagingServiceStub stub;
  private final ArtifactStagingServiceBlockingStub blockingStub;
  private final ListeningExecutorService executorService =
      MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

  private ArtifactServiceStager(Channel channel, int bufferSize) {
    this.stub = ArtifactStagingServiceGrpc.newStub(channel);
    this.blockingStub = ArtifactStagingServiceGrpc.newBlockingStub(channel);
    this.bufferSize = bufferSize;
  }

  public String stage(Iterable<File> files) throws IOException, InterruptedException {
    final Map<File, CompletionStage<ArtifactMetadata>> futures = new HashMap<>();
    for (File file : files) {
      futures.put(file, MoreFutures.supplyAsync(new StagingCallable(file), executorService));
    }
    CompletionStage<StagingResult> stagingResult =
        MoreFutures.allAsList(futures.values())
            .thenApply(ignored -> new ExtractStagingResultsCallable(futures).call());
    return stageManifest(stagingResult);
  }

  private String stageManifest(CompletionStage<StagingResult> stagingFuture)
      throws InterruptedException {
    try {
      StagingResult stagingResult = MoreFutures.get(stagingFuture);
      if (stagingResult.isSuccess()) {
        Manifest manifest =
            Manifest.newBuilder().addAllArtifact(stagingResult.getMetadata()).build();
        return blockingStub.commitManifest(
            CommitManifestRequest.newBuilder().setManifest(manifest).build()).getStagingToken();
      } else {
        RuntimeException failure =
            new RuntimeException(
                String.format(
                    "Failed to stage %s files: %s",
                    stagingResult.getFailures().size(), stagingResult.getFailures().keySet()));
        for (Throwable t : stagingResult.getFailures().values()) {
          failure.addSuppressed(t);
        }
        throw failure;
      }
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private class StagingCallable implements ThrowingSupplier<ArtifactMetadata> {
    private final File file;

    private StagingCallable(File file) {
      this.file = file;
    }

    @Override
    public ArtifactMetadata get() throws Exception {
      // TODO: Add Retries
      PutArtifactResponseObserver responseObserver = new PutArtifactResponseObserver();
      StreamObserver<PutArtifactRequest> requestObserver = stub.putArtifact(responseObserver);
      // HACK: Encode paths into a flat namespace. The SDK harness will attempt to load artifacts
      // into a flat directory by name.
      String artifactName = escapePath(file.getPath());
      ArtifactMetadata metadata = ArtifactMetadata.newBuilder().setName(artifactName).build();
      requestObserver.onNext(PutArtifactRequest.newBuilder().setMetadata(metadata).build());

      MessageDigest md5Digest = MessageDigest.getInstance("MD5");
      FileChannel channel = new FileInputStream(file).getChannel();
      ByteBuffer readBuffer = ByteBuffer.allocate(bufferSize);
      while (!responseObserver.isTerminal() && channel.position() < channel.size()) {
        readBuffer.clear();
        channel.read(readBuffer);
        readBuffer.flip();
        md5Digest.update(readBuffer);
        readBuffer.rewind();
        PutArtifactRequest request =
            PutArtifactRequest.newBuilder()
                .setData(
                    ArtifactChunk.newBuilder().setData(ByteString.copyFrom(readBuffer)).build())
                .build();
        requestObserver.onNext(request);
      }

      requestObserver.onCompleted();
      responseObserver.awaitTermination();
      if (responseObserver.err.get() != null) {
        throw new RuntimeException(responseObserver.err.get());
      }
      return metadata.toBuilder().setMd5(BaseEncoding.base64().encode(md5Digest.digest())).build();
    }

    private class PutArtifactResponseObserver implements StreamObserver<PutArtifactResponse> {
      private final CountDownLatch completed = new CountDownLatch(1);
      private final AtomicReference<Throwable> err = new AtomicReference<>(null);

      @Override
      public void onNext(PutArtifactResponse value) {}

      @Override
      public void onError(Throwable t) {
        err.set(t);
        completed.countDown();
        throw new RuntimeException(t);
      }

      @Override
      public void onCompleted() {
        completed.countDown();
      }

      public boolean isTerminal() {
        return completed.getCount() == 0;
      }

      public void awaitTermination() throws InterruptedException {
        completed.await();
      }
    }
  }

  private static class ExtractStagingResultsCallable implements Callable<StagingResult> {
    private final Map<File, CompletionStage<ArtifactMetadata>> futures;

    private ExtractStagingResultsCallable(
        Map<File, CompletionStage<ArtifactMetadata>> futures) {
      this.futures = futures;
    }

    @Override
    public StagingResult call() {
      Set<ArtifactMetadata> metadata = new HashSet<>();
      Map<File, Throwable> failures = new HashMap<>();
      for (Entry<File, CompletionStage<ArtifactMetadata>> stagedFileResult : futures.entrySet()) {
        try {
          metadata.add(MoreFutures.get(stagedFileResult.getValue()));
        } catch (ExecutionException ee) {
          failures.put(stagedFileResult.getKey(), ee.getCause());
        } catch (InterruptedException ie) {
          throw new AssertionError(
              "This should never happen. " + "All of the futures are complete by construction", ie);
        }
      }
      if (failures.isEmpty()) {
        return StagingResult.success(metadata);
      } else {
        return StagingResult.failure(failures);
      }
    }
  }

  @AutoValue
  abstract static class StagingResult {
    static StagingResult success(Set<ArtifactMetadata> metadata) {
      return new AutoValue_ArtifactServiceStager_StagingResult(metadata, Collections.emptyMap());
    }

    static StagingResult failure(Map<File, Throwable> failures) {
      return new AutoValue_ArtifactServiceStager_StagingResult(
          null, failures);
    }

    boolean isSuccess() {
      return getMetadata() != null;
    }

    @Nullable
    abstract Set<ArtifactMetadata> getMetadata();

    abstract Map<File, Throwable> getFailures();
  }

  private static String escapePath(String path) {
    StringBuilder result = new StringBuilder(path.length() * 2);
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      switch (c) {
        case '_':
          result.append("__");
          break;
        case '\\':
          result.append("_.");
          break;
        case '/':
          result.append("._");
          break;
        case '.':
          result.append("..");
          break;
        default:
          result.append(c);
      }
    }
    return result.toString();
  }
}
