package org.apache.beam.runners.fnexecution.artifact;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class EphemeralArtifactSourceTest {
  private static final String EXAMPLE_ARTIFACT_NAME = "example_artifact_name";
  private static final ArtifactApi.ArtifactMetadata EXAMPLE_ARTIFACT_METADATA =
      ArtifactApi.ArtifactMetadata.newBuilder().setName(EXAMPLE_ARTIFACT_NAME).build();
  private static final ArtifactApi.Manifest EXAMPLE_MANIFEST =
      ArtifactApi.Manifest.newBuilder().addArtifact(EXAMPLE_ARTIFACT_METADATA).build();
  private static final byte[][] EXAMPLE_DATA = {{0,1,2}, {3,4,5}, {6,7,8}};
  private static final ArtifactApi.ArtifactChunk[] EXAMPLE_ARTIFACT_CHUNKS = {
      ArtifactApi.ArtifactChunk.newBuilder().setData(ByteString.copyFrom(EXAMPLE_DATA[0])).build(),
      ArtifactApi.ArtifactChunk.newBuilder().setData(ByteString.copyFrom(EXAMPLE_DATA[1])).build(),
      ArtifactApi.ArtifactChunk.newBuilder().setData(ByteString.copyFrom(EXAMPLE_DATA[2])).build()
  };
  private static final long TIMEOUT_MS = 5000;


  @Mock
  private ArtifactSource upstream;
  @Mock
  private StreamObserver<ArtifactApi.ArtifactChunk> outerObserver;

  private EphemeralArtifactSource artifactSource;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    artifactSource = EphemeralArtifactSource.fromArtifactSource(upstream);
  }

  @Test
  public void testGetsManifestFromUpstream() throws Exception {
    setUpNormalUpstreamBehavior();
    ArtifactApi.Manifest manifest = artifactSource.getManifest();
    verify(upstream, times(1)).getManifest();
  }

  @Test
  public void testGetsArtifactFromUpstream() throws Exception {
    setUpNormalUpstreamBehavior();
    artifactSource.getArtifact(EXAMPLE_ARTIFACT_NAME, outerObserver);
    verify(outerObserver, times(3)).onNext(any());
  }

  @Test
  public void testGetManifestBlocksCloseUntilAllConnectionsFinished() throws Exception {
    // submit some calls that will block
    ExecutorService executorService =
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).build());
    CountDownLatch manifestEntryLatch = new CountDownLatch(1);
    CountDownLatch manifestReplyLatch = new CountDownLatch(1);
    CountDownLatch closeCompleteLatch = new CountDownLatch(1);
    AtomicBoolean closeCompleteObserved = new AtomicBoolean(false);
    AtomicBoolean closeExceptionObserved = new AtomicBoolean(false);

    // set up latched response to query
    doAnswer(invocation -> {
      manifestEntryLatch.countDown();
      manifestReplyLatch.await();
      return EXAMPLE_MANIFEST;
    }).when(upstream).getManifest();

    // start a manifest request
    Future<ArtifactApi.Manifest> manifestFuture =
        executorService.submit(() -> artifactSource.getManifest());

    // wait for the reply to start
    manifestEntryLatch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    // call close on the artifactSource
    executorService.execute(() -> {
      try {
        artifactSource.close();
        closeCompleteObserved.set(true);
      } catch (Exception e) {
        closeExceptionObserved.set(true);
      } finally {
        closeCompleteLatch.countDown();
      }
    });

    // make sure that the response still isn't done
    assertThat(manifestFuture.isDone(), is(false));
    assertThat(closeCompleteObserved.get(), is(false));
    assertThat(closeExceptionObserved.get(), is(false));

    // allow the response to finish
    manifestReplyLatch.countDown();

    // wait for manifest reply to finish and close to complete
    ArtifactApi.Manifest manifest = manifestFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    closeCompleteLatch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertThat(closeCompleteObserved.get(), is(true));
    assertThat(closeExceptionObserved.get(), is(false));
    assertThat(manifest, equals(EXAMPLE_MANIFEST));
  }

  private void setUpNormalUpstreamBehavior() throws Exception {
    when(upstream.getManifest()).thenReturn(EXAMPLE_MANIFEST);
    doAnswer(invocation -> {
      StreamObserver<ArtifactApi.ArtifactChunk> innerObserver =
          (StreamObserver< ArtifactApi.ArtifactChunk>) invocation.getArguments()[1];
      for(ArtifactApi.ArtifactChunk artifactChunk : EXAMPLE_ARTIFACT_CHUNKS) {
        innerObserver.onNext(artifactChunk);
      }
      innerObserver.onCompleted();
      return null;
    }).when(upstream).getArtifact(eq(EXAMPLE_ARTIFACT_NAME), any());
  }
}
