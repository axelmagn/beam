package org.apache.beam.runners.fnexecution.artifact;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

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
    verify(upstream.getManifest(), times(1));
  }

  @Test
  public void testGetsArtifactFromUpstream() throws Exception {
    setUpNormalUpstreamBehavior();
    artifactSource.getArtifact(EXAMPLE_ARTIFACT_NAME, outerObserver);
    verify(outerObserver, times(3)).onNext(any());
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
