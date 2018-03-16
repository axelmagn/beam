package org.apache.beam.runners.fnexecution.state;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/** Tests for {@link org.apache.beam.runners.fnexecution.state.GrpcStateService} */
@RunWith(JUnit4.class)
public class GrpcStateServiceTest {
  private static long TIMEOUT = 30 * 1000;

  private GrpcStateService stateService;

  @Mock
  private StreamObserver<BeamFnApi.StateResponse> responseObserver;

  @Mock
  private StateRequestHandler handler;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    stateService = new GrpcStateService();
  }

  /**
   * After a handler has been registered with
   * {@link GrpcStateService#registerForProcessBundleInstructionId(String, StateRequestHandler)},
   * the {@link GrpcStateService} should delegate requests through
   * {@link GrpcStateService#state(StreamObserver)} to the registered handler.
   */
  @Test
  public void testStateRequestsHandledByRegisteredHandlers() throws Exception {
    // register handler
    String bundleInstructionId = "bundle_instruction";
    stateService.registerForProcessBundleInstructionId(bundleInstructionId, handler);

    // open state stream
    StreamObserver requestObserver = stateService.state(responseObserver);

    // send state request
    BeamFnApi.StateRequest request =
        BeamFnApi.StateRequest.newBuilder().setInstructionReference(bundleInstructionId).build();
    requestObserver.onNext(request);

    // assert behavior
    verify(handler).accept(eq(request), any());
  }

  @Test
  public void testHandlerResponseSentToStateStream() throws Exception {
    // define handler behavior
    ByteString expectedResponseData =
        ByteString.copyFrom("EXPECTED_RESPONSE_DATA", StandardCharsets.UTF_8);
    String bundleInstructionId = "EXPECTED_BUNDLE_INSTRUCTION_ID";
    BeamFnApi.StateResponse.Builder expectedBuilder =
        BeamFnApi.StateResponse
            .newBuilder()
            .setGet(BeamFnApi.StateGetResponse.newBuilder().setData(expectedResponseData));
    StateRequestHandler dummyHandler =
        (request, result) -> result.toCompletableFuture().complete(expectedBuilder);

    // define observer behavior
    CompletableFuture<Void> onNextCalled = new CompletableFuture<>();
    StreamObserver<BeamFnApi.StateResponse> recordingResponseObserver =
        TestStreams
            .<BeamFnApi.StateResponse>withOnNext(next -> {
              synchronized (onNextCalled) {
                onNextCalled.complete(null);
              }
            })
            .build();
    recordingResponseObserver = Mockito.spy(recordingResponseObserver);

    // register handler
    stateService.registerForProcessBundleInstructionId(bundleInstructionId, dummyHandler);

    // open state stream
    StreamObserver<BeamFnApi.StateRequest> requestObserver =
        stateService.state(recordingResponseObserver);

    // send state request
    BeamFnApi.StateRequest request =
        BeamFnApi.StateRequest.newBuilder().setInstructionReference(bundleInstructionId).build();
    requestObserver.onNext(request);

    // wait for response
    synchronized (onNextCalled) {
      onNextCalled.wait(TIMEOUT);
    }

    // assert that responses contain a built state response
    ArgumentCaptor<BeamFnApi.StateResponse> response =
        ArgumentCaptor.forClass(BeamFnApi.StateResponse.class);
    verify(recordingResponseObserver, times(1)).onNext(response.capture());
    verify(recordingResponseObserver, never()).onCompleted();
    verify(recordingResponseObserver, never()).onError(any());
    assertThat(response.getValue().getGet().getData(), equalTo(expectedResponseData));
  }
}
