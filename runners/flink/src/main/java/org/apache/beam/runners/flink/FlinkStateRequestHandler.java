package org.apache.beam.runners.flink;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;

import java.util.concurrent.CompletionStage;

public class FlinkStateRequestHandler implements StateRequestHandler {

  @Override
  public CompletionStage<BeamFnApi.StateResponse.Builder> handle(BeamFnApi.StateRequest request)
      throws Exception {
    switch (request.getStateKey().getTypeCase()) {
      case BAG_USER_STATE:
        return handleBagUserState(request);
      case MULTIMAP_SIDE_INPUT:
        return handleMultimapSideInput(request);
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Flink does not handle StateRequests of type %s",
                request.getStateKey().getTypeCase()));
    }
  }

  private CompletionStage<BeamFnApi.StateResponse.Builder> handleBagUserState(
      BeamFnApi.StateRequest request) {
    // TODO: handle bag user state
    throw new UnsupportedOperationException("Not yet implemented");
  }

  private CompletionStage<BeamFnApi.StateResponse.Builder> handleMultimapSideInput(
      BeamFnApi.StateRequest request) {
    // TODO: handle multimap side input
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
