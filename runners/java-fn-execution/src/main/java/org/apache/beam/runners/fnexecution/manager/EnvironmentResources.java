package org.apache.beam.runners.fnexecution.manager;

import com.google.auto.value.AutoValue;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;

/**
 * Resources bound to the lifetime of a container environment.
 * @TODO(axelmagn): identify environment resources
 */
@AutoValue
abstract class EnvironmentResources {
  static EnvironmentResources create(
      ControlClientPool controlClientPool,
      RemoteEnvironment remoteEnvironment,
      GrpcFnServer<FnApiControlClientPoolService> controlServer) {
    return new AutoValue_EnvironmentResources(controlClientPool, remoteEnvironment, controlServer);
  }

  abstract ControlClientPool controlClientPool();
  abstract RemoteEnvironment remoteEnvironment();
  abstract GrpcFnServer<FnApiControlClientPoolService> controlServer();
}

