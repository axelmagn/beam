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

package org.apache.beam.runners.fnexecution.environment;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;

/** A {@link RemoteEnvironment} running via a Docker Container. */
public class DockerContainerEnvironment implements RemoteEnvironment {
  /**
   * Create a new {@link DockerContainerEnvironment} for the provided {@link Environment}. The
   * {@link SdkHarnessClient} will connect to the provided {@link Process}.
   */
  public static DockerContainerEnvironment forProcessAndClient(
      RunnerApi.Environment environment, Process dockerProcess, SdkHarnessClient client) {
    return new DockerContainerEnvironment(environment, dockerProcess, client);
  }

  private final RunnerApi.Environment environment;
  private final Process dockerProcess;
  private final SdkHarnessClient client;

  private DockerContainerEnvironment(
      RunnerApi.Environment environment, Process dockerProcess, SdkHarnessClient client) {
    this.environment = environment;
    this.dockerProcess = dockerProcess;
    this.client = client;
  }

  @Override
  public RunnerApi.Environment getEnvironment() {
    return environment;
  }

  @Override
  public SdkHarnessClient getClient() {
    return client;
  }

  @Override
  public void close() throws Exception {
    getClient().close();
    // Closing the client should shut down the harness, and then we can destroy it!
    dockerProcess.destroy();
    dockerProcess.waitFor();
  }
}
