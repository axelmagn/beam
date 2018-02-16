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

package org.apache.beam.runners.flink.execution;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClientControlService;
import org.apache.beam.runners.fnexecution.environment.ContainerManager;

import javax.annotation.Nullable;

/**
 * A long-lived class to manage SDK harness container instances on behalf of
 * shorter-lived Flink operators.  It is responsible for initializing and storing
 * job resources for retrieval.
 */
public class SingletonSdkHarnessManager implements  SdkHarnessManager {
  private static SingletonSdkHarnessManager ourInstance = new SingletonSdkHarnessManager();

  /** Get the singleton instance of the harness manager. */
  public static SingletonSdkHarnessManager getInstance() {
    return ourInstance;
  }

  private final ServerFactory serverFactory;
  private final ExecutorService executorService;
  private final JobResourceManagerFactory jobResourceManagerFactory;

  private SingletonSdkHarnessManager() {
    this(
        ServerFactory.createDefault(),
        Executors.newCachedThreadPool(),
        JobResourceManagerFactory.create());
  }

  @VisibleForTesting
  SingletonSdkHarnessManager(
      ServerFactory serverFactory,
      ExecutorService executorService,
      JobResourceManagerFactory jobResourceManagerFactory) {
    this.serverFactory = serverFactory;
    this.executorService = executorService;
    this.jobResourceManagerFactory = jobResourceManagerFactory;
  }

  @Nullable
  private JobResourceManager jobResourceManager = null;

  @Override
  public EnvironmentSession getSession(
      ProvisionApi.ProvisionInfo jobInfo,
      RunnerApi.Environment environment,
      ArtifactSource artifactSource) throws Exception {

    if (jobResourceManager == null) {
      jobResourceManager =
          jobResourceManagerFactory.create(
              jobInfo,
              environment,
              artifactSource,
              serverFactory,
              executorService);
      jobResourceManager.start();
    }

    return jobResourceManager.getSession();
  }
}
