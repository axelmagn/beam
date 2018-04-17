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

package org.apache.beam.runners.fnexecution.artifact;

import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.Manifest;

/**
 * Makes artifacts available to an ArtifactRetrievalService by
 * encapsulating runner-specific resources.
 *
 * An ArtifactSource may be accessed concurrently, and therefore <b>needs to be threadsafe</b>.
 */
public interface ArtifactSource {

  /**
   * Get the artifact manifest available from this source.
   */
  Manifest getManifest() throws IOException;

  /**
   * Get an artifact by its name.
   */
  void getArtifact(String name, StreamObserver<ArtifactChunk> responseObserver);
}
