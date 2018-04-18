package org.apache.beam.runners.fnexecution.artifact;

import io.grpc.stub.StreamObserver;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

/**
 * A closeable ArtifactSource that:
 *
 * <li>
 *   <ul>tracks open connections</ul>
 *   <ul>does not allow new calls once close has been called</ul>
 *   <ul>blocks a call to close until all connections have completed</ul>
 * </li>
 *
 * Only one call to close per source is legal.  Subsequent calls to close when in a closing or
 * closed state are illegal.
 */
public class EphemeralArtifactSource implements ArtifactSource, AutoCloseable {
  // So the

  private enum State {
    OPEN,
    CLOSING,
    CLOSED,
  }

  // this number is arbitrarily high.  The important thing is for all connections to release
  // their permits before we deregister the artifact source.
  private static final int MAX_CONNECTIONS = Integer.MAX_VALUE;

  private final ArtifactSource artifactSource;
  // ALL METHODS ACCESSING STATE NEED TO BE SYNCHRONIZED
  private State state;
  private Semaphore connectionsLock;
  private CountDownLatch closeLatch;

  private EphemeralArtifactSource(ArtifactSource artifactSource) {
    this.artifactSource = artifactSource;
    this.connectionsLock = new Semaphore(MAX_CONNECTIONS, true);
    this.state = State.OPEN;
    this.closeLatch = new CountDownLatch(1);
  }

  @Override
  public ArtifactApi.Manifest getManifest() throws Exception {
    if(openConnection()) {
      ArtifactApi.Manifest manifest;
      try {
        manifest = artifactSource.getManifest();
      } finally {
        closeConnection();
      }
      return manifest;
    } else {
      throw new IllegalStateException(
          String.format(
              "%s is in state %s and cannot retrieve the manifest.",
              EphemeralArtifactSource.class.getName(),
              state.toString()));
    }
  }

  @Override
  public void getArtifact(String name, StreamObserver<ArtifactApi.ArtifactChunk> responseObserver) {
    // TODO(axelmagn):
    // openConnection();
    // add callback to closeConnection() when observer finishes
    // call getArtifact on artifactSource
  }

  @Override
  public void close() throws Exception {
    State prevState = startClose();
    switch (prevState) {
      case OPEN:
        finishExistingConnections();
        break;
      case CLOSING:
        awaitClosed();
        break;
      case CLOSED:
    }
  }

  /**
   * Open a connection, returning true if successful.
   * @return true if a connection could be opened.
   * @throws InterruptedException
   */
  private synchronized boolean openConnection() throws InterruptedException {
    if(state == State.OPEN) {
      connectionsLock.acquire();
      return true;
    }
    return false;
  }

  private void closeConnection() {
    connectionsLock.release();
  }

  /**
   * Start the closing operation.  After this is called, no more connections will be accepted.
   * @return previous state.
   */
  private synchronized State startClose() {
    State prevState = state;
    if(state == State.OPEN) {
      state = State.CLOSING;
    }
    return prevState;
  }

  private void finishExistingConnections() throws InterruptedException {
    // wait to acquire all semaphore permits
    connectionsLock.acquire(MAX_CONNECTIONS);
    // transition to closed state
    synchronized (this) {
      state = State.CLOSED;
    }
    closeLatch.countDown();
  }

  private void awaitClosed() throws InterruptedException {
    closeLatch.await();
  }
}
