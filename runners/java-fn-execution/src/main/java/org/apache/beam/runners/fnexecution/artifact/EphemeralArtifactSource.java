package org.apache.beam.runners.fnexecution.artifact;

import io.grpc.stub.StreamObserver;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private enum State {
    OPEN,
    CLOSING,
    CLOSED,
  }

  // this number is arbitrarily high.  The important thing is for all connections to release
  // their permits before we deregister the artifact source.
  private static final int MAX_CONNECTIONS = Integer.MAX_VALUE;
  private static final Logger log = LoggerFactory.getLogger(EphemeralArtifactSource.class);

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
    try {
      if (openConnection()) {
        DisconnectingStreamObserver<ArtifactApi.ArtifactChunk> disconnectingStreamObserver =
            new DisconnectingStreamObserver<>(responseObserver, this);
        artifactSource.getArtifact(name, disconnectingStreamObserver);
      } else {
        throw new IllegalStateException(
            String.format(
                "%s is in state %s and cannot retrieve the artifact.",
                EphemeralArtifactSource.class.getName(),
                state.toString()));
      }
    } catch (InterruptedException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void close() throws Exception {
    // enter closing state
    State prevState;
    synchronized (this) {
      prevState = state;
      if (state == State.OPEN) {
        state = State.CLOSING;
      }
    }
    switch (prevState) {
      case OPEN:
        // finish existing connections
        connectionsLock.acquire(MAX_CONNECTIONS);
        synchronized (this) {
          state = State.CLOSED;
        }
        closeLatch.countDown();
        break;
      case CLOSING:
        // wait to be closed.  another thread is already acquiring the lock.
        closeLatch.await();
        break;
      case CLOSED:
        // nothing to do
    }
  }

  /**
   * Open a connection, returning true if successful.
   * @return true if a connection could be opened.
   * @throws InterruptedException
   */
  private boolean openConnection() throws InterruptedException {
    synchronized (this) {
      if (state == State.OPEN) {
        connectionsLock.acquire();
        return true;
      }
      return false;
    }
  }

  private void closeConnection() {
    connectionsLock.release();
  }

  private static class DisconnectingStreamObserver<T> implements StreamObserver<T> {
    private final StreamObserver<T> underlying;
    private final EphemeralArtifactSource artifactSource;

    DisconnectingStreamObserver(
        StreamObserver<T> underlying, EphemeralArtifactSource artifactSource) {
      this.underlying = underlying;
      this.artifactSource = artifactSource;
    }

    @Override
    public void onNext(T t) {
      underlying.onNext(t);
    }

    @Override
    public void onError(Throwable throwable) {
      artifactSource.closeConnection();
      underlying.onError(throwable);
    }

    @Override
    public void onCompleted() {
      artifactSource.closeConnection();
      underlying.onCompleted();
    }
  }
}
