package org.apache.beam.runners.flink.execution;

/**
 * Thrown when an operator attempts to use a session that has been closed.
 */
public class EnvironmentSessionClosedException extends EnvironmentSessionException {
  public EnvironmentSessionClosedException() {
    super("The remote session was used after it was closed.");
  }
}
