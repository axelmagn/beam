package org.apache.beam.runners.flink.execution;

/**
 * An exception caused by a problem in an EnvironmentSession
 */
public class EnvironmentSessionException extends Exception {

  public EnvironmentSessionException(String message) {
    super(message);
  }
}
