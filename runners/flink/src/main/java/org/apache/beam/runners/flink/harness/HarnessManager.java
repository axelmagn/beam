package org.apache.beam.runners.flink.harness;

/**
 * Manage long-lived Beam SDK harnesses on behalf of shorter lived flink operations.
 */
public class HarnessManager {
    private static final HarnessManager instance = new HarnessManager();

    public static HarnessManager getInstance() {
        return instance;
    }

    // TODO: remove me before pull request
    public static void main(String[] args) throws Exception {
        DockerShellClient client = new DockerShellClient();
        boolean result = client.isAvailable();
        if (result) {
            System.out.println("Docker is available");
        } else {
            System.out.println("Docker is unavailable");
        }
    }

    public void runProofOfConcept() throws Exception {
        DockerShellClient client = new DockerShellClient();

        // TODO: check if docker is available
        if (!client.isAvailable()) {
            throw new DockerException("Docker is not installed or available.");
        }
        // TODO: import image from gcs


        // TODO: start harness

        // TODO: use harness for data pipeline

        // TODO: stop harness

        // TODO: clean up image
    }

}
