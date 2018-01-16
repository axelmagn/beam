package org.apache.beam.runners.flink.harness;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Manage long-lived Beam SDK harnesses on behalf of shorter lived flink operations.
 */
public class HarnessManager {
    private static final HarnessManager instance = new HarnessManager();

    private static final String POC_IMAGE = "wcserver";
    private static final int POC_IMAGE_PORT = 7000;

    public static HarnessManager getInstance() {
        return instance;
    }

    // TODO: remove me before pull request
    public static void main(String[] args) throws Exception {
        HarnessManager.getInstance().runProofOfConcept();
    }

    public void runProofOfConcept() throws Exception {
        DockerShellClient client = new DockerShellClient();

        // check if docker is available
        if (!client.isAvailable()) {
            throw new DockerException("Docker is not installed or available.");
        }

        // start harness
        String[] dockerArgs = {"--publish", "7000:7000"};
        String[] imageArgs = new String[0];
        Process harnessProc = client.run(POC_IMAGE, dockerArgs, imageArgs);
        Thread.sleep(3000);

        // TODO: use harness for data pipeline
        Socket wcSocket = new Socket("127.0.0.1", 7000);
        PrintWriter out = new PrintWriter(wcSocket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(wcSocket.getInputStream()));
        String msg = "hello world";
        out.println(msg);
        // String line = in.readLine();
        System.out.println(in.readLine());

        // TODO: stop harness
        harnessProc.destroy();

        // TODO: clean up image
    }

}
