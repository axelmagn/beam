package org.apache.beam.runners.flink.harness;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A docker client that operates by executing shell commands to interact with docker.
 */
public class DockerShellClient {

    // set as a variable in case we need to refactor to /usr/bin/docker or $(which docker) later.
    private static final String DOCKER_CMD = "docker";
    private static final String WHICH_CMD = "which";
    private static final Long TIMEOUT_MS = 25L;
    private static final TimeUnit TIMEOUT_UNIT = TimeUnit.MILLISECONDS;

    private final Runtime runtime;

    public DockerShellClient() {
        this.runtime = Runtime.getRuntime();
    }

    public DockerShellClient(Runtime runtime) {
        this.runtime = runtime;
    }

    public boolean isAvailable() throws IOException, InterruptedException {
        // check that a `docker` binary exists
        Process proc = runCmd(WHICH_CMD, DOCKER_CMD);
        if (proc.waitFor() != 0) {
            return false;
        }
        // check that this binary can handle the `version` command
        proc = runCmd(DOCKER_CMD, "version");
        return proc.waitFor() == 0;
    }

    public Process run(String imageName, String[] dockerArgs, String[] imageArgs)
             throws IOException {
        int dockerOptsIdx = 2;
        int imageArgsIdx = dockerOptsIdx + dockerArgs.length + 1;
        String[] cmd = new String[imageArgsIdx + imageArgs.length];
        cmd[0] = DOCKER_CMD;
        cmd[1] = "run";
        cmd[imageArgsIdx - 1] = imageName;
        for (int i = 0; i < dockerArgs.length; i++) {
            cmd[i + dockerOptsIdx] = dockerArgs[i];
        }
        for (int i = 0; i < imageArgs.length; i++) {
            cmd[i + imageArgsIdx] = imageArgs[i];
        }
        return runCmd(cmd);
    }

    /**
     * Execute a command in bash.
     * @param command command to execute
     * @return the resulting process
     */
    private Process runCmd(String... command) throws IOException {
        // TODO: explicitly set envp and working dir from config?
        return runtime.exec(command);
    }
}
