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

    // TODO: import

    // TODO: create

    // TODO: start

    // TODO: stop

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
