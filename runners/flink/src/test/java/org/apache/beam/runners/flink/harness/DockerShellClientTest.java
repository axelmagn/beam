package org.apache.beam.runners.flink.harness;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for the DockerShellClient.
 */
@RunWith(JUnit4.class)
public class DockerShellClientTest {

    @Mock
    private Runtime runtime;
    private DockerShellClient client;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        client = new DockerShellClient(runtime);
    }

    @Test
    public void testIsAvailableWhenAvailable() throws Exception {
        // TODO
    }

    @Test
    public void testIsAvailableWhenUnavailable() throws Exception {
        // TODO
    }
}
