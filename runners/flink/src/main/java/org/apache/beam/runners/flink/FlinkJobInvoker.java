package org.apache.beam.runners.flink;

import com.google.common.util.concurrent.ListeningExecutorService;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvocation;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvoker;
import org.apache.beam.runners.fnexecution.jobsubmission.JobPreparation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Job Invoker for the {@link FlinkRunner}.
 */
public class FlinkJobInvoker implements JobInvoker {
  public static FlinkJobInvoker create(ListeningExecutorService executorService) {
    return new FlinkJobInvoker(executorService);
  }

  private final ListeningExecutorService executorService;

  private FlinkJobInvoker(ListeningExecutorService executorService) {
    this.executorService = executorService;
  }

  @Override
  public JobInvocation invoke(JobPreparation preparation, @Nullable String artifactToken)
      throws IOException {
      String invocationId =
          String.format("%s_%d", preparation.id(), ThreadLocalRandom.current().nextInt());
      PipelineOptions options = PipelineOptionsTranslation.fromProto(preparation.options());
      Pipeline pipeline = PipelineTranslation.fromProto(preparation.pipeline());
      FlinkRunner runner = FlinkRunner.fromOptions(options);
      return FlinkJobInvocation.create(invocationId, executorService, runner, pipeline);
  }
}
