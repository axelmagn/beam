package org.apache.beam.runners.flink;

import com.google.common.collect.Iterables;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.CoderTranslation;
import akka.dispatch.BatchingExecutor;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.TreeMap;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.ExecutableStageTranslation;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.flink.translation.functions.FlinkPartialReduceFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkReduceFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStagePruningFunction;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.types.KvKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.ImpulseInputFormat;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupCombineOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.Grouping;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
/**
 * Translate a bounded portable pipeline representation into a Flink pipeline representation.
 */
public class FlinkBatchPortablePipelineTranslator
    implements FlinkPortablePipelineTranslator<
    FlinkBatchPortablePipelineTranslator.BatchTranslationContext> {

public class FlinkBatchPortablePipelineTranslator implements FlinkPortablePipelineTranslator<FlinkBatchPortablePipelineTranslator.BatchTranslationContext> {

  // Shared between batch and streaming
  interface TranslationContext {
    PipelineOptions getPipelineOptions();
    <T> void addDataSet(String pCollectionId, DataSet<T> dataSet);
    <T> DataSet<T> getDataSetOrThrow(String pCollectionId);
  }

  // For batch only
  interface BatchTranslationContext extends TranslationContext {
    ExecutionEnvironment getExecutionEnvironment();
  }

  private abstract class TranslationContextImpl
      implements TranslationContext {
    private final Map<String, DataSet<?>> dataSets;
    private final PipelineOptions pipelineOptions;

    protected TranslationContextImpl(PipelineOptions pipelineOptions) {
      this.pipelineOptions = pipelineOptions;
      dataSets = new HashMap<>();
    }

    public <T> void addDataSet(String pCollectionId, DataSet<T> dataSet) {
      dataSets.put(pCollectionId, dataSet);
    }

    public <T> DataSet<T> getDataSetOrThrow(String pCollectionId) {
      DataSet<T> dataSet = (DataSet<T>) dataSets.get(pCollectionId);
      if (dataSet == null) {
        throw new IllegalArgumentException(
            String.format("Unknown dataset for id %s.", pCollectionId));
      }
      return dataSet;
    }

    public PipelineOptions getPipelineOptions() {
      return pipelineOptions;
    }
  }

  private class BatchTranslationContextImpl
      extends TranslationContextImpl
      implements BatchTranslationContext {
    private final ExecutionEnvironment executionEnvironment;
    private BatchTranslationContextImpl(
        PipelineOptions pipelineOptions,
        ExecutionEnvironment executionEnvironment) {
      super(pipelineOptions);
      this.executionEnvironment = executionEnvironment;
    }

    @Override
    public ExecutionEnvironment getExecutionEnvironment() {
      return executionEnvironment;
    }
  }

  interface PTransformTranslator<T> {
    void translate(String id, RunnerApi.Pipeline pipeline, T t);
  }

  private final Map<String, PTransformTranslator<BatchTranslationContext>>
      urnToTransformTranslator = new HashMap<>();

  FlinkBatchPortablePipelineTranslator() {
    urnToTransformTranslator.put(PTransformTranslation.FLATTEN_TRANSFORM_URN,
        this::translateFlatten);
    urnToTransformTranslator.put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN,
        this::translateGroupByKey);
    urnToTransformTranslator.put(PTransformTranslation.IMPULSE_TRANSFORM_URN,
        this::translateImpulse);
    urnToTransformTranslator.put(ExecutableStage.URN, this::translateExecutableStage);
  }

  @Override
  public void translate(
      BatchTranslationContext context, RunnerApi.Pipeline pipeline) {
    QueryablePipeline p = QueryablePipeline.forPrimitivesIn(pipeline.getComponents());
    for (PipelineNode.PTransformNode transform : p.getTopologicallyOrderedTransforms()) {
      urnToTransformTranslator.getOrDefault(
          transform.getTransform().getSpec().getUrn(), this::urnNotFound)
          .translate(transform.getId(), pipeline, context);
    }
  }

  private <InputT> void translateExecutableStage(
      String id,
      RunnerApi.Pipeline pipeline,
      BatchTranslationContext context) {
    // TODO: Fail on stateful DoFns for now.
    // TODO: Support stateful DoFns by inserting group-by-keys where necessary.
    // TODO: Fail on splittable DoFns.
    // TODO: Special-case single outputs to avoid multiplexing PCollections.

    RunnerApi.Components components = pipeline.getComponents();
    RunnerApi.PTransform transform = components.getTransformsOrThrow(id);
    Map<String, String> outputs = transform.getOutputsMap();
    // Mapping from local outputs to coder tag id.
    BiMap<String, Integer> outputMap = createOutputMap(outputs.keySet());
    // Collect all output Coders and create a UnionCoder for our tagged outputs.
    List<Coder<?>> unionCoders = Lists.newArrayList();
    // Enforce tuple tag sorting by union tag index.
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(components);
    Map<String, Coder<WindowedValue<?>>> outputCoders = Maps.newHashMap();
    for (String localOutputName : new TreeMap<>(outputMap.inverse()).values()) {
      String collectionId = outputs.get(localOutputName);
      RunnerApi.PCollection coll = components.getPcollectionsOrThrow(collectionId);
      RunnerApi.Coder coderProto = components.getCodersOrThrow(coll.getCoderId());
      Coder<WindowedValue<?>> windowCoder;
      try {
        Coder elementCoder = CoderTranslation.fromProto(coderProto, rehydratedComponents);
        RunnerApi.WindowingStrategy windowProto = components.getWindowingStrategiesOrThrow(
            coll.getWindowingStrategyId());
        WindowingStrategy<Object, BoundedWindow> windowingStrategy =
            (WindowingStrategy<Object, BoundedWindow>)
            WindowingStrategyTranslation.fromProto(windowProto, rehydratedComponents);
        windowCoder = WindowedValue.getFullCoder(elementCoder, windowingStrategy.getWindowFn().windowCoder());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      unionCoders.add(windowCoder);
    }
    UnionCoder unionCoder = UnionCoder.of(unionCoders);
    TypeInformation<RawUnionValue> typeInformation =
        new CoderTypeInformation<>(unionCoder);

    RunnerApi.ExecutableStagePayload stagePayload;
    try {
      stagePayload = RunnerApi.ExecutableStagePayload.parseFrom(transform.getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    FlinkExecutableStageFunction<InputT> function =
        new FlinkExecutableStageFunction<>(stagePayload, components,
            outputMap);
    Map<String, String> inputs = transform.getInputsMap();
    DataSet<WindowedValue<InputT>> inputDataSet =
        context.getDataSetOrThrow(Iterables.getOnlyElement(inputs.values()));
    DataSet<RawUnionValue> taggedDataset =
        new MapPartitionOperator<>(inputDataSet,
            typeInformation,
            function,
            transform.getUniqueName());
    for (Map.Entry<String, String> output : outputs.entrySet()) {
      pruneOutput(taggedDataset,
          context,
          outputMap.get(output.getKey()),
          null,
          transform.getUniqueName(),
          output.getValue());
    }
    if (outputs.isEmpty()) {
      // TODO: Is this still necessary?
      taggedDataset.output(new DiscardingOutputFormat());
    }
  }

  private <T> void translateFlatten(
      String id,
      RunnerApi.Pipeline pipeline,
      BatchTranslationContext context) {
    Map<String, String> allInputs =
        pipeline.getComponents().getTransformsOrThrow(id).getInputsMap();
    DataSet<WindowedValue<T>> result = null;

    if (allInputs.isEmpty()) {

      // create an empty dummy source to satisfy downstream operations
      // we cannot create an empty source in Flink, therefore we have to
      // add the flatMap that simply never forwards the single element
      DataSource<String> dummySource =
          context.getExecutionEnvironment().fromElements("dummy");
      result =
          dummySource
              .<WindowedValue<T>>flatMap(
                  (s, collector) -> {
                    // never return anything
                  })
              .returns(
                  new CoderTypeInformation<>(
                      WindowedValue.getFullCoder(
                          (Coder<T>) VoidCoder.of(), GlobalWindow.Coder.INSTANCE)));
    } else {
      for (String pCollectionId : allInputs.values()) {
        DataSet<WindowedValue<T>> current = context.getDataSetOrThrow(pCollectionId);
        if (result == null) {
          result = current;
        } else {
          result = result.union(current);
        }
      }
    }

    // insert a dummy filter, there seems to be a bug in Flink
    // that produces duplicate elements after the union in some cases
    // if we don't
    result = result.filter(tWindowedValue -> true).name("UnionFixFilter");
    context.addDataSet(Iterables.getOnlyElement(
        pipeline.getComponents().getTransformsOrThrow(id).getOutputsMap().values()),
        result);
  }

  private <K, V> void translateGroupByKey(
      String id, RunnerApi.Pipeline pipeline, BatchTranslationContext context) {
    RunnerApi.PTransform pTransform =
        pipeline.getComponents().getTransformsOrThrow(id);
    String inputPCollectionId =
        Iterables.getOnlyElement(pTransform.getInputsMap().values());
    DataSet<WindowedValue<KV<K, V>>> inputDataSet =
        context.getDataSetOrThrow(inputPCollectionId);
    RunnerApi.Coder inputCoderProto =
        pipeline.getComponents().getCodersOrThrow(
            pipeline.getComponents().getPcollectionsOrThrow(
                inputPCollectionId).getCoderId());
    RunnerApi.WindowingStrategy windowingStrategyProto =
        pipeline.getComponents().getWindowingStrategiesOrThrow(
            pipeline.getComponents().getPcollectionsOrThrow(
                inputPCollectionId).getWindowingStrategyId());

    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(pipeline.getComponents());
    KvCoder<K, V> inputCoder;
    try {
      inputCoder = (KvCoder<K, V>) CoderTranslation.fromProto(
          inputCoderProto,
          rehydratedComponents);
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format("Unable to hydrate GroupByKey input coder %s.",
              inputCoderProto),
          e);
    }

    WindowingStrategy<Object, BoundedWindow> windowingStrategy;
    try {
      windowingStrategy =
          (WindowingStrategy<Object, BoundedWindow>) WindowingStrategyTranslation.fromProto(
              windowingStrategyProto,
              rehydratedComponents);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(
          String.format("Unable to hydrate GroupByKey windowing strategy %s.",
              windowingStrategyProto),
          e);
    }

    Concatenate combineFn = new Concatenate<>();
    Coder<List<V>> accumulatorCoder = combineFn.getAccumulatorCoder(
          CoderRegistry.createDefault(),
          inputCoder.getValueCoder());

    TypeInformation<WindowedValue<KV<K, List<V>>>> partialReduceTypeInfo =
        new CoderTypeInformation<>(
            WindowedValue.getFullCoder(
                KvCoder.of(inputCoder.getKeyCoder(), accumulatorCoder),
                windowingStrategy.getWindowFn().windowCoder()));

    Grouping<WindowedValue<KV<K, V>>> inputGrouping =
        inputDataSet.groupBy(new KvKeySelector<>(inputCoder.getKeyCoder()));

    FlinkPartialReduceFunction<K, V, List<V>, ?> partialReduceFunction =
        new FlinkPartialReduceFunction<>(
            combineFn, windowingStrategy, Collections.emptyMap(), context.getPipelineOptions());

    FlinkReduceFunction<K, List<V>, List<V>, ?> reduceFunction =
        new FlinkReduceFunction<>(
            combineFn, windowingStrategy, Collections.emptyMap(), context.getPipelineOptions());

    // Partially GroupReduce the values into the intermediate format AccumT (combine)
    GroupCombineOperator<
        WindowedValue<KV<K, V>>,
        WindowedValue<KV<K, List<V>>>> groupCombine =
        new GroupCombineOperator<>(
            inputGrouping,
            partialReduceTypeInfo,
            partialReduceFunction,
            "GroupCombine: " + pTransform.getUniqueName());

    Grouping<WindowedValue<KV<K, List<V>>>> intermediateGrouping =
        groupCombine.groupBy(new KvKeySelector<>(inputCoder.getKeyCoder()));

    // Fully reduce the values and create output format VO
    GroupReduceOperator<
        WindowedValue<KV<K, List<V>>>, WindowedValue<KV<K, List<V>>>> outputDataSet =
        new GroupReduceOperator<>(
            intermediateGrouping,
            partialReduceTypeInfo,
            reduceFunction,
            pTransform.getUniqueName());

    context.addDataSet(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        outputDataSet);
  }

  private void translateImpulse(
      String id, RunnerApi.Pipeline pipeline, BatchTranslationContext context) {
    RunnerApi.PTransform pTransform =
        pipeline.getComponents().getTransformsOrThrow(id);

    TypeInformation<WindowedValue<byte[]>> typeInformation =
        new CoderTypeInformation<>(
            WindowedValue.getFullCoder(ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE));
    DataSource<WindowedValue<byte[]>> dataSource = new DataSource<>(
        context.getExecutionEnvironment(),
        new ImpulseInputFormat(),
        typeInformation,
        pTransform.getUniqueName());

    context.addDataSet(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        dataSource);
  }

  /**
   * Combiner that combines {@code T}s into a single {@code List<T>} containing all inputs.
   *
   * <p>For internal use to translate {@link GroupByKey}. For a large {@link PCollection} this
   * is expected to crash!
   *
   * <p>This is copied from the dataflow runner code.
   *
   * @param <T> the type of elements to concatenate.
   */
  private static class Concatenate<T> extends Combine.CombineFn<T, List<T>, List<T>> {
    @Override
    public List<T> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<T> addInput(List<T> accumulator, T input) {
      accumulator.add(input);
      return accumulator;
    }

    @Override
    public List<T> mergeAccumulators(Iterable<List<T>> accumulators) {
      List<T> result = createAccumulator();
      for (List<T> accumulator : accumulators) {
        result.addAll(accumulator);
      }
      return result;
    }

    @Override
    public List<T> extractOutput(List<T> accumulator) {
      return accumulator;
    }

    @Override
    public Coder<List<T>> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }

    @Override
    public Coder<List<T>> getDefaultOutputCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }
  }

  public void urnNotFound(
      String id, RunnerApi.Pipeline pipeline, BatchTranslationContext context) {
    throw new IllegalArgumentException(
        String.format("Unknown type of URN %s for PTrasnform with id %s.",
            pipeline.getComponents().getTransformsOrThrow(id).getSpec().getUrn(),
            id));
  }

  private static <T> void pruneOutput(
      DataSet<RawUnionValue> taggedDataset,
      BatchTranslationContext context,
      int unionTag,
      Coder<WindowedValue<T>> outputCoder,
      String transformName,
      String collectionId) {
    TypeInformation<WindowedValue<T>> outputType = new CoderTypeInformation<>(outputCoder);
    FlinkExecutableStagePruningFunction<T> pruningFunction =
        new FlinkExecutableStagePruningFunction<>(unionTag);
    FlatMapOperator<RawUnionValue, WindowedValue<T>> pruningOperator =
        new FlatMapOperator<>(taggedDataset, outputType, pruningFunction,
            String.format("%s/out.%d", transformName, unionTag));
    context.addDataSet(collectionId, pruningOperator);
  }

  // Creates a mapping from local names to output tag integers.
  private static BiMap<String, Integer> createOutputMap(Iterable<String> localOutputs) {
    ImmutableBiMap.Builder<String, Integer> builder = ImmutableBiMap.builder();
    int outputIndex = 0;
    for (String tag : localOutputs) {
      builder.put(tag, outputIndex);
      outputIndex++;
    }
    return builder.build();
  }

}
