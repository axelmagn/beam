/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.flink;

import com.google.common.collect.BiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.ExecutableStageDoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItem;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItemCoder;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WindowDoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WorkItemKeySelector;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Translate an unbounded portable pipeline representation into a Flink pipeline representation.
 */
public class FlinkStreamingPortablePipelineTranslator implements FlinkPortablePipelineTranslator<
        FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext> {

  public static StreamingTranslationContext createStreamingContext(FlinkPipelineOptions options) {
    return new StreamingTranslationContextImpl(
            options,
            FlinkPipelineExecutionEnvironment.createStreamExecutionEnvironment(options));
  }

  interface StreamingTranslationContext extends FlinkPortablePipelineTranslator.TranslationContext {
    StreamExecutionEnvironment getExecutionEnvironment();
    <T> void addDataStream(String pCollectionId, DataStream<T> dataStream);
    <T> DataStream<T> getDataStreamOrThrow(String pCollectionId);
  }

  private static class StreamingTranslationContextImpl
          extends TranslationContextImpl
          implements StreamingTranslationContext {
    private final StreamExecutionEnvironment executionEnvironment;
    private final Map<String, DataStream<?>> dataStreams;
    private StreamingTranslationContextImpl(
            PipelineOptions pipelineOptions,
            StreamExecutionEnvironment executionEnvironment) {
      super(pipelineOptions);
      this.executionEnvironment = executionEnvironment;
      dataStreams = new HashMap<>();
    }

    @Override
    public StreamExecutionEnvironment getExecutionEnvironment() {
      return executionEnvironment;
    }

    @Override
    public <T> void addDataStream(String pCollectionId, DataStream<T> dataSet) {
      dataStreams.put(pCollectionId, dataSet);
    }

    @Override
    public <T> DataStream<T> getDataStreamOrThrow(String pCollectionId) {
      DataStream<T> dataSet = (DataStream<T>) dataStreams.get(pCollectionId);
      if (dataSet == null) {
        throw new IllegalArgumentException(
                String.format("Unknown datastream for id %s.", pCollectionId));
      }
      return dataSet;
    }

  }

  interface PTransformTranslator<T> {
    void translate(String id, RunnerApi.Pipeline pipeline, T t);
  }

  private final Map<String, PTransformTranslator<StreamingTranslationContext>>
          urnToTransformTranslator = new HashMap<>();

  FlinkStreamingPortablePipelineTranslator() {
    urnToTransformTranslator.put(PTransformTranslation.FLATTEN_TRANSFORM_URN,
            this::translateFlatten);
    urnToTransformTranslator.put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN,
            this::translateGroupByKey);
    urnToTransformTranslator.put(PTransformTranslation.IMPULSE_TRANSFORM_URN,
            this::translateImpulse);
    urnToTransformTranslator.put(ExecutableStage.URN,
        this::translateExecutableStage);
    urnToTransformTranslator.put(PTransformTranslation.RESHUFFLE_URN,
        this::translateReshuffle);
  }


  @Override
  public void translate(StreamingTranslationContext context, RunnerApi.Pipeline pipeline) {
    QueryablePipeline p = QueryablePipeline.forTransforms(
        pipeline.getRootTransformIdsList(), pipeline.getComponents());
    for (PipelineNode.PTransformNode transform : p.getTopologicallyOrderedTransforms()) {
      urnToTransformTranslator.getOrDefault(
              transform.getTransform().getSpec().getUrn(), this::urnNotFound)
              .translate(transform.getId(), pipeline, context);
    }

  }

  public void urnNotFound(
          String id, RunnerApi.Pipeline pipeline,
          FlinkBatchPortablePipelineTranslator.TranslationContext context) {
    throw new IllegalArgumentException(
            String.format("Unknown type of URN %s for PTrasnform with id %s.",
                    pipeline.getComponents().getTransformsOrThrow(id).getSpec().getUrn(),
                    id));
  }

  private <K, V> void translateReshuffle(
      String id,
      RunnerApi.Pipeline pipeline,
      StreamingTranslationContext context) {
    RunnerApi.PTransform transform = pipeline.getComponents().getTransformsOrThrow(id);
    DataStream<WindowedValue<KV<K, V>>> inputDataSet =
        context.getDataStreamOrThrow(
            Iterables.getOnlyElement(transform.getInputsMap().values()));
    context.addDataStream(Iterables.getOnlyElement(transform.getOutputsMap().values()),
        inputDataSet.rebalance());
  }

  public <T>  void translateFlatten(
          String id,
          RunnerApi.Pipeline pipeline,
          StreamingTranslationContext context) {
    Map<String, String> allInputs =
            pipeline.getComponents().getTransformsOrThrow(id).getInputsMap();

    if (allInputs.isEmpty()) {

      // create an empty dummy source to satisfy downstream operations
      // we cannot create an empty source in Flink, therefore we have to
      // add the flatMap that simply never forwards the single element
      DataStreamSource<String> dummySource =
              context.getExecutionEnvironment().fromElements("dummy");

      DataStream<WindowedValue<T>> result =
              dummySource
                      .<WindowedValue<T>>flatMap(
                              (s, collector) -> {
                                // never return anything
                              })
                      .returns(
                              new CoderTypeInformation<>(
                                      WindowedValue.getFullCoder(
                                              (Coder<T>) VoidCoder.of(),
                                              GlobalWindow.Coder.INSTANCE)));
      context.addDataStream(Iterables.getOnlyElement(
              pipeline.getComponents().getTransformsOrThrow(id).getOutputsMap().values()), result);
    } else {
      DataStream<T> result = null;

      // Determine DataStreams that we use as input several times. For those, we need to uniquify
      // input streams because Flink seems to swallow watermarks when we have a union of one and
      // the same stream.
      Map<DataStream<T>, Integer> duplicates = new HashMap<>();
      for (String input : allInputs.values()) {
        DataStream<T> current = context.getDataStreamOrThrow(input);
        Integer oldValue = duplicates.put(current, 1);
        if (oldValue != null) {
          duplicates.put(current, oldValue + 1);
        }
      }

      for (String input : allInputs.values()) {
        DataStream<T> current = context.getDataStreamOrThrow(input);

        final Integer timesRequired = duplicates.get(current);
        if (timesRequired > 1) {
          current = current.flatMap(new FlatMapFunction<T, T>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void flatMap(T t, Collector<T> collector) throws Exception {
              collector.collect(t);
            }
          });
        }
        result = (result == null) ? current : result.union(current);
      }

      context.addDataStream(Iterables.getOnlyElement(
              pipeline.getComponents().getTransformsOrThrow(id).getOutputsMap().values()), result);
    }
  }

  public <K, V> void translateGroupByKey(
          String id,
          RunnerApi.Pipeline pipeline,
          StreamingTranslationContext context) {

    RunnerApi.PTransform pTransform =
            pipeline.getComponents().getTransformsOrThrow(id);
    String inputPCollectionId =
            Iterables.getOnlyElement(pTransform.getInputsMap().values());
    String outputPCollectionId =
            Iterables.getOnlyElement(pTransform.getOutputsMap().values());

    RunnerApi.WindowingStrategy windowingStrategyProto =
            pipeline.getComponents().getWindowingStrategiesOrThrow(
                    pipeline.getComponents().getPcollectionsOrThrow(
                            inputPCollectionId).getWindowingStrategyId());

    RunnerApi.Coder inputCoderProto =
            pipeline.getComponents().getCodersOrThrow(
                    pipeline.getComponents().getPcollectionsOrThrow(
                            inputPCollectionId).getCoderId());

    DataStream<WindowedValue<KV<K, V>>> inputDataStream =
            context.getDataStreamOrThrow(inputPCollectionId);

    RunnerApi.Coder outputCoderProto =
            pipeline.getComponents().getCodersOrThrow(
                    pipeline.getComponents().getPcollectionsOrThrow(
                            outputPCollectionId).getCoderId());

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

    WindowingStrategy<?, ?> windowingStrategy;
    try {
      windowingStrategy = WindowingStrategyTranslation.fromProto(
                      windowingStrategyProto,
                      rehydratedComponents);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(
              String.format("Unable to hydrate GroupByKey windowing strategy %s.",
                      windowingStrategyProto),
              e);
    }

    SingletonKeyedWorkItemCoder<K, V> workItemCoder = SingletonKeyedWorkItemCoder.of(
            inputCoder.getKeyCoder(),
            inputCoder.getValueCoder(),
            windowingStrategy.getWindowFn().windowCoder());

    WindowedValue.
            FullWindowedValueCoder<SingletonKeyedWorkItem<K, V>> windowedWorkItemCoder =
            WindowedValue.getFullCoder(
                    workItemCoder,
                    windowingStrategy.getWindowFn().windowCoder());

    CoderTypeInformation<WindowedValue<SingletonKeyedWorkItem<K, V>>> workItemTypeInfo =
            new CoderTypeInformation<>(windowedWorkItemCoder);

    DataStream<WindowedValue<SingletonKeyedWorkItem<K, V>>> workItemStream =
            inputDataStream
                    .flatMap(new FlinkStreamingTransformTranslators.ToKeyedWorkItem<>())
                    .returns(workItemTypeInfo)
                    .name("ToKeyedWorkItem");

    KeyedStream<WindowedValue<SingletonKeyedWorkItem<K, V>>, ByteBuffer>
            keyedWorkItemStream =
            workItemStream.keyBy(new WorkItemKeySelector<>(inputCoder.getKeyCoder()));

    SystemReduceFn<K, V, Iterable<V>, Iterable<V>, BoundedWindow> reduceFn =
            SystemReduceFn.buffering(inputCoder.getValueCoder());

    Coder<KV<K, Iterable<V>>> outputCoder;
    try {
      outputCoder = (Coder<KV<K, Iterable<V>>>) CoderTranslation.fromProto(
              outputCoderProto,
              rehydratedComponents);
    } catch (IOException e) {
      throw new IllegalStateException(
              String.format("Unable to hydrate GroupByKey input coder %s.",
                      inputCoderProto),
              e);
    }

    //TypeInformation<WindowedValue<KV<K, Iterable<V>>>> outputTypeInfo =
    //        context.getTypeInfo(context.getOutput(transform));
    // TODO: use output windowing strategy?
    WindowedValue.FullWindowedValueCoder<?> windowedValueOutputCoder =
            WindowedValue.getFullCoder(
                    outputCoder,
                    windowingStrategy.getWindowFn().windowCoder());
    TypeInformation<WindowedValue<KV<K, Iterable<V>>>> outputTypeInfo =
            new CoderTypeInformation(windowedValueOutputCoder);

    TupleTag<KV<K, Iterable<V>>> mainTag = new TupleTag<>("main output");

    // TODO: remove non-portable operator re-use
    WindowDoFnOperator<K, V, Iterable<V>> doFnOperator =
            new WindowDoFnOperator<K, V, Iterable<V>>(
                    reduceFn,
                    pTransform.getUniqueName(),
                    (Coder) windowedWorkItemCoder,
                    mainTag,
                    Collections.emptyList(),
                    new DoFnOperator.MultiOutputOutputManagerFactory(mainTag, outputCoder),
                    windowingStrategy,
                    new HashMap<>(), /* side-input mapping */
                    Collections.emptyList(), /* side inputs */
                    context.getPipelineOptions(),
                    inputCoder.getKeyCoder());

    // our operator excepts WindowedValue<KeyedWorkItem> while our input stream
    // is WindowedValue<SingletonKeyedWorkItem>, which is fine but Java doesn't like it ...
    //@SuppressWarnings("unchecked")
    SingleOutputStreamOperator<WindowedValue<KV<K, Iterable<V>>>> outputDataStream =
            keyedWorkItemStream
                    .transform(
                            pTransform.getUniqueName(),
                            outputTypeInfo,
                            (OneInputStreamOperator) doFnOperator);

    context.addDataStream(
            Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
            outputDataStream);

  }

  public void translateImpulse(
          String id,
          RunnerApi.Pipeline pipeline,
          StreamingTranslationContext context) {
    RunnerApi.PTransform pTransform =
            pipeline.getComponents().getTransformsOrThrow(id);

    TypeInformation<WindowedValue<byte[]>> typeInfo =
            new CoderTypeInformation<>(
                    WindowedValue.getFullCoder(ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE));

    DataStreamSource<WindowedValue<byte[]>> source = context.getExecutionEnvironment()
            .fromCollection(Collections.singleton(WindowedValue.valueInGlobalWindow(
                    new byte[0])), typeInfo);

    context.addDataStream(
            Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
            source);
  }

  public <InputT, OutputT> void translateExecutableStage(
          String id,
          RunnerApi.Pipeline pipeline,
          StreamingTranslationContext context) {
    // TODO: Fail on stateful DoFns for now.
    // TODO: Support stateful DoFns by inserting group-by-keys where necessary.
    // TODO: Fail on splittable DoFns.
    // TODO: Special-case single outputs to avoid multiplexing PCollections.
    RunnerApi.Components components = pipeline.getComponents();
    RunnerApi.PTransform transform = components.getTransformsOrThrow(id);
    Map<String, String> outputs = transform.getOutputsMap();
    RehydratedComponents rehydratedComponents =
            RehydratedComponents.forComponents(components);

    // Mapping from local outputs to coder tag id.
    BiMap<String, Integer> outputMap =
            FlinkBatchPortablePipelineTranslator.createOutputMap(outputs.keySet());
    // Collect all output Coders and create a UnionCoder for our tagged outputs.
    List<Coder<?>> unionCoders = Lists.newArrayList();
    // Enforce tuple tag sorting by union tag index.
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
        WindowingStrategy<?, ?> windowingStrategy =
                WindowingStrategyTranslation.fromProto(windowProto, rehydratedComponents);
        windowCoder = WindowedValue.getFullCoder(elementCoder,
                windowingStrategy.getWindowFn().windowCoder());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      outputCoders.put(localOutputName, windowCoder);
      unionCoders.add(windowCoder);
    }
    //UnionCoder unionCoder = UnionCoder.of(unionCoders);
    //TypeInformation<RawUnionValue> typeInformation =
    //        new CoderTypeInformation<>(unionCoder);

    RunnerApi.ExecutableStagePayload stagePayload;
    try {
      stagePayload = RunnerApi.ExecutableStagePayload.parseFrom(transform.getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    String inputPCollectionId =
            Iterables.getOnlyElement(transform.getInputsMap().values());

    // TODO: is this still relevant?
    // we assume that the transformation does not change the windowing strategy.
    RunnerApi.WindowingStrategy windowingStrategyProto =
            pipeline.getComponents().getWindowingStrategiesOrThrow(
                    pipeline.getComponents().getPcollectionsOrThrow(
                            inputPCollectionId).getWindowingStrategyId());
    final WindowingStrategy<?, ?> windowingStrategy;
    try {
      windowingStrategy = WindowingStrategyTranslation.fromProto(
                      windowingStrategyProto,
                      rehydratedComponents);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(
              String.format("Unable to hydrate ExecutableStage windowing strategy %s.",
                      windowingStrategyProto),
              e);
    }

    Map<TupleTag<?>, OutputTag<WindowedValue<?>>> tagsToOutputTags = Maps.newHashMap();
    Map<TupleTag<?>, Coder<WindowedValue<?>>> tagsToCoders = Maps.newHashMap();
    // TODO: does it matter which output we designate as "main"
    final TupleTag<OutputT> mainOutputTag;
    if (!outputs.isEmpty()) {
      mainOutputTag = new TupleTag(outputs.keySet().iterator().next());
    } else {
      mainOutputTag = null;
    }

    // TODO: use the id mapping built above
    // We associate output tags with ids, the Integer is easier to serialize than TupleTag.
    // The return map of AppliedPTransform.getOutputs() is an ImmutableMap, its implementation is
    // RegularImmutableMap, its entrySet order is the same with the order of insertion.
    // So we can use the original AppliedPTransform.getOutputs() to produce deterministic ids.
    Map<TupleTag<?>, Integer> tagsToIds = Maps.newHashMap();
    int idCount = 0;
    for (Map.Entry<String, String> entry : outputs.entrySet()) {
      TupleTag<?> tupleTag = new TupleTag<>(entry.getKey());
      CoderTypeInformation<WindowedValue<?>> typeInformation =
              new CoderTypeInformation(
                      outputCoders.get(entry.getKey()));
      tagsToOutputTags.put(
              tupleTag,
              new OutputTag<>(entry.getKey(), typeInformation)
      );
      tagsToCoders.put(tupleTag, outputCoders.get(entry.getKey()));
      tagsToIds.put(tupleTag, idCount++);
    }

    SingleOutputStreamOperator<WindowedValue<OutputT>> outputStream;

    DataStream<WindowedValue<InputT>> inputDataStream = context.getDataStreamOrThrow(
            inputPCollectionId);

    RunnerApi.Coder inputCoderProto =
            pipeline.getComponents().getCodersOrThrow(
                    pipeline.getComponents().getPcollectionsOrThrow(
                            inputPCollectionId).getCoderId());
    final Coder<InputT> inputCoder;
    try {
      inputCoder = (Coder<InputT>) CoderTranslation.fromProto(
              inputCoderProto,
              rehydratedComponents);
    } catch (IOException e) {
      throw new IllegalStateException(
              String.format("Unable to hydrate ExecutableStage input coder %s.",
                      inputCoderProto),
              e);
    }

    Coder keyCoder = null;
    CoderTypeInformation<WindowedValue<OutputT>> outputTypeInformation = (!outputs.isEmpty())
            ? new CoderTypeInformation(outputCoders.get(mainOutputTag.getId())) : null;

    ArrayList<TupleTag<?>> additionalOutputTags = Lists.newArrayList();
    for (TupleTag<?> tupleTag : tagsToCoders.keySet()) {
      if (!mainOutputTag.getId().equals(tupleTag.getId())) {
        additionalOutputTags.add(tupleTag);
      }
    }

    DoFnOperator.MultiOutputOutputManagerFactory<OutputT> outputManagerFactory =
            new DoFnOperator.MultiOutputOutputManagerFactory<>(
            mainOutputTag, tagsToOutputTags, tagsToCoders, tagsToIds);

    // TODO: side inputs
    DoFnOperator<InputT, OutputT> doFnOperator =
              new ExecutableStageDoFnOperator(
                      transform.getUniqueName(),
                      inputCoder,
                      mainOutputTag,
                      additionalOutputTags,
                      outputManagerFactory,
                      windowingStrategy,
                      Collections.emptyMap() /* sideInputTagMapping */,
                      Collections.emptyList() /* sideInputs */,
                      context.getPipelineOptions(),
                      keyCoder,
                      stagePayload,
                      components,
                      stagePayload.getEnvironment()
              );

    outputStream = inputDataStream
              .transform(transform.getUniqueName(), outputTypeInformation, doFnOperator);

    if (!outputs.isEmpty()) {
      context.addDataStream(outputs.get(mainOutputTag.getId()), outputStream);
    }

    for (TupleTag<?> tupleTag : additionalOutputTags) {
        context.addDataStream(outputs.get(tupleTag.getId()),
                outputStream.getSideOutput(tagsToOutputTags.get(tupleTag)));
    }

  }

}