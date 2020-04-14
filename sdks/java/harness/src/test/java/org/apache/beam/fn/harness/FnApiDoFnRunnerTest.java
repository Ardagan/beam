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
package org.apache.beam.fn.harness;

import static org.apache.beam.sdk.util.WindowedValue.timestampedValueInGlobalWindow;
import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import org.apache.beam.fn.harness.control.BundleSplitListener;
import org.apache.beam.fn.harness.data.FakeBeamFnTimerClient;
import org.apache.beam.fn.harness.data.PCollectionConsumerRegistry;
import org.apache.beam.fn.harness.data.PTransformFunctionRegistry;
import org.apache.beam.fn.harness.state.FakeBeamFnStateClient;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.graph.ProtoOverrides;
import org.apache.beam.runners.core.construction.graph.SplittableParDoExpander;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.ResetDateTimeProvider;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.hamcrest.collection.IsMapContaining;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link FnApiDoFnRunner}. */
@RunWith(JUnit4.class)
public class FnApiDoFnRunnerTest implements Serializable {

  @Rule public transient ResetDateTimeProvider dateTimeProvider = new ResetDateTimeProvider();

  private static final Logger LOG = LoggerFactory.getLogger(FnApiDoFnRunnerTest.class);

  public static final String TEST_TRANSFORM_ID = "pTransformId";

  private static class ConcatCombineFn extends CombineFn<String, String, String> {
    @Override
    public String createAccumulator() {
      return "";
    }

    @Override
    public String addInput(String accumulator, String input) {
      return accumulator.concat(input);
    }

    @Override
    public String mergeAccumulators(Iterable<String> accumulators) {
      StringBuilder builder = new StringBuilder();
      for (String value : accumulators) {
        builder.append(value);
      }
      return builder.toString();
    }

    @Override
    public String extractOutput(String accumulator) {
      return accumulator;
    }
  }

  private static class TestStatefulDoFn extends DoFn<KV<String, String>, String> {
    private static final TupleTag<String> mainOutput = new TupleTag<>("mainOutput");
    private static final TupleTag<String> additionalOutput = new TupleTag<>("output");

    @StateId("value")
    private final StateSpec<ValueState<String>> valueStateSpec =
        StateSpecs.value(StringUtf8Coder.of());

    @StateId("bag")
    private final StateSpec<BagState<String>> bagStateSpec = StateSpecs.bag(StringUtf8Coder.of());

    @StateId("combine")
    private final StateSpec<CombiningState<String, String, String>> combiningStateSpec =
        StateSpecs.combining(StringUtf8Coder.of(), new ConcatCombineFn());

    @ProcessElement
    public void processElement(
        ProcessContext context,
        @StateId("value") ValueState<String> valueState,
        @StateId("bag") BagState<String> bagState,
        @StateId("combine") CombiningState<String, String, String> combiningState) {
      context.output("value:" + valueState.read());
      valueState.write(context.element().getValue());

      context.output("bag:" + Iterables.toString(bagState.read()));
      bagState.add(context.element().getValue());

      context.output("combine:" + combiningState.read());
      combiningState.add(context.element().getValue());
    }
  }

  @Test
  public void testUsingUserState() throws Exception {
    Pipeline p = Pipeline.create();
    PCollection<KV<String, String>> valuePCollection =
        p.apply(Create.of(KV.of("unused", "unused")));
    PCollection<String> outputPCollection =
        valuePCollection.apply(TEST_TRANSFORM_ID, ParDo.of(new TestStatefulDoFn()));

    SdkComponents sdkComponents = SdkComponents.create(p.getOptions());
    RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents);
    String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
    String outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);
    RunnerApi.PTransform pTransform =
        pProto
            .getComponents()
            .getTransformsOrThrow(
                pProto.getComponents().getTransformsOrThrow(TEST_TRANSFORM_ID).getSubtransforms(0));

    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                bagUserStateKey("value", "X"), encode("X0"),
                bagUserStateKey("bag", "X"), encode("X0"),
                bagUserStateKey("combine", "X"), encode("X0")));

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        outputPCollectionId,
        TEST_TRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* splitListener */,
            null /* bundleFinalizer */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    // Ensure that bag user state that is initially empty or populated works.
    // Ensure that the key order does not matter when we traverse over KV pairs.
    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    mainInput.accept(valueInGlobalWindow(KV.of("X", "X1")));
    mainInput.accept(valueInGlobalWindow(KV.of("Y", "Y1")));
    mainInput.accept(valueInGlobalWindow(KV.of("X", "X2")));
    mainInput.accept(valueInGlobalWindow(KV.of("Y", "Y2")));
    assertThat(
        mainOutputValues,
        contains(
            valueInGlobalWindow("value:X0"),
            valueInGlobalWindow("bag:[X0]"),
            valueInGlobalWindow("combine:X0"),
            valueInGlobalWindow("value:null"),
            valueInGlobalWindow("bag:[]"),
            valueInGlobalWindow("combine:"),
            valueInGlobalWindow("value:X1"),
            valueInGlobalWindow("bag:[X0, X1]"),
            valueInGlobalWindow("combine:X0X1"),
            valueInGlobalWindow("value:Y1"),
            valueInGlobalWindow("bag:[Y1]"),
            valueInGlobalWindow("combine:Y1")));
    mainOutputValues.clear();

    Iterables.getOnlyElement(finishFunctionRegistry.getFunctions()).run();
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());

    assertEquals(
        ImmutableMap.<StateKey, ByteString>builder()
            .put(bagUserStateKey("value", "X"), encode("X2"))
            .put(bagUserStateKey("bag", "X"), encode("X0", "X1", "X2"))
            .put(bagUserStateKey("combine", "X"), encode("X0X1X2"))
            .put(bagUserStateKey("value", "Y"), encode("Y2"))
            .put(bagUserStateKey("bag", "Y"), encode("Y1", "Y2"))
            .put(bagUserStateKey("combine", "Y"), encode("Y1Y2"))
            .build(),
        fakeClient.getData());
  }

  /** Produces a bag user {@link StateKey} for the test PTransform id in the global window. */
  private StateKey bagUserStateKey(String userStateId, String key) throws IOException {
    return StateKey.newBuilder()
        .setBagUserState(
            StateKey.BagUserState.newBuilder()
                .setTransformId(TEST_TRANSFORM_ID)
                .setUserStateId(userStateId)
                .setKey(encode(key))
                .setWindow(
                    ByteString.copyFrom(
                        CoderUtils.encodeToByteArray(
                            GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE))))
        .build();
  }

  private static class TestSideInputDoFn extends DoFn<String, String> {
    private final PCollectionView<String> defaultSingletonSideInput;
    private final PCollectionView<String> singletonSideInput;
    private final PCollectionView<Iterable<String>> iterableSideInput;
    private final TupleTag<String> additionalOutput;

    private TestSideInputDoFn(
        PCollectionView<String> defaultSingletonSideInput,
        PCollectionView<String> singletonSideInput,
        PCollectionView<Iterable<String>> iterableSideInput,
        TupleTag<String> additionalOutput) {
      this.defaultSingletonSideInput = defaultSingletonSideInput;
      this.singletonSideInput = singletonSideInput;
      this.iterableSideInput = iterableSideInput;
      this.additionalOutput = additionalOutput;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(context.element() + ":" + context.sideInput(defaultSingletonSideInput));
      context.output(context.element() + ":" + context.sideInput(singletonSideInput));
      for (String sideInputValue : context.sideInput(iterableSideInput)) {
        context.output(context.element() + ":" + sideInputValue);
      }
      context.output(additionalOutput, context.element() + ":additional");
    }
  }

  @Test
  public void testBasicWithSideInputsAndOutputs() throws Exception {
    Pipeline p = Pipeline.create();
    PCollection<String> valuePCollection = p.apply(Create.of("unused"));
    PCollectionView<String> defaultSingletonSideInputView =
        valuePCollection.apply(
            View.<String>asSingleton().withDefaultValue("defaultSingletonValue"));
    PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
    PCollectionView<Iterable<String>> iterableSideInputView =
        valuePCollection.apply(View.asIterable());
    TupleTag<String> mainOutput = new TupleTag<String>("main") {};
    TupleTag<String> additionalOutput = new TupleTag<String>("additional") {};
    PCollectionTuple outputPCollection =
        valuePCollection.apply(
            TEST_TRANSFORM_ID,
            ParDo.of(
                    new TestSideInputDoFn(
                        defaultSingletonSideInputView,
                        singletonSideInputView,
                        iterableSideInputView,
                        additionalOutput))
                .withSideInputs(
                    defaultSingletonSideInputView, singletonSideInputView, iterableSideInputView)
                .withOutputTags(mainOutput, TupleTagList.of(additionalOutput)));

    SdkComponents sdkComponents = SdkComponents.create(p.getOptions());
    RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents, true);
    String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
    String outputPCollectionId =
        sdkComponents.registerPCollection(outputPCollection.get(mainOutput));
    String additionalPCollectionId =
        sdkComponents.registerPCollection(outputPCollection.get(additionalOutput));

    RunnerApi.PTransform pTransform =
        pProto.getComponents().getTransformsOrThrow(TEST_TRANSFORM_ID);

    ImmutableMap<StateKey, ByteString> stateData =
        ImmutableMap.of(
            multimapSideInputKey(singletonSideInputView.getTagInternal().getId(), ByteString.EMPTY),
            encode("singletonValue"),
            multimapSideInputKey(iterableSideInputView.getTagInternal().getId(), ByteString.EMPTY),
            encode("iterableValue1", "iterableValue2", "iterableValue3"));

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(stateData);

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    List<WindowedValue<String>> additionalOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        outputPCollectionId,
        TEST_TRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);
    consumers.register(
        additionalPCollectionId,
        TEST_TRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) additionalOutputValues::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* splitListener */,
            null /* bundleFinalizer */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(
        consumers.keySet(),
        containsInAnyOrder(inputPCollectionId, outputPCollectionId, additionalPCollectionId));

    // Ensure that bag user state that is initially empty or populated works.
    // Ensure that the bagUserStateKey order does not matter when we traverse over KV pairs.
    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    mainInput.accept(valueInGlobalWindow("X"));
    mainInput.accept(valueInGlobalWindow("Y"));
    assertThat(
        mainOutputValues,
        contains(
            valueInGlobalWindow("X:defaultSingletonValue"),
            valueInGlobalWindow("X:singletonValue"),
            valueInGlobalWindow("X:iterableValue1"),
            valueInGlobalWindow("X:iterableValue2"),
            valueInGlobalWindow("X:iterableValue3"),
            valueInGlobalWindow("Y:defaultSingletonValue"),
            valueInGlobalWindow("Y:singletonValue"),
            valueInGlobalWindow("Y:iterableValue1"),
            valueInGlobalWindow("Y:iterableValue2"),
            valueInGlobalWindow("Y:iterableValue3")));
    assertThat(
        additionalOutputValues,
        contains(valueInGlobalWindow("X:additional"), valueInGlobalWindow("Y:additional")));
    mainOutputValues.clear();

    Iterables.getOnlyElement(finishFunctionRegistry.getFunctions()).run();
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());

    // Assert that state data did not change
    assertEquals(stateData, fakeClient.getData());
  }

  private static class TestSideInputIsAccessibleForDownstreamCallersDoFn
      extends DoFn<String, Iterable<String>> {
    public static final String USER_COUNTER_NAME = "userCountedElems";
    private final Counter countedElements =
        Metrics.counter(TestSideInputIsAccessibleForDownstreamCallersDoFn.class, USER_COUNTER_NAME);

    private final PCollectionView<Iterable<String>> iterableSideInput;

    private TestSideInputIsAccessibleForDownstreamCallersDoFn(
        PCollectionView<Iterable<String>> iterableSideInput) {
      this.iterableSideInput = iterableSideInput;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      countedElements.inc();
      context.output(context.sideInput(iterableSideInput));
    }
  }

  @Test
  public void testSideInputIsAccessibleForDownstreamCallers() throws Exception {
    FixedWindows windowFn = FixedWindows.of(Duration.millis(1L));
    IntervalWindow windowA = windowFn.assignWindow(new Instant(1L));
    IntervalWindow windowB = windowFn.assignWindow(new Instant(2L));
    ByteString encodedWindowA =
        ByteString.copyFrom(CoderUtils.encodeToByteArray(windowFn.windowCoder(), windowA));
    ByteString encodedWindowB =
        ByteString.copyFrom(CoderUtils.encodeToByteArray(windowFn.windowCoder(), windowB));

    Pipeline p = Pipeline.create();
    PCollection<String> valuePCollection =
        p.apply(Create.of("unused")).apply(Window.into(windowFn));
    PCollectionView<Iterable<String>> iterableSideInputView =
        valuePCollection.apply(View.asIterable());
    PCollection<Iterable<String>> outputPCollection =
        valuePCollection.apply(
            TEST_TRANSFORM_ID,
            ParDo.of(new TestSideInputIsAccessibleForDownstreamCallersDoFn(iterableSideInputView))
                .withSideInputs(iterableSideInputView));

    SdkComponents sdkComponents = SdkComponents.create(p.getOptions());
    RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents, true);
    String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
    String outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);

    RunnerApi.PTransform pTransform =
        pProto
            .getComponents()
            .getTransformsOrThrow(
                pProto.getComponents().getTransformsOrThrow(TEST_TRANSFORM_ID).getSubtransforms(0));

    ImmutableMap<StateKey, ByteString> stateData =
        ImmutableMap.of(
            multimapSideInputKey(
                iterableSideInputView.getTagInternal().getId(), ByteString.EMPTY, encodedWindowA),
            encode("iterableValue1A", "iterableValue2A", "iterableValue3A"),
            multimapSideInputKey(
                iterableSideInputView.getTagInternal().getId(), ByteString.EMPTY, encodedWindowB),
            encode("iterableValue1B", "iterableValue2B", "iterableValue3B"));

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(stateData);

    List<WindowedValue<Iterable<String>>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        TEST_TRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<Iterable<String>>>) mainOutputValues::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* splitListener */,
            null /* bundleFinalizer */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    // Ensure that bag user state that is initially empty or populated works.
    // Ensure that the bagUserStateKey order does not matter when we traverse over KV pairs.
    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    mainInput.accept(valueInWindow("X", windowA));
    mainInput.accept(valueInWindow("Y", windowB));
    assertThat(mainOutputValues, hasSize(2));
    assertThat(
        mainOutputValues.get(0).getValue(),
        contains("iterableValue1A", "iterableValue2A", "iterableValue3A"));
    assertThat(
        mainOutputValues.get(1).getValue(),
        contains("iterableValue1B", "iterableValue2B", "iterableValue3B"));
    mainOutputValues.clear();

    Iterables.getOnlyElement(finishFunctionRegistry.getFunctions()).run();
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());

    // Assert that state data did not change
    assertEquals(stateData, fakeClient.getData());
  }

  /** @return a test MetricUpdate for expected metrics to compare against */
  public MetricUpdate create(String stepName, MetricName name, long value) {
    return MetricUpdate.create(MetricKey.create(stepName, name), value);
  }

  @Test
  public void testUsingMetrics() throws Exception {
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    MetricsContainerImpl metricsContainer = metricsContainerRegistry.getUnboundContainer();
    Closeable closeable = MetricsEnvironment.scopedMetricsContainer(metricsContainer);
    FixedWindows windowFn = FixedWindows.of(Duration.millis(1L));
    IntervalWindow windowA = windowFn.assignWindow(new Instant(1L));
    IntervalWindow windowB = windowFn.assignWindow(new Instant(2L));
    ByteString encodedWindowA =
        ByteString.copyFrom(CoderUtils.encodeToByteArray(windowFn.windowCoder(), windowA));
    ByteString encodedWindowB =
        ByteString.copyFrom(CoderUtils.encodeToByteArray(windowFn.windowCoder(), windowB));

    Pipeline p = Pipeline.create();
    PCollection<String> valuePCollection =
        p.apply(Create.of("unused")).apply(Window.into(windowFn));
    PCollectionView<Iterable<String>> iterableSideInputView =
        valuePCollection.apply(View.asIterable());
    PCollection<Iterable<String>> outputPCollection =
        valuePCollection.apply(
            TEST_TRANSFORM_ID,
            ParDo.of(new TestSideInputIsAccessibleForDownstreamCallersDoFn(iterableSideInputView))
                .withSideInputs(iterableSideInputView));

    SdkComponents sdkComponents = SdkComponents.create(p.getOptions());
    RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents, true);
    String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
    String outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);

    RunnerApi.PTransform pTransform =
        pProto
            .getComponents()
            .getTransformsOrThrow(
                pProto.getComponents().getTransformsOrThrow(TEST_TRANSFORM_ID).getSubtransforms(0));

    ImmutableMap<StateKey, ByteString> stateData =
        ImmutableMap.of(
            multimapSideInputKey(
                iterableSideInputView.getTagInternal().getId(), ByteString.EMPTY, encodedWindowA),
            encode("iterableValue1A", "iterableValue2A", "iterableValue3A"),
            multimapSideInputKey(
                iterableSideInputView.getTagInternal().getId(), ByteString.EMPTY, encodedWindowB),
            encode("iterableValue1B", "iterableValue2B", "iterableValue3B"));

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(stateData);

    List<WindowedValue<Iterable<String>>> mainOutputValues = new ArrayList<>();

    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        TEST_TRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<Iterable<String>>>) mainOutputValues::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* splitListener */,
            null /* bundleFinalizer */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    // Ensure that bag user state that is initially empty or populated works.
    // Ensure that the bagUserStateKey order does not matter when we traverse over KV pairs.
    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    mainInput.accept(valueInWindow("X", windowA));
    mainInput.accept(valueInWindow("Y", windowB));
    mainOutputValues.clear();

    Iterables.getOnlyElement(finishFunctionRegistry.getFunctions()).run();
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());

    MetricsContainer mc = MetricsEnvironment.getCurrentContainer();

    List<MonitoringInfo> expected = new ArrayList<MonitoringInfo>();
    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
    builder.setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, "Window.Into()/Window.Assign.out");
    builder.setInt64SumValue(2);
    expected.add(builder.build());

    builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
    builder.setLabel(
        MonitoringInfoConstants.Labels.PCOLLECTION,
        "pTransformId/ParMultiDo(TestSideInputIsAccessibleForDownstreamCallers).output");
    builder.setInt64SumValue(2);
    expected.add(builder.build());

    builder = new SimpleMonitoringInfoBuilder();
    builder
        .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
        .setLabel(
            MonitoringInfoConstants.Labels.NAMESPACE,
            TestSideInputIsAccessibleForDownstreamCallersDoFn.class.getName())
        .setLabel(
            MonitoringInfoConstants.Labels.NAME,
            TestSideInputIsAccessibleForDownstreamCallersDoFn.USER_COUNTER_NAME);
    builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, TEST_TRANSFORM_ID);
    builder.setInt64SumValue(2);
    expected.add(builder.build());

    closeable.close();
    List<MonitoringInfo> result = new ArrayList<MonitoringInfo>();
    for (MonitoringInfo mi : metricsContainerRegistry.getMonitoringInfos()) {
      result.add(mi);
    }
    assertThat(result, containsInAnyOrder(expected.toArray()));
  }

  private static class TestTimerfulDoFn extends DoFn<KV<String, String>, String> {
    @StateId("bag")
    private final StateSpec<BagState<String>> bagStateSpec = StateSpecs.bag(StringUtf8Coder.of());

    @TimerId("event")
    private final TimerSpec eventTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @TimerId("processing")
    private final TimerSpec processingTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void processElement(
        ProcessContext context,
        @StateId("bag") BagState<String> bagState,
        @TimerId("event") Timer eventTimeTimer,
        @TimerId("processing") Timer processingTimeTimer) {
      context.output("main" + context.element().getKey() + Iterables.toString(bagState.read()));
      bagState.add(context.element().getValue());
      eventTimeTimer.withOutputTimestamp(context.timestamp()).set(context.timestamp().plus(1L));
      processingTimeTimer.offset(Duration.millis(2L));
      processingTimeTimer.setRelative();
    }

    @OnTimer("event")
    public void eventTimer(
        OnTimerContext context,
        @StateId("bag") BagState<String> bagState,
        @TimerId("event") Timer eventTimeTimer,
        @TimerId("processing") Timer processingTimeTimer) {
      context.output("event" + Iterables.toString(bagState.read()));
      bagState.add("event");
      eventTimeTimer
          .withOutputTimestamp(context.timestamp())
          .set(context.fireTimestamp().plus(11L));
      processingTimeTimer.offset(Duration.millis(12L));
      processingTimeTimer.setRelative();
    }

    @OnTimer("processing")
    public void processingTimer(
        OnTimerContext context,
        @StateId("bag") BagState<String> bagState,
        @TimerId("event") Timer eventTimeTimer,
        @TimerId("processing") Timer processingTimeTimer) {
      context.output("processing" + Iterables.toString(bagState.read()));
      bagState.add("processing");
      eventTimeTimer.withOutputTimestamp(context.timestamp()).set(context.timestamp().plus(21L));
      processingTimeTimer.offset(Duration.millis(22L));
      processingTimeTimer.setRelative();
    }
  }

  @Test
  public void testTimers() throws Exception {
    dateTimeProvider.setDateTimeFixed(10000L);

    Pipeline p = Pipeline.create();
    PCollection<KV<String, String>> valuePCollection =
        p.apply(Create.of(KV.of("unused", "unused")));
    PCollection<String> outputPCollection =
        valuePCollection.apply(TEST_TRANSFORM_ID, ParDo.of(new TestTimerfulDoFn()));

    SdkComponents sdkComponents = SdkComponents.create();
    sdkComponents.registerEnvironment(Environment.getDefaultInstance());
    RunnerApi.Pipeline pProto = PipelineTranslation.toProto(p, sdkComponents);
    String inputPCollectionId = sdkComponents.registerPCollection(valuePCollection);
    String outputPCollectionId = sdkComponents.registerPCollection(outputPCollection);

    RunnerApi.PTransform pTransform =
        pProto
            .getComponents()
            .getTransformsOrThrow(
                pProto.getComponents().getTransformsOrThrow(TEST_TRANSFORM_ID).getSubtransforms(0))
            .toBuilder()
            .build();

    FakeBeamFnStateClient fakeStateClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                bagUserStateKey("bag", "X"), encode("X0"),
                bagUserStateKey("bag", "A"), encode("A0"),
                bagUserStateKey("bag", "C"), encode("C0")));
    FakeBeamFnTimerClient fakeTimerClient = new FakeBeamFnTimerClient();

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        outputPCollectionId,
        TEST_TRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);

    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeStateClient,
            fakeTimerClient,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* splitListener */,
            null /* bundleFinalizer */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    LogicalEndpoint eventTimer = LogicalEndpoint.timer("57L", TEST_TRANSFORM_ID, "ts-event");
    LogicalEndpoint processingTimer =
        LogicalEndpoint.timer("57L", TEST_TRANSFORM_ID, "ts-processing");
    // Ensure that bag user state that is initially empty or populated works.
    // Ensure that the key order does not matter when we traverse over KV pairs.
    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    mainInput.accept(timestampedValueInGlobalWindow(KV.of("X", "X1"), new Instant(1000L)));
    mainInput.accept(timestampedValueInGlobalWindow(KV.of("Y", "Y1"), new Instant(1100L)));
    mainInput.accept(timestampedValueInGlobalWindow(KV.of("X", "X2"), new Instant(1200L)));
    mainInput.accept(timestampedValueInGlobalWindow(KV.of("Y", "Y2"), new Instant(1300L)));
    fakeTimerClient.sendTimer(
        eventTimer, timerInGlobalWindow("A", new Instant(1400L), new Instant(2400L)));
    fakeTimerClient.sendTimer(
        eventTimer, timerInGlobalWindow("B", new Instant(1500L), new Instant(2500L)));
    fakeTimerClient.sendTimer(
        eventTimer, timerInGlobalWindow("A", new Instant(1600L), new Instant(2600L)));
    fakeTimerClient.sendTimer(
        processingTimer, timerInGlobalWindow("X", new Instant(1700L), new Instant(2700L)));
    fakeTimerClient.sendTimer(
        processingTimer, timerInGlobalWindow("C", new Instant(1800L), new Instant(2800L)));
    fakeTimerClient.sendTimer(
        processingTimer, timerInGlobalWindow("B", new Instant(1900L), new Instant(2900L)));
    assertThat(
        mainOutputValues,
        contains(
            timestampedValueInGlobalWindow("mainX[X0]", new Instant(1000L)),
            timestampedValueInGlobalWindow("mainY[]", new Instant(1100L)),
            timestampedValueInGlobalWindow("mainX[X0, X1]", new Instant(1200L)),
            timestampedValueInGlobalWindow("mainY[Y1]", new Instant(1300L)),
            timestampedValueInGlobalWindow("event[A0]", new Instant(1400L)),
            timestampedValueInGlobalWindow("event[]", new Instant(1500L)),
            timestampedValueInGlobalWindow("event[A0, event]", new Instant(1600L)),
            timestampedValueInGlobalWindow("processing[X0, X1, X2]", new Instant(1700L)),
            timestampedValueInGlobalWindow("processing[C0]", new Instant(1800L)),
            timestampedValueInGlobalWindow("processing[event]", new Instant(1900L))));
    assertThat(
        fakeTimerClient.getTimers(eventTimer),
        contains(
            timerInGlobalWindow("X", new Instant(1000L), new Instant(1001L)),
            timerInGlobalWindow("Y", new Instant(1100L), new Instant(1101L)),
            timerInGlobalWindow("X", new Instant(1200L), new Instant(1201L)),
            timerInGlobalWindow("Y", new Instant(1300L), new Instant(1301L)),
            timerInGlobalWindow("A", new Instant(1400L), new Instant(2411L)),
            timerInGlobalWindow("B", new Instant(1500L), new Instant(2511L)),
            timerInGlobalWindow("A", new Instant(1600L), new Instant(2611L)),
            timerInGlobalWindow("X", new Instant(1700L), new Instant(1721L)),
            timerInGlobalWindow("C", new Instant(1800L), new Instant(1821L)),
            timerInGlobalWindow("B", new Instant(1900L), new Instant(1921L))));
    assertThat(
        fakeTimerClient.getTimers(processingTimer),
        contains(
            timerInGlobalWindow("X", new Instant(1000L), new Instant(10002L)),
            timerInGlobalWindow("Y", new Instant(1100L), new Instant(10002L)),
            timerInGlobalWindow("X", new Instant(1200L), new Instant(10002L)),
            timerInGlobalWindow("Y", new Instant(1300L), new Instant(10002L)),
            timerInGlobalWindow("A", new Instant(1400L), new Instant(10012L)),
            timerInGlobalWindow("B", new Instant(1500L), new Instant(10012L)),
            timerInGlobalWindow("A", new Instant(1600L), new Instant(10012L)),
            timerInGlobalWindow("X", new Instant(1700L), new Instant(10022L)),
            timerInGlobalWindow("C", new Instant(1800L), new Instant(10022L)),
            timerInGlobalWindow("B", new Instant(1900L), new Instant(10022L))));
    mainOutputValues.clear();

    assertFalse(fakeTimerClient.isOutboundClosed(eventTimer));
    assertFalse(fakeTimerClient.isOutboundClosed(processingTimer));
    fakeTimerClient.closeInbound(eventTimer);
    fakeTimerClient.closeInbound(processingTimer);

    Iterables.getOnlyElement(finishFunctionRegistry.getFunctions()).run();
    assertThat(mainOutputValues, empty());

    assertTrue(fakeTimerClient.isOutboundClosed(eventTimer));
    assertTrue(fakeTimerClient.isOutboundClosed(processingTimer));

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());

    assertEquals(
        ImmutableMap.<StateKey, ByteString>builder()
            .put(bagUserStateKey("bag", "X"), encode("X0", "X1", "X2", "processing"))
            .put(bagUserStateKey("bag", "Y"), encode("Y1", "Y2"))
            .put(bagUserStateKey("bag", "A"), encode("A0", "event", "event"))
            .put(bagUserStateKey("bag", "B"), encode("event", "processing"))
            .put(bagUserStateKey("bag", "C"), encode("C0", "processing"))
            .build(),
        fakeStateClient.getData());
  }

  private <T> WindowedValue<T> valueInWindow(T value, BoundedWindow window) {
    return WindowedValue.of(value, window.maxTimestamp(), window, PaneInfo.NO_FIRING);
  }

  private <K> org.apache.beam.runners.core.construction.Timer<K> timerInGlobalWindow(
      K userKey, Instant holdTimestamp, Instant fireTimestamp) {
    return org.apache.beam.runners.core.construction.Timer.of(
        userKey,
        "",
        Collections.singletonList(GlobalWindow.INSTANCE),
        fireTimestamp,
        holdTimestamp,
        PaneInfo.NO_FIRING);
  }

  /**
   * Produces a multimap side input {@link StateKey} for the test PTransform id in the global
   * window.
   */
  private StateKey multimapSideInputKey(String sideInputId, ByteString key) throws IOException {
    return multimapSideInputKey(
        sideInputId,
        key,
        ByteString.copyFrom(
            CoderUtils.encodeToByteArray(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE)));
  }

  /**
   * Produces a multimap side input {@link StateKey} for the test PTransform id in the supplied
   * window.
   */
  private StateKey multimapSideInputKey(String sideInputId, ByteString key, ByteString windowKey) {
    return StateKey.newBuilder()
        .setMultimapSideInput(
            StateKey.MultimapSideInput.newBuilder()
                .setTransformId(TEST_TRANSFORM_ID)
                .setSideInputId(sideInputId)
                .setKey(key)
                .setWindow(windowKey))
        .build();
  }

  private ByteString encode(String... values) throws IOException {
    ByteString.Output out = ByteString.newOutput();
    for (String value : values) {
      StringUtf8Coder.of().encode(value, out);
    }
    return out.toByteString();
  }

  @Test
  public void testRegistration() {
    for (PTransformRunnerFactory.Registrar registrar :
        ServiceLoader.load(PTransformRunnerFactory.Registrar.class)) {
      if (registrar instanceof FnApiDoFnRunner.Registrar) {
        assertThat(
            registrar.getPTransformRunnerFactories(),
            IsMapContaining.hasKey(PTransformTranslation.PAR_DO_TRANSFORM_URN));
        return;
      }
    }
    fail("Expected registrar not found.");
  }

  static class TestSplittableDoFn extends DoFn<String, String> {
    private final PCollectionView<String> singletonSideInput;

    private TestSplittableDoFn(PCollectionView<String> singletonSideInput) {
      this.singletonSideInput = singletonSideInput;
    }

    @ProcessElement
    public ProcessContinuation processElement(
        ProcessContext context,
        RestrictionTracker<OffsetRange, Long> tracker,
        ManualWatermarkEstimator<Instant> watermarkEstimator) {
      int upperBound = Integer.parseInt(context.sideInput(singletonSideInput));
      for (int i = 0; i < upperBound; ++i) {
        if (tracker.tryClaim((long) i)) {
          context.outputWithTimestamp(
              context.element() + ":" + i, GlobalWindow.TIMESTAMP_MIN_VALUE.plus(i));
          watermarkEstimator.setWatermark(GlobalWindow.TIMESTAMP_MIN_VALUE.plus(i));
        }
      }
      if (tracker.currentRestriction().getTo() > upperBound) {
        return ProcessContinuation.resume().withResumeDelay(Duration.millis(42L));
      } else {
        return ProcessContinuation.stop();
      }
    }

    @GetInitialRestriction
    public OffsetRange restriction(@Element String element) {
      return new OffsetRange(0, Integer.parseInt(element));
    }

    @NewTracker
    public RestrictionTracker<OffsetRange, Long> newTracker(@Restriction OffsetRange restriction) {
      return new OffsetRangeTracker(restriction);
    }

    @SplitRestriction
    public void splitRange(@Restriction OffsetRange range, OutputReceiver<OffsetRange> receiver) {
      receiver.output(new OffsetRange(range.getFrom(), (range.getFrom() + range.getTo()) / 2));
      receiver.output(new OffsetRange((range.getFrom() + range.getTo()) / 2, range.getTo()));
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimatorState() {
      return GlobalWindow.TIMESTAMP_MIN_VALUE;
    }

    @NewWatermarkEstimator
    public WatermarkEstimators.Manual newWatermarkEstimator(
        @WatermarkEstimatorState Instant watermark) {
      return new WatermarkEstimators.Manual(watermark);
    }
  }

  @Test
  public void testProcessElementForSizedElementAndRestriction() throws Exception {
    Pipeline p = Pipeline.create();
    PCollection<String> valuePCollection = p.apply(Create.of("unused"));
    PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
    valuePCollection.apply(
        TEST_TRANSFORM_ID,
        ParDo.of(new TestSplittableDoFn(singletonSideInputView))
            .withSideInputs(singletonSideInputView));

    RunnerApi.Pipeline pProto =
        ProtoOverrides.updateTransform(
            PTransformTranslation.PAR_DO_TRANSFORM_URN,
            PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
            SplittableParDoExpander.createSizedReplacement());
    String expandedTransformId =
        Iterables.find(
                pProto.getComponents().getTransformsMap().entrySet(),
                entry ->
                    entry
                            .getValue()
                            .getSpec()
                            .getUrn()
                            .equals(
                                PTransformTranslation
                                    .SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN)
                        && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
            .getKey();
    RunnerApi.PTransform pTransform =
        pProto.getComponents().getTransformsOrThrow(expandedTransformId);
    String inputPCollectionId =
        pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
    String outputPCollectionId = pTransform.getOutputsOrThrow("output");

    ImmutableMap<StateKey, ByteString> stateData =
        ImmutableMap.of(
            multimapSideInputKey(singletonSideInputView.getTagInternal().getId(), ByteString.EMPTY),
            encode("3"));

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(stateData);

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        outputPCollectionId,
        TEST_TRANSFORM_ID,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();
    List<BundleApplication> primarySplits = new ArrayList<>();
    List<DelayedBundleApplication> residualSplits = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            new BundleSplitListener() {
              @Override
              public void split(
                  List<BundleApplication> primaryRoots,
                  List<DelayedBundleApplication> residualRoots) {
                primarySplits.addAll(primaryRoots);
                residualSplits.addAll(residualRoots);
              }
            },
            null /* bundleFinalizer */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    mainInput.accept(
        valueInGlobalWindow(
            KV.of(
                KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)), 5.0)));
    BundleApplication primaryRoot = Iterables.getOnlyElement(primarySplits);
    DelayedBundleApplication residualRoot = Iterables.getOnlyElement(residualSplits);
    assertEquals(ParDoTranslation.getMainInputName(pTransform), primaryRoot.getInputId());
    assertEquals(TEST_TRANSFORM_ID, primaryRoot.getTransformId());
    assertEquals(
        ParDoTranslation.getMainInputName(pTransform), residualRoot.getApplication().getInputId());
    assertEquals(TEST_TRANSFORM_ID, residualRoot.getApplication().getTransformId());
    Instant expectedOutputWatermark =
        GlobalWindow.TIMESTAMP_MIN_VALUE.plus(
            2); // side input upperBound is 3 hence we only process the first two elements
    assertEquals(
        ImmutableMap.of(
            "output",
            org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(expectedOutputWatermark.getMillis() / 1000)
                .setNanos((int) (expectedOutputWatermark.getMillis() % 1000) * 1000000)
                .build()),
        residualRoot.getApplication().getOutputWatermarksMap());
    primarySplits.clear();
    residualSplits.clear();

    mainInput.accept(
        valueInGlobalWindow(
            KV.of(
                KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)), 2.0)));
    assertThat(
        mainOutputValues,
        contains(
            timestampedValueInGlobalWindow("5:0", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(0)),
            timestampedValueInGlobalWindow("5:1", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(1)),
            timestampedValueInGlobalWindow("5:2", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(2)),
            timestampedValueInGlobalWindow("2:0", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(0)),
            timestampedValueInGlobalWindow("2:1", GlobalWindow.TIMESTAMP_MIN_VALUE.plus(1))));
    assertTrue(primarySplits.isEmpty());
    assertTrue(residualSplits.isEmpty());
    mainOutputValues.clear();

    Iterables.getOnlyElement(finishFunctionRegistry.getFunctions()).run();
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());

    // Assert that state data did not change
    assertEquals(stateData, fakeClient.getData());
  }

  @Test
  public void testProcessElementForPairWithRestriction() throws Exception {
    Pipeline p = Pipeline.create();
    PCollection<String> valuePCollection = p.apply(Create.of("unused"));
    PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
    valuePCollection.apply(
        TEST_TRANSFORM_ID,
        ParDo.of(new TestSplittableDoFn(singletonSideInputView))
            .withSideInputs(singletonSideInputView));

    RunnerApi.Pipeline pProto =
        ProtoOverrides.updateTransform(
            PTransformTranslation.PAR_DO_TRANSFORM_URN,
            PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
            SplittableParDoExpander.createSizedReplacement());
    String expandedTransformId =
        Iterables.find(
                pProto.getComponents().getTransformsMap().entrySet(),
                entry ->
                    entry
                            .getValue()
                            .getSpec()
                            .getUrn()
                            .equals(PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN)
                        && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
            .getKey();
    RunnerApi.PTransform pTransform =
        pProto.getComponents().getTransformsOrThrow(expandedTransformId);
    String inputPCollectionId =
        pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
    String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(ImmutableMap.of());

    List<WindowedValue<KV<String, OffsetRange>>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(outputPCollectionId, TEST_TRANSFORM_ID, ((List) mainOutputValues)::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* bundleSplitListener */,
            null /* bundleFinalizer */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    mainInput.accept(valueInGlobalWindow("5"));
    mainInput.accept(valueInGlobalWindow("2"));
    assertThat(
        mainOutputValues,
        contains(
            valueInGlobalWindow(
                KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE))),
            valueInGlobalWindow(
                KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)))));
    mainOutputValues.clear();

    Iterables.getOnlyElement(finishFunctionRegistry.getFunctions()).run();
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());
  }

  @Test
  public void testProcessElementForSplitAndSizeRestriction() throws Exception {
    Pipeline p = Pipeline.create();
    PCollection<String> valuePCollection = p.apply(Create.of("unused"));
    PCollectionView<String> singletonSideInputView = valuePCollection.apply(View.asSingleton());
    valuePCollection.apply(
        TEST_TRANSFORM_ID,
        ParDo.of(new TestSplittableDoFn(singletonSideInputView))
            .withSideInputs(singletonSideInputView));

    RunnerApi.Pipeline pProto =
        ProtoOverrides.updateTransform(
            PTransformTranslation.PAR_DO_TRANSFORM_URN,
            PipelineTranslation.toProto(p, SdkComponents.create(p.getOptions()), true),
            SplittableParDoExpander.createSizedReplacement());
    String expandedTransformId =
        Iterables.find(
                pProto.getComponents().getTransformsMap().entrySet(),
                entry ->
                    entry
                            .getValue()
                            .getSpec()
                            .getUrn()
                            .equals(
                                PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN)
                        && entry.getValue().getUniqueName().contains(TEST_TRANSFORM_ID))
            .getKey();
    RunnerApi.PTransform pTransform =
        pProto.getComponents().getTransformsOrThrow(expandedTransformId);
    String inputPCollectionId =
        pTransform.getInputsOrThrow(ParDoTranslation.getMainInputName(pTransform));
    String outputPCollectionId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());

    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(ImmutableMap.of());

    List<WindowedValue<KV<KV<String, OffsetRange>, Double>>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(outputPCollectionId, TEST_TRANSFORM_ID, ((List) mainOutputValues)::add);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    new FnApiDoFnRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            fakeClient,
            null /* beamFnTimerClient */,
            TEST_TRANSFORM_ID,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            pProto.getComponents().getPcollectionsMap(),
            pProto.getComponents().getCodersMap(),
            pProto.getComponents().getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            teardownFunctions::add,
            null /* bundleSplitListener */,
            null /* bundleFinalizer */);

    Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
    mainOutputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(inputPCollectionId, outputPCollectionId));

    FnDataReceiver<WindowedValue<?>> mainInput =
        consumers.getMultiplexingConsumer(inputPCollectionId);
    mainInput.accept(
        valueInGlobalWindow(
            KV.of("5", KV.of(new OffsetRange(0, 5), GlobalWindow.TIMESTAMP_MIN_VALUE))));
    mainInput.accept(
        valueInGlobalWindow(
            KV.of("2", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE))));
    assertThat(
        mainOutputValues,
        contains(
            valueInGlobalWindow(
                KV.of(
                    KV.of("5", KV.of(new OffsetRange(0, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    2.0)),
            valueInGlobalWindow(
                KV.of(
                    KV.of("5", KV.of(new OffsetRange(2, 5), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    3.0)),
            valueInGlobalWindow(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(0, 1), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    1.0)),
            valueInGlobalWindow(
                KV.of(
                    KV.of("2", KV.of(new OffsetRange(1, 2), GlobalWindow.TIMESTAMP_MIN_VALUE)),
                    1.0))));
    mainOutputValues.clear();

    Iterables.getOnlyElement(finishFunctionRegistry.getFunctions()).run();
    assertThat(mainOutputValues, empty());

    Iterables.getOnlyElement(teardownFunctions).run();
    assertThat(mainOutputValues, empty());
  }
}
