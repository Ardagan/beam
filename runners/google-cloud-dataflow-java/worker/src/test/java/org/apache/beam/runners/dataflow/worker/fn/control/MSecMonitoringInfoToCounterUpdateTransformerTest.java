package org.apache.beam.runners.dataflow.worker.fn.control;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.CounterUpdate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.SpecMonitoringInfoValidator;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class MSecMonitoringInfoToCounterUpdateTransformerTest {

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Mock
  private SpecMonitoringInfoValidator mockSpecValidator;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testTransformReturnsNullIfSpecValidationFails() {
    Map<String, String> counterNameMapping = new HashMap<>();
    counterNameMapping.put("beam:counter:supported", "supportedCounter");

    Map<String, DataflowStepContext> stepContextMapping = new HashMap<>();

    MSecMonitoringInfoToCounterUpdateTransformer testObject =
        new MSecMonitoringInfoToCounterUpdateTransformer(mockSpecValidator, stepContextMapping,
            counterNameMapping);

    Optional<String> error = Optional.of("Error text");
    when(mockSpecValidator.validate(any())).thenReturn(error);

    MonitoringInfo monitoringInfo = MonitoringInfo.newBuilder()
        .setUrn("beam:metric:pardo_execution_time:start_bundle_msecs:v1:invalid").build();
    assertEquals(null, testObject.transform(monitoringInfo));
  }

  @Test
  public void testTransformThrowsIfMonitoringInfoWithUnknownUrnReceived() {
    Map<String, String> counterNameMapping = new HashMap<>();
    counterNameMapping.put("beam:counter:supported", "supportedCounter");

    Map<String, DataflowStepContext> stepContextMapping = new HashMap<>();
    MonitoringInfo monitoringInfo = MonitoringInfo.newBuilder()
        .setUrn("beam:metric:pardo_execution_time:start_bundle_msecs:v1:invalid").build();

    MSecMonitoringInfoToCounterUpdateTransformer testObject =
        new MSecMonitoringInfoToCounterUpdateTransformer(mockSpecValidator, stepContextMapping,
            counterNameMapping);

    when(mockSpecValidator.validate(any())).thenReturn(Optional.empty());

    exception.expect(RuntimeException.class);
    testObject.transform(monitoringInfo);
  }

  @Test
  public void testTransformThrowsIfMonitoringInfoWithUnknownPTransformLabelPresent() {
    Map<String, String> counterNameMapping = new HashMap<>();
    counterNameMapping.put("beam:counter:supported", "supportedCounter");

    Map<String, DataflowStepContext> stepContextMapping = new HashMap<>();

    MSecMonitoringInfoToCounterUpdateTransformer testObject =
        new MSecMonitoringInfoToCounterUpdateTransformer(mockSpecValidator, stepContextMapping);

    when(mockSpecValidator.validate(any())).thenReturn(Optional.empty());

    MonitoringInfo monitoringInfo = MonitoringInfo.newBuilder()
        .setUrn("beam:counter:unsupported")
        .putLabels("PTRANSFORM", "anyValue")
        .build();

    exception.expect(RuntimeException.class);
    testObject.transform(monitoringInfo);
  }

  @Test
  public void testTransformReturnsValidCounterUpdateWhenValidMSecMonitoringInfoReceived() {
    // Setup
    Map<String, String> counterNameMapping = new HashMap<>();
    counterNameMapping.put("beam:counter:supported", "supportedCounter");

    Map<String, DataflowStepContext> stepContextMapping = new HashMap<>();
    NameContext nc = NameContext
        .create("anyStageName", "anyOriginalName", "anySystemName", "anyUserName");
    DataflowStepContext dsc = mock(DataflowStepContext.class);
    when(dsc.getNameContext()).thenReturn(nc);
    stepContextMapping.put("anyValue", dsc);

    MSecMonitoringInfoToCounterUpdateTransformer testObject =
        new MSecMonitoringInfoToCounterUpdateTransformer(mockSpecValidator, stepContextMapping,
            counterNameMapping);
    when(mockSpecValidator.validate(any())).thenReturn(Optional.empty());

    // Execute
    MonitoringInfo monitoringInfo = MonitoringInfo.newBuilder()
        .setUrn("beam:counter:supported").putLabels("PTRANSFORM", "anyValue")
        .build();

    CounterUpdate result = testObject.transform(monitoringInfo);

    // Validate
    assertNotEquals(null, result);

    assertEquals(
        "{cumulative=true, integer={highBits=0, lowBits=0}, "
            + "structuredNameAndMetadata={metadata={kind=SUM}, "
            + "name={executionStepName=anyStageName, name=supportedCounter, origin=SYSTEM, "
            + "originalStepName=anyOriginalName}}}",
        result.toString());
  }

  @Test
  public void testCreateKnownUrnToCounterNameMappingRetursExpectedValues() {
    Map<String, DataflowStepContext> stepContextMapping = new HashMap<>();
    MSecMonitoringInfoToCounterUpdateTransformer testObject =
        new MSecMonitoringInfoToCounterUpdateTransformer(mockSpecValidator, stepContextMapping);
    Map<String, String> result = testObject.createKnownUrnToCounterNameMapping();
    assertEquals("process-msecs",
        result.get("beam:metric:pardo_execution_time:process_bundle_msecs:v1"));
    assertEquals("finish-msecs",
        result.get("beam:metric:pardo_execution_time:finish_bundle_msecs:v1"));
    assertEquals("start-msecs",
        result.get("beam:metric:pardo_execution_time:start_bundle_msecs:v1"));
  }
}