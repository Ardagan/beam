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

public class UserMonitoringInfoToCounterUpdateTransformerTest {

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
    Map<String, DataflowStepContext> stepContextMapping = new HashMap<>();
    UserMonitoringInfoToCounterUpdateTransformer testObject =
        new UserMonitoringInfoToCounterUpdateTransformer(mockSpecValidator, stepContextMapping);
    Optional<String> error = Optional.of("Error text");
    when(mockSpecValidator.validate(any())).thenReturn(error);
    assertEquals(null, testObject.transform(null));
  }

  @Test
  public void testTransformThrowsIfMonitoringInfoWithWrongUrnPrefixReceived() {
    Map<String, DataflowStepContext> stepContextMapping = new HashMap<>();
    MonitoringInfo monitoringInfo = MonitoringInfo.newBuilder()
        .setUrn("beam:metric:element_count:v1").build();
    UserMonitoringInfoToCounterUpdateTransformer testObject =
        new UserMonitoringInfoToCounterUpdateTransformer(mockSpecValidator, stepContextMapping);
    when(mockSpecValidator.validate(any())).thenReturn(Optional.empty());

    exception.expect(RuntimeException.class);
    testObject.transform(monitoringInfo);
  }

  @Test
  public void testTransformReturnsNullIfMonitoringInfoWithUnknownPTransformLabelPresent() {
    Map<String, DataflowStepContext> stepContextMapping = new HashMap<>();
    MonitoringInfo monitoringInfo = MonitoringInfo.newBuilder()
        .setUrn("beam:metric:user:anyNamespace:anyName").putLabels("PTRANSFORM", "anyValue")
        .build();
    UserMonitoringInfoToCounterUpdateTransformer testObject =
        new UserMonitoringInfoToCounterUpdateTransformer(mockSpecValidator, stepContextMapping);
    when(mockSpecValidator.validate(any())).thenReturn(Optional.empty());
    assertEquals(null, testObject.transform(monitoringInfo));
  }

  @Test
  public void testTransformReturnsValidCounterUpdateWhenValidUserMonitoringInfoReceived() {
    Map<String, DataflowStepContext> stepContextMapping = new HashMap<>();
    NameContext nc = NameContext
        .create("anyStageName", "anyOriginalName", "anySystemName", "anyUserName");
    DataflowStepContext dsc = mock(DataflowStepContext.class);
    when(dsc.getNameContext()).thenReturn(nc);
    stepContextMapping.put("anyValue", dsc);

    MonitoringInfo monitoringInfo = MonitoringInfo.newBuilder()
        .setUrn("beam:metric:user:anyNamespace:anyName").putLabels("PTRANSFORM", "anyValue")
        .build();
    UserMonitoringInfoToCounterUpdateTransformer testObject =
        new UserMonitoringInfoToCounterUpdateTransformer(mockSpecValidator, stepContextMapping);
    when(mockSpecValidator.validate(any())).thenReturn(Optional.empty());

    CounterUpdate result = testObject.transform(monitoringInfo);
    assertNotEquals(null, result);

    assertEquals(
        "{cumulative=true, integer={highBits=0, lowBits=0}, "
            + "structuredNameAndMetadata={metadata={kind=SUM}, "
            + "name={name=anyName, origin=USER, originNamespace=anyNamespace, "
            + "originalStepName=anyOriginalName}}}",
        result.toString());
  }
}