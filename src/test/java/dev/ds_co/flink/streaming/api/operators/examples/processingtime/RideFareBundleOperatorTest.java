package dev.ds_co.flink.streaming.api.operators.examples.processingtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.ds_co.flink.streaming.util.testing.KeyedMultiInputOperatorTestHarness3;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies {@link RideFareBundleOperator} with {@link KeyedMultiInputOperatorTestHarness3}.
 * Processing time advances only via {@link
 * org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness#setProcessingTime(long)
 * setProcessingTime}; unlike a live job, the harness does not advance wall clock when elements are
 * processed, so debounce deadlines are fully deterministic.
 */
class RideFareBundleOperatorTest {

  private KeyedMultiInputOperatorTestHarness3<Long, RideEvent, FareEvent, TipEvent, RideFareBundle>
      harness;

  @BeforeEach
  void setUp() throws Exception {
    harness =
        new KeyedMultiInputOperatorTestHarness3<>(
            RideFareBundleOperator.class,
            RideEvent::getRideId,
            FareEvent::getRideId,
            TipEvent::getRideId,
            Types.LONG);
    harness.setup();
    harness.open();
  }

  @AfterEach
  void tearDown() throws Exception {
    harness.close();
  }

  /**
   * Three inputs in one burst reschedule a single quiet-period timer; advancing processing time
   * past that deadline yields one {@link RideFareBundle} with ride, fare, and tip.
   */
  @Test
  void emitsSingleBundleAfterBurstThenQuiet() throws Exception {
    harness.processElement1(new StreamRecord<>(new RideEvent(1L, 2)));
    harness.processElement2(new StreamRecord<>(new FareEvent(1L, 20.0f)));
    harness.processElement3(new StreamRecord<>(new TipEvent(1L, 3.0f)));

    harness.setProcessingTime(RideFareBundleOperator.QUIET_PERIOD_MS + 1);

    List<RideFareBundle> out = harness.extractOutputValues();
    assertEquals(1, out.size());
    assertEquals(
        new RideFareBundle(
            1L, new RideEvent(1L, 2), new FareEvent(1L, 20.0f), new TipEvent(1L, 3.0f)),
        out.get(0));
  }

  /**
   * A late input before the quiet period ends deletes the pending timer and registers a later one;
   * advancing only to the original deadline must not emit; advancing past the rescheduled deadline
   * emits once (partial bundle if only some inputs fired).
   */
  @Test
  void resetsQuietTimerWhenInputArrivesBeforePreviousDeadline() throws Exception {
    harness.processElement1(new StreamRecord<>(new RideEvent(1L, 1)));

    harness.setProcessingTime(RideFareBundleOperator.QUIET_PERIOD_MS - 1);
    harness.processElement1(new StreamRecord<>(new RideEvent(1L, 2)));

    harness.setProcessingTime(RideFareBundleOperator.QUIET_PERIOD_MS);
    assertTrue(harness.extractOutputValues().isEmpty());

    long secondDeadline =
        RideFareBundleOperator.QUIET_PERIOD_MS - 1 + RideFareBundleOperator.QUIET_PERIOD_MS;
    harness.setProcessingTime(secondDeadline + 1);

    List<RideFareBundle> out = harness.extractOutputValues();
    assertEquals(1, out.size());
    RideFareBundle bundle = out.get(0);
    assertEquals(1L, bundle.getRideId());
    assertEquals(new RideEvent(1L, 2), bundle.getRide());
    assertNull(bundle.getFare());
    assertNull(bundle.getTip());
  }

  /** Only inputs 1 and 3 receive data: bundle keeps fare null. */
  @Test
  void partialBundleWhenOnlyTwoInputsReceiveData() throws Exception {
    harness.processElement1(new StreamRecord<>(new RideEvent(1L, 5)));
    harness.processElement3(new StreamRecord<>(new TipEvent(1L, 7.0f)));

    harness.setProcessingTime(RideFareBundleOperator.QUIET_PERIOD_MS + 1);

    List<RideFareBundle> out = harness.extractOutputValues();
    assertEquals(1, out.size());
    RideFareBundle bundle = out.get(0);
    assertEquals(1L, bundle.getRideId());
    assertEquals(new RideEvent(1L, 5), bundle.getRide());
    assertNull(bundle.getFare());
    assertEquals(new TipEvent(1L, 7.0f), bundle.getTip());
  }

  /**
   * Independent keys maintain separate debounce state; one quiet period after activity on both keys
   * yields two bundles.
   */
  @Test
  void separateKeysEmitSeparateBundles() throws Exception {
    harness.processElement1(new StreamRecord<>(new RideEvent(1L, 1)));
    harness.processElement2(new StreamRecord<>(new FareEvent(1L, 2.0f)));
    harness.processElement3(new StreamRecord<>(new TipEvent(1L, 3.0f)));

    harness.processElement1(new StreamRecord<>(new RideEvent(2L, 10)));
    harness.processElement2(new StreamRecord<>(new FareEvent(2L, 20.0f)));
    harness.processElement3(new StreamRecord<>(new TipEvent(2L, 30.0f)));

    harness.setProcessingTime(RideFareBundleOperator.QUIET_PERIOD_MS + 1);

    List<RideFareBundle> out = harness.extractOutputValues();
    out.sort(Comparator.comparing(RideFareBundle::getRideId));
    assertEquals(2, out.size());
    assertEquals(
        new RideFareBundle(
            1L, new RideEvent(1L, 1), new FareEvent(1L, 2.0f), new TipEvent(1L, 3.0f)),
        out.get(0));
    assertEquals(
        new RideFareBundle(
            2L, new RideEvent(2L, 10), new FareEvent(2L, 20.0f), new TipEvent(2L, 30.0f)),
        out.get(1));
  }
}
