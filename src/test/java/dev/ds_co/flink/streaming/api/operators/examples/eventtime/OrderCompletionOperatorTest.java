package dev.ds_co.flink.streaming.api.operators.examples.eventtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.ds_co.flink.streaming.util.testing.KeyedMultiInputOperatorTestHarness3;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies {@link OrderCompletionOperator} via {@link KeyedMultiInputOperatorTestHarness3}. The
 * harness does not run a {@code WatermarkStrategy}, so {@code StreamRecord} timestamps drive {@code
 * Context.timestamp()} but the event-time clock only advances when tests call {@code
 * processWatermark} on each input; see {@link #advanceWatermark(long)}.
 */
class OrderCompletionOperatorTest {

  private KeyedMultiInputOperatorTestHarness3<String, Order, Payment, Shipment, OrderResult>
      harness;

  @BeforeEach
  void setUp() throws Exception {
    harness =
        new KeyedMultiInputOperatorTestHarness3<>(
            OrderCompletionOperator.class,
            Order::getOrderId,
            Payment::getOrderId,
            Shipment::getOrderId,
            Types.STRING);
    harness.setup();
    harness.open();
  }

  @AfterEach
  void tearDown() throws Exception {
    harness.close();
  }

  /**
   * Order, payment, and shipment all arrive before the event-time deadline: the operator emits
   * COMPLETED on the main output and cancels the deadline timer; advancing the watermark afterward
   * produces no timed-out side output.
   */
  @Test
  void completesWhenPaymentAndShipmentArriveBeforeDeadline() throws Exception {
    harness.processElement1(new StreamRecord<>(new Order("order1", 1000L), 1000L));
    harness.processElement2(new StreamRecord<>(new Payment("order1", 2000L), 2000L));
    harness.processElement3(new StreamRecord<>(new Shipment("order1", 3000L), 3000L));

    List<OrderResult> main = harness.extractOutputValues();
    assertEquals(1, main.size());
    assertEquals(new OrderResult("order1", OrderResult.Status.COMPLETED, true, true), main.get(0));

    advanceWatermark(20_000L);
    assertTrue(extractTimedOut().isEmpty());
  }

  /**
   * Deadline passes with order and shipment but no payment: event-time timer fires and the
   * incomplete correlation is reported on the timed-out side output.
   */
  @Test
  void timesOutWhenPaymentMissing() throws Exception {
    harness.processElement1(new StreamRecord<>(new Order("order1", 1000L), 1000L));
    harness.processElement3(new StreamRecord<>(new Shipment("order1", 3000L), 3000L));

    advanceWatermark(1000L + OrderCompletionOperator.COMPLETION_DEADLINE_MS + 1);

    assertTrue(harness.extractOutputValues().isEmpty());
    List<OrderResult> timedOut = extractTimedOut();
    assertEquals(1, timedOut.size());
    assertEquals(
        new OrderResult("order1", OrderResult.Status.TIMED_OUT, false, true), timedOut.get(0));
  }

  /**
   * Deadline passes with order and payment but no shipment: timer fires and marks shipment as
   * missing on the side output.
   */
  @Test
  void timesOutWhenShipmentMissing() throws Exception {
    harness.processElement1(new StreamRecord<>(new Order("order1", 1000L), 1000L));
    harness.processElement2(new StreamRecord<>(new Payment("order1", 2000L), 2000L));

    advanceWatermark(1000L + OrderCompletionOperator.COMPLETION_DEADLINE_MS + 1);

    assertTrue(harness.extractOutputValues().isEmpty());
    List<OrderResult> timedOut = extractTimedOut();
    assertEquals(1, timedOut.size());
    assertEquals(
        new OrderResult("order1", OrderResult.Status.TIMED_OUT, true, false), timedOut.get(0));
  }

  /**
   * Only the order (trigger) arrives before the watermark passes the deadline: both secondary
   * signals are reported missing.
   */
  @Test
  void timesOutWhenOnlyOrderArrives() throws Exception {
    harness.processElement1(new StreamRecord<>(new Order("order1", 1000L), 1000L));

    advanceWatermark(1000L + OrderCompletionOperator.COMPLETION_DEADLINE_MS + 1);

    assertTrue(harness.extractOutputValues().isEmpty());
    List<OrderResult> timedOut = extractTimedOut();
    assertEquals(1, timedOut.size());
    assertEquals(
        new OrderResult("order1", OrderResult.Status.TIMED_OUT, false, false), timedOut.get(0));
  }

  /**
   * In a full job, {@code assignTimestampsAndWatermarks} on each input advances the event-time
   * clock. The operator test harness only forwards element timestamps into {@link
   * org.apache.flink.streaming.runtime.streamrecord.StreamRecord StreamRecords}; it does not run
   * watermark generators, so event-time timers are only triggered after explicit {@link
   * org.apache.flink.streaming.util.MultiInputStreamOperatorTestHarness#processWatermark(int,
   * org.apache.flink.streaming.api.watermark.Watermark) processWatermark} calls on every input.
   */
  private void advanceWatermark(long watermarkMillis) throws Exception {
    Watermark watermark = new Watermark(watermarkMillis);
    harness.processWatermark(0, watermark);
    harness.processWatermark(1, watermark);
    harness.processWatermark(2, watermark);
  }

  private List<OrderResult> extractTimedOut() {
    var records = harness.getSideOutput(OrderCompletionOperator.TIMED_OUT_ORDERS);
    if (records == null) {
      return Collections.emptyList();
    }
    List<OrderResult> list = new ArrayList<>();
    for (StreamRecord<OrderResult> r : records) {
      list.add(r.getValue());
    }
    return list;
  }
}
