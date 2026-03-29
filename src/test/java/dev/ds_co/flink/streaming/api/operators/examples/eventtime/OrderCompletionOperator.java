package dev.ds_co.flink.streaming.api.operators.examples.eventtime;

import dev.ds_co.flink.streaming.api.operators.KeyedMultiInputOperator3;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Example keyed three-input operator that demonstrates event-time timers in the same spirit as
 * Apache Flink training's Long Ride Alerts: one stream supplies the trigger (order), the others
 * supply completion signals (payment, shipment). When an {@link Order} arrives, the operator
 * registers an event-time timer at {@code order event time + COMPLETION_DEADLINE_MS}. If both
 * {@link Payment} and {@link Shipment} arrive for the same key before that deadline, it deletes the
 * timer and emits {@link OrderResult.Status#COMPLETED} on the main output. If the watermark passes
 * the deadline first, it emits {@link OrderResult.Status#TIMED_OUT} to {@link #TIMED_OUT_ORDERS}
 * with flags showing which signals were seen.
 *
 * <p>Records without an event time ({@link
 * dev.ds_co.flink.streaming.api.operators.BaseMultiInputKeyedOperator.Context#hasTimestamp()
 * Context.hasTimestamp()} false) are ignored.
 *
 * <p>This exercises {@link
 * dev.ds_co.flink.streaming.api.operators.BaseMultiInputKeyedOperator.Context#registerEventTimeTimer(long)},
 * {@link
 * dev.ds_co.flink.streaming.api.operators.BaseMultiInputKeyedOperator.Context#deleteEventTimeTimer(long)},
 * and {@link dev.ds_co.flink.streaming.api.operators.KeyedMultiInputOperator3} timer callbacks.
 */
public class OrderCompletionOperator
    extends KeyedMultiInputOperator3<Order, Payment, Shipment, OrderResult> {

  public static final OutputTag<OrderResult> TIMED_OUT_ORDERS =
      new OutputTag<OrderResult>("timed-out-orders") {};

  static final long COMPLETION_DEADLINE_MS = 5000L;

  private transient ValueState<Order> orderState;
  private transient ValueState<Boolean> hasPayment;
  private transient ValueState<Boolean> hasShipment;
  private transient ValueState<Long> registeredTimerDeadline;

  public OrderCompletionOperator(StreamOperatorParameters<OrderResult> params) {
    super(params);
  }

  @Override
  public void open() throws Exception {
    super.open();
    var store =
        getKeyedStateStore()
            .orElseThrow(
                () -> new IllegalStateException("OrderCompletionOperator requires keyed state"));
    orderState =
        store.getState(new ValueStateDescriptor<>("order", TypeInformation.of(Order.class)));
    hasPayment = store.getState(new ValueStateDescriptor<>("hasPayment", Types.BOOLEAN));
    hasShipment = store.getState(new ValueStateDescriptor<>("hasShipment", Types.BOOLEAN));
    registeredTimerDeadline =
        store.getState(new ValueStateDescriptor<>("registeredTimerDeadline", Types.LONG));
  }

  @Override
  protected void processElement1(Order order, Context ctx, Collector<OrderResult> out)
      throws Exception {
    if (!ctx.hasTimestamp()) {
      return;
    }
    orderState.update(order);
    Long previousDeadline = registeredTimerDeadline.value();
    if (previousDeadline != null) {
      ctx.deleteEventTimeTimer(previousDeadline);
    }
    long deadline = ctx.timestamp() + COMPLETION_DEADLINE_MS;
    ctx.registerEventTimeTimer(deadline);
    registeredTimerDeadline.update(deadline);
    tryComplete(ctx, out);
  }

  @Override
  protected void processElement2(Payment payment, Context ctx, Collector<OrderResult> out)
      throws Exception {
    if (!ctx.hasTimestamp()) {
      return;
    }
    hasPayment.update(true);
    tryComplete(ctx, out);
  }

  @Override
  protected void processElement3(Shipment shipment, Context ctx, Collector<OrderResult> out)
      throws Exception {
    if (!ctx.hasTimestamp()) {
      return;
    }
    hasShipment.update(true);
    tryComplete(ctx, out);
  }

  private void tryComplete(Context ctx, Collector<OrderResult> out) throws Exception {
    Order order = orderState.value();
    if (order == null) {
      return;
    }
    if (!Boolean.TRUE.equals(hasPayment.value()) || !Boolean.TRUE.equals(hasShipment.value())) {
      return;
    }
    Long deadline = registeredTimerDeadline.value();
    if (deadline != null) {
      ctx.deleteEventTimeTimer(deadline);
    }
    out.collect(new OrderResult(order.getOrderId(), OrderResult.Status.COMPLETED, true, true));
    clearAll();
  }

  @Override
  protected void onTimer(OnTimerContext ctx) throws Exception {
    if (ctx.timeDomain() != TimeDomain.EVENT_TIME) {
      return;
    }
    Long expectedDeadline = registeredTimerDeadline.value();
    if (expectedDeadline == null || expectedDeadline != ctx.timestamp()) {
      return;
    }
    Order order = orderState.value();
    if (order == null) {
      registeredTimerDeadline.clear();
      return;
    }
    boolean hasPaymentRecorded = Boolean.TRUE.equals(hasPayment.value());
    boolean hasShipmentRecorded = Boolean.TRUE.equals(hasShipment.value());
    if (hasPaymentRecorded && hasShipmentRecorded) {
      out.collect(new OrderResult(order.getOrderId(), OrderResult.Status.COMPLETED, true, true));
      clearAll();
      return;
    }
    ctx.output(
        TIMED_OUT_ORDERS,
        new OrderResult(
            order.getOrderId(),
            OrderResult.Status.TIMED_OUT,
            hasPaymentRecorded,
            hasShipmentRecorded));
    clearAll();
  }

  private void clearAll() throws Exception {
    orderState.clear();
    hasPayment.clear();
    hasShipment.clear();
    registeredTimerDeadline.clear();
  }
}
