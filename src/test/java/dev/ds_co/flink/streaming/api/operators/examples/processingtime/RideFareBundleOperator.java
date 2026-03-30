package dev.ds_co.flink.streaming.api.operators.examples.processingtime;

import dev.ds_co.flink.streaming.api.operators.KeyedMultiInputOperator3;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.util.Collector;

/**
 * Example keyed three-input operator in the same spirit as Apache Flink training's
 * <i>rides-and-fares</i> exercise: three <strong>different</strong> event types ({@link RideEvent},
 * {@link FareEvent}, {@link TipEvent}) share a common key ({@code rideId}), so the library can wire
 * typed {@code processElement} callbacks instead of a union type and a single dispatch switch.
 *
 * <p>The operator keeps the latest value per input in state and emits one {@link RideFareBundle}
 * per key after a <strong>processing-time quiet period</strong>: each input reschedules a timer at
 * {@code currentProcessingTime() + QUIET_PERIOD_MS}, deleting the previous registration. When the
 * quiet period elapses with no further input for that key, {@link #onTimer} emits a bundle with
 * whatever components have been seen (missing legs remain {@code null}).
 *
 * <p>This demonstrates {@link
 * dev.ds_co.flink.streaming.api.operators.BaseMultiInputKeyedOperator.Context#registerProcessingTimeTimer(long)},
 * {@link
 * dev.ds_co.flink.streaming.api.operators.BaseMultiInputKeyedOperator.Context#deleteProcessingTimeTimer(long)},
 * {@link
 * dev.ds_co.flink.streaming.api.operators.BaseMultiInputKeyedOperator.Context#currentProcessingTime()},
 * and processing-time {@code onTimer}. For a follow-up transformation (sum, filter, etc.), apply it
 * downstream; bundle assembly and debouncing are intentionally separated from that phase.
 *
 * <p><strong>Event time:</strong> this operator only decides <em>when</em> to emit in processing
 * time. If downstream windows need event time, assign timestamps and watermarks on the output
 * {@link org.apache.flink.streaming.api.datastream.DataStream} (e.g. {@code
 * max(rideTime,fareTime,tipTime)} or the last contributing record) according to your job's
 * semantics.
 */
public class RideFareBundleOperator
    extends KeyedMultiInputOperator3<RideEvent, FareEvent, TipEvent, RideFareBundle> {

  static final long QUIET_PERIOD_MS = 2000L;

  private transient ValueState<RideEvent> lastRide;
  private transient ValueState<FareEvent> lastFare;
  private transient ValueState<TipEvent> lastTip;
  private transient ValueState<Long> registeredTimerDeadline;

  public RideFareBundleOperator(StreamOperatorParameters<RideFareBundle> params) {
    super(params);
  }

  @Override
  public void open() throws Exception {
    super.open();
    var store =
        getKeyedStateStore()
            .orElseThrow(
                () -> new IllegalStateException("RideFareBundleOperator requires keyed state"));
    lastRide =
        store.getState(new ValueStateDescriptor<>("ride", TypeInformation.of(RideEvent.class)));
    lastFare =
        store.getState(new ValueStateDescriptor<>("fare", TypeInformation.of(FareEvent.class)));
    lastTip = store.getState(new ValueStateDescriptor<>("tip", TypeInformation.of(TipEvent.class)));
    registeredTimerDeadline =
        store.getState(new ValueStateDescriptor<>("registeredTimerDeadline", Types.LONG));
  }

  @Override
  protected void processElement1(RideEvent ride, Context ctx, Collector<RideFareBundle> out)
      throws Exception {
    lastRide.update(ride);
    rescheduleQuietTimer(ctx);
  }

  @Override
  protected void processElement2(FareEvent fare, Context ctx, Collector<RideFareBundle> out)
      throws Exception {
    lastFare.update(fare);
    rescheduleQuietTimer(ctx);
  }

  @Override
  protected void processElement3(TipEvent tip, Context ctx, Collector<RideFareBundle> out)
      throws Exception {
    lastTip.update(tip);
    rescheduleQuietTimer(ctx);
  }

  private void rescheduleQuietTimer(Context ctx) throws Exception {
    Long previousDeadline = registeredTimerDeadline.value();
    if (previousDeadline != null) {
      ctx.deleteProcessingTimeTimer(previousDeadline);
    }
    long deadline = ctx.currentProcessingTime() + QUIET_PERIOD_MS;
    ctx.registerProcessingTimeTimer(deadline);
    registeredTimerDeadline.update(deadline);
  }

  @Override
  protected void onTimer(OnTimerContext ctx) throws Exception {
    if (ctx.timeDomain() != TimeDomain.PROCESSING_TIME) {
      return;
    }
    Long expectedDeadline = registeredTimerDeadline.value();
    if (expectedDeadline == null || expectedDeadline != ctx.timestamp()) {
      return;
    }
    long rideId = ctx.getCurrentKey(Long.class);
    out.collect(new RideFareBundle(rideId, lastRide.value(), lastFare.value(), lastTip.value()));
    clearAll();
  }

  private void clearAll() throws Exception {
    lastRide.clear();
    lastFare.clear();
    lastTip.clear();
    registeredTimerDeadline.clear();
  }
}
