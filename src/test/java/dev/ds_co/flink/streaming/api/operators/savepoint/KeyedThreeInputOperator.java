package dev.ds_co.flink.streaming.api.operators.savepoint;

import dev.ds_co.flink.streaming.api.operators.KeyedMultiInputOperator3;
import dev.ds_co.flink.streaming.api.operators.testing.Out;
import dev.ds_co.flink.streaming.api.operators.testing.X;
import dev.ds_co.flink.streaming.api.operators.testing.Y;
import dev.ds_co.flink.streaming.api.operators.testing.Z;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.util.Collector;

public class KeyedThreeInputOperator extends KeyedMultiInputOperator3<X, Y, Z, Out> {

  private transient ValueState<Integer> lastX;
  private transient ValueState<Integer> lastY;
  private transient ValueState<Integer> lastZ;

  public KeyedThreeInputOperator(StreamOperatorParameters<Out> params) {
    super(params);
  }

  @Override
  public void open() throws Exception {
    super.open();
    var store =
        getKeyedStateStore()
            .orElseThrow(
                () -> new IllegalStateException("KeyedThreeInputOperator requires keyed state"));
    lastX = store.getState(new ValueStateDescriptor<>("x", Types.INT));
    lastY = store.getState(new ValueStateDescriptor<>("y", Types.INT));
    lastZ = store.getState(new ValueStateDescriptor<>("z", Types.INT));
  }

  @Override
  protected void processElement1(X x, Context ctx, Collector<Out> out) throws Exception {
    lastX.update(x.getX());
    tryEmit(ctx, out);
  }

  @Override
  protected void processElement2(Y y, Context ctx, Collector<Out> out) throws Exception {
    lastY.update(y.getY());
    tryEmit(ctx, out);
  }

  @Override
  protected void processElement3(Z z, Context ctx, Collector<Out> out) throws Exception {
    lastZ.update(z.getZ());
    tryEmit(ctx, out);
  }

  private void tryEmit(Context ctx, Collector<Out> out) throws Exception {
    Integer a = lastX.value();
    Integer b = lastY.value();
    Integer c = lastZ.value();
    String key = ctx.getCurrentKey(String.class);
    if (a != null && b != null) {
      SavepointReadinessSignal.markKeyReady(key);
    }
    if (a != null && b != null && c != null) {
      out.collect(new Out(key, a + b + c));
      lastX.clear();
      lastY.clear();
      lastZ.clear();
    }
  }
}
