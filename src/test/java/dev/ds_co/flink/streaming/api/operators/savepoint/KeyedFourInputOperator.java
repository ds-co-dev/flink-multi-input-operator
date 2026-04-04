package dev.ds_co.flink.streaming.api.operators.savepoint;

import dev.ds_co.flink.streaming.api.operators.KeyedMultiInputOperator4;
import dev.ds_co.flink.streaming.api.operators.testing.Out;
import dev.ds_co.flink.streaming.api.operators.testing.W;
import dev.ds_co.flink.streaming.api.operators.testing.X;
import dev.ds_co.flink.streaming.api.operators.testing.Y;
import dev.ds_co.flink.streaming.api.operators.testing.Z;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.util.Collector;

public class KeyedFourInputOperator extends KeyedMultiInputOperator4<X, Y, Z, W, Out> {

  private transient ValueState<Integer> lastX;
  private transient ValueState<Integer> lastY;
  private transient ValueState<Integer> lastZ;
  private transient ValueState<Integer> lastW;

  public KeyedFourInputOperator(StreamOperatorParameters<Out> params) {
    super(params);
  }

  @Override
  public void open() throws Exception {
    super.open();
    var store =
        getKeyedStateStore()
            .orElseThrow(
                () -> new IllegalStateException("KeyedFourInputOperator requires keyed state"));
    lastX = store.getState(new ValueStateDescriptor<>("x", Types.INT));
    lastY = store.getState(new ValueStateDescriptor<>("y", Types.INT));
    lastZ = store.getState(new ValueStateDescriptor<>("z", Types.INT));
    lastW = store.getState(new ValueStateDescriptor<>("w", Types.INT));
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

  @Override
  protected void processElement4(W w, Context ctx, Collector<Out> out) throws Exception {
    lastW.update(w.getW());
    tryEmit(ctx, out);
  }

  private void tryEmit(Context ctx, Collector<Out> out) throws Exception {
    Integer a = lastX.value();
    Integer b = lastY.value();
    Integer c = lastZ.value();
    Integer d = lastW.value();
    if (a != null && b != null && c != null && d != null) {
      String key = ctx.getCurrentKey(String.class);
      out.collect(new Out(key, a + b + c + d));
      lastX.clear();
      lastY.clear();
      lastZ.clear();
      lastW.clear();
    }
  }
}
