package dev.ds_co.flink.streaming.api.operators.testing;

import dev.ds_co.flink.streaming.api.operators.KeyedMultiInputOperator3;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.util.Collector;

/**
 * Example of three-way keyed join:
 *
 * <p>X(id, x), Y(id, y), Z(id, z) -> Out(id, x + y + z)
 *
 * <p>Uses the KeyedMultiInputOperator3 base to get CoProcess-style handlers: processElement1(X
 * value, Context ctx, Collector<Out> out) processElement2(Y value, Context ctx, Collector<Out> out)
 * processElement3(Z value, Context ctx, Collector<Out> out)
 */
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
            .orElseThrow(() -> new IllegalStateException("MultiJoinOperator requires keyed state"));

    lastX = store.getState(new ValueStateDescriptor<>("x", Types.INT));
    lastY = store.getState(new ValueStateDescriptor<>("y", Types.INT));
    lastZ = store.getState(new ValueStateDescriptor<>("z", Types.INT));
  }

  //
  // One method per input (polymorphic dispatch)
  //

  @Override
  protected void processElement1(X x, Context ctx, Collector<Out> out) throws Exception {
    lastX.update(x.getX());
    join(ctx, out);
  }

  @Override
  protected void processElement2(Y y, Context ctx, Collector<Out> out) throws Exception {
    lastY.update(y.getY());
    join(ctx, out);
  }

  @Override
  protected void processElement3(Z z, Context ctx, Collector<Out> out) throws Exception {
    lastZ.update(z.getZ());
    join(ctx, out);
  }

  //
  // Join logic (take the sum of the 3 current values for each key)
  //

  private void join(Context ctx, Collector<Out> out) throws Exception {
    Integer a = lastX.value();
    Integer b = lastY.value();
    Integer c = lastZ.value();

    if (a != null && b != null && c != null) {
      String key = ctx.getCurrentKey(String.class);
      out.collect(new Out(key, a + b + c));
    }
  }
}
