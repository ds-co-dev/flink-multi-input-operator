package dev.ds_co.flink.streaming.api.operators.testing;

import dev.ds_co.flink.streaming.api.operators.KeyedMultiInputOperatorN;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.util.Collector;

/**
 * Example of three-way keyed join implemented by extending the generic (N) class.
 *
 * <p>X(id, x), Y(id, y), Z(id, z) -> Out(id, x + y + z) Uses the KeyedMultiInputOperatorN base to
 * get a unique handler: processElement(inputIndex, Object value, Context ctx, Collector<Out> out)
 */
public class KeyedNInputOperator extends KeyedMultiInputOperatorN<Out> {

  private transient ValueState<Integer> lastX;
  private transient ValueState<Integer> lastY;
  private transient ValueState<Integer> lastZ;

  public KeyedNInputOperator(StreamOperatorParameters<Out> params) {
    super(params, params.getStreamConfig().getNumberOfNetworkInputs());
  }

  @Override
  public void open() throws Exception {
    super.open();

    var store =
        getKeyedStateStore()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "KeyedMultiJoinOperatorGeneric requires keyed state"));

    lastX = store.getState(new ValueStateDescriptor<>("x", Types.INT));
    lastY = store.getState(new ValueStateDescriptor<>("y", Types.INT));
    lastZ = store.getState(new ValueStateDescriptor<>("z", Types.INT));
  }

  //
  // One method for all the inputs
  //

  @Override
  protected void processElement(int inputIndex, Object value, Context ctx, Collector<Out> out)
      throws Exception {
    switch (inputIndex) {
      case 1:
        processElement1((X) value, ctx, out);
        break;
      case 2:
        processElement2((Y) value, ctx, out);
        break;
      case 3:
        processElement3((Z) value, ctx, out);
        break;
      default:
        break;
    }
  }

  private void processElement1(X x, Context ctx, Collector<Out> out) throws Exception {
    lastX.update(x.getX());
    join(ctx, out);
  }

  private void processElement2(Y y, Context ctx, Collector<Out> out) throws Exception {
    lastY.update(y.getY());
    join(ctx, out);
  }

  private void processElement3(Z z, Context ctx, Collector<Out> out) throws Exception {
    lastZ.update(z.getZ());
    join(ctx, out);
  }

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
