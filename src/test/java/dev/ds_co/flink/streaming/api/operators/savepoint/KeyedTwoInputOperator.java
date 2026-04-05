package dev.ds_co.flink.streaming.api.operators.savepoint;

import dev.ds_co.flink.streaming.api.operators.testing.Out;
import dev.ds_co.flink.streaming.api.operators.testing.X;
import dev.ds_co.flink.streaming.api.operators.testing.Y;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedTwoInputOperator extends KeyedCoProcessFunction<String, X, Y, Out> {

  private transient ValueState<Integer> lastX;
  private transient ValueState<Integer> lastY;

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    lastX = getRuntimeContext().getState(new ValueStateDescriptor<>("x", Types.INT));
    lastY = getRuntimeContext().getState(new ValueStateDescriptor<>("y", Types.INT));
  }

  @Override
  public void processElement1(X x, Context ctx, Collector<Out> out) throws Exception {
    lastX.update(x.getX());
    markKeyIfReady(ctx);
  }

  @Override
  public void processElement2(Y y, Context ctx, Collector<Out> out) throws Exception {
    lastY.update(y.getY());
    markKeyIfReady(ctx);
  }

  private void markKeyIfReady(Context ctx) throws Exception {
    Integer a = lastX.value();
    Integer b = lastY.value();
    if (a != null && b != null) {
      SavepointReadinessSignal.markKeyReady(ctx.getCurrentKey());
    }
  }
}
