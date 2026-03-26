package dev.ds_co.flink.streaming.api.operators;

import static org.junit.Assert.assertEquals;

import dev.ds_co.flink.streaming.api.operators.testing.KeyedNInputOperator;
import dev.ds_co.flink.streaming.api.operators.testing.KeyedThreeInputOperator;
import dev.ds_co.flink.streaming.api.operators.testing.Out;
import dev.ds_co.flink.streaming.api.operators.testing.X;
import dev.ds_co.flink.streaming.api.operators.testing.Y;
import dev.ds_co.flink.streaming.api.operators.testing.Z;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.streaming.runtime.util.TestListResultSink;
import org.junit.Before;
import org.junit.Test;

public class MultiInputITCase {

  StreamExecutionEnvironment env;
  DataStream<X> xs;
  DataStream<Y> ys;
  DataStream<Z> zs;

  @Before
  public void setup() {
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    xs = env.fromData(new X("a", 10), new X("b", 100));
    ys = env.fromData(new Y("a", 20), new Y("b", 200));
    zs = env.fromData(new Z("a", 30), new Z("b", 300));
  }

  @Test
  public void testKeyedThreeWayJoin() throws Exception {
    KeyedMultiInputOperatorBuilder<String, Out> builder =
        new KeyedMultiInputOperatorBuilder<>(
            env, KeyedThreeInputOperator.class, TypeInformation.of(Out.class), Types.STRING);

    builder.addInput(xs, X::getKey).addInput(ys, Y::getKey).addInput(zs, Z::getKey);

    DataStream<Out> joined = builder.build("xyz-join");

    TestListResultSink<Out> resultSink = new TestListResultSink<>();
    joined.addSink(resultSink);

    env.execute("Keyed Three-Way Join Test");

    List<Out> result = resultSink.getResult();
    result.sort(Comparator.comparing(Out::getId).thenComparing(Out::getSum));

    assertEquals(2, result.size());
    assertEquals(new Out("a", 60), result.get(0));
    assertEquals(new Out("b", 600), result.get(1));
  }

  @Test
  public void testKeyedNWayJoin() throws Exception {
    KeyedMultiInputOperatorBuilder<String, Out> builder =
        new KeyedMultiInputOperatorBuilder<>(
            env, KeyedNInputOperator.class, TypeInformation.of(Out.class), Types.STRING);

    builder.addInput(xs, X::getKey).addInput(ys, Y::getKey).addInput(zs, Z::getKey);

    DataStream<Out> joined = builder.build("xyz-join");

    TestListResultSink<Out> resultSink = new TestListResultSink<>();
    joined.addSink(resultSink);

    env.execute("Keyed Three-Way Join Test");

    List<Out> result = resultSink.getResult();
    result.sort(Comparator.comparing(Out::getId).thenComparing(Out::getSum));

    assertEquals(2, result.size());
    assertEquals(new Out("a", 60), result.get(0));
    assertEquals(new Out("b", 600), result.get(1));
  }
}
