package dev.ds_co.flink.streaming.api.operators;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.streaming.runtime.util.TestListResultSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MultiInputITCase {

  StreamExecutionEnvironment env;
  DataStream<X> xs;
  DataStream<Y> ys;
  DataStream<Z> zs;

  @BeforeEach
  void setup() {
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    xs = env.fromData(new X("a", 10), new X("b", 100));
    ys = env.fromData(new Y("a", 20), new Y("b", 200));
    zs = env.fromData(new Z("a", 30), new Z("b", 300));
  }

  @Test
  void testKeyedThreeWayJoin() throws Exception {
    KeyedMultiInputOperatorBuilder<String, Out> builder =
        new KeyedMultiInputOperatorBuilder<>(
            env, KeyedThreeInputOperator.class, TypeInformation.of(Out.class), Types.STRING);

    builder.addInput(xs, X::getKey).addInput(ys, Y::getKey).addInput(zs, Z::getKey);

    SingleOutputStreamOperator<Out> joined = builder.build("xyz-join");

    DataStream<String> side = joined.getSideOutput(KeyedThreeInputOperator.JOINED_KEYS);

    TestListResultSink<Out> resultSink = new TestListResultSink<>();
    TestListResultSink<String> sideSink = new TestListResultSink<>();
    joined.addSink(resultSink);
    side.addSink(sideSink);

    env.execute("Keyed Three-Way Join Test");

    List<Out> result = resultSink.getResult();
    result.sort(Comparator.comparing(Out::getId).thenComparing(Out::getSum));

    List<String> joinedKeys = sideSink.getResult();
    joinedKeys.sort(String::compareTo);

    assertEquals(2, result.size());
    assertEquals(new Out("a", 60), result.get(0));
    assertEquals(new Out("b", 600), result.get(1));

    assertEquals(2, joinedKeys.size());
    assertEquals("a", joinedKeys.get(0));
    assertEquals("b", joinedKeys.get(1));
  }

  @Test
  void testBroadcastInput() throws Exception {
    KeyedMultiInputOperatorBuilder<String, Out> builder =
        new KeyedMultiInputOperatorBuilder<>(
            env, KeyedNInputOperator.class, TypeInformation.of(Out.class), Types.STRING);

    DataStream<String> config = env.fromData("ignored-1", "ignored-2");

    builder
        .addInput(xs, X::getKey)
        .addInput(ys, Y::getKey)
        .addInput(zs, Z::getKey)
        .addBroadcastInput(config);

    DataStream<Out> joined = builder.build("broadcast-test");

    TestListResultSink<Out> resultSink = new TestListResultSink<>();
    joined.addSink(resultSink);

    env.execute("Broadcast Input Test");

    List<Out> result = resultSink.getResult();
    result.sort(Comparator.comparing(Out::getId).thenComparing(Out::getSum));

    assertEquals(2, result.size());
    assertEquals(new Out("a", 60), result.get(0));
    assertEquals(new Out("b", 600), result.get(1));
  }

  @Test
  void testKeyedNWayJoin() throws Exception {
    KeyedMultiInputOperatorBuilder<String, Out> builder =
        new KeyedMultiInputOperatorBuilder<>(
            env, KeyedNInputOperator.class, TypeInformation.of(Out.class), Types.STRING);

    builder.addInput(xs, X::getKey).addInput(ys, Y::getKey).addInput(zs, Z::getKey);

    DataStream<Out> joined = builder.build("xyz-join");

    TestListResultSink<Out> resultSink = new TestListResultSink<>();
    joined.addSink(resultSink);

    env.execute("Keyed N-Way Join Test");

    List<Out> result = resultSink.getResult();
    result.sort(Comparator.comparing(Out::getId).thenComparing(Out::getSum));

    assertEquals(2, result.size());
    assertEquals(new Out("a", 60), result.get(0));
    assertEquals(new Out("b", 600), result.get(1));
  }
}
