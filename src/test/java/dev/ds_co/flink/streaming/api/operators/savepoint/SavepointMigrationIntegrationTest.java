package dev.ds_co.flink.streaming.api.operators.savepoint;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.ds_co.flink.streaming.api.operators.KeyedMultiInputOperatorBuilder;
import dev.ds_co.flink.streaming.api.operators.testing.Out;
import dev.ds_co.flink.streaming.api.operators.testing.W;
import dev.ds_co.flink.streaming.api.operators.testing.X;
import dev.ds_co.flink.streaming.api.operators.testing.Y;
import dev.ds_co.flink.streaming.api.operators.testing.Z;
import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.test.streaming.runtime.util.TestListResultSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proves that keyed state saved by an N-input operator can be correctly restored into an
 * (N+1)-input operator — the core guarantee of {@link
 * dev.ds_co.flink.streaming.api.operators.KeyedMultiInputOperator} savepoint compatibility.
 *
 * <p>Each test runs two Flink jobs against the same MiniCluster:
 *
 * <ol>
 *   <li><b>Job 1</b> — uses blocking sources to populate X + Y keyed state, then a savepoint is
 *       taken and the job is cancelled.
 *   <li><b>Job 2</b> — restores from that savepoint with an operator that has more inputs, sends
 *       the new input(s), and asserts the combined output includes the restored state.
 * </ol>
 */
class SavepointMigrationIntegrationTest {

  private static final BooleanSupplier NEVER_EXIT = (BooleanSupplier & Serializable) () -> false;

  @Test
  void testRestoreFrom2InputSavepointWith3InputOperator(@TempDir File tempFolder) throws Exception {
    try (SavepointTestCluster cluster = new SavepointTestCluster(tempFolder)) {
      cluster.start();

      String savepointPath = runJob1_2InputsAccumulateXYStateThenSavepoint(cluster);
      List<Out> result = runJob2_3InputsRestoreSendZCollectOutput(cluster, savepointPath);

      result.sort(Comparator.comparing(Out::getId).thenComparing(Out::getSum));
      assertEquals(2, result.size());
      assertEquals(new Out("a", 60), result.get(0));
      assertEquals(new Out("b", 600), result.get(1));
    }
  }

  @Test
  void testRestoreFrom3InputSavepointWith4InputOperator(@TempDir File tempFolder) throws Exception {
    try (SavepointTestCluster cluster = new SavepointTestCluster(tempFolder)) {
      cluster.start();

      String savepointPath = runJob1_3InputsAccumulateXYStateThenSavepoint(cluster);
      List<Out> result = runJob2_4InputsRestoreSendZAndWCollectOutput(cluster, savepointPath);

      result.sort(Comparator.comparing(Out::getId).thenComparing(Out::getSum));
      assertEquals(2, result.size());
      assertEquals(new Out("a", 100), result.get(0));
      assertEquals(new Out("b", 1000), result.get(1));
    }
  }

  private String runJob1_2InputsAccumulateXYStateThenSavepoint(SavepointTestCluster cluster)
      throws Exception {
    cluster.prepareForJob1();
    StreamExecutionEnvironment env = cluster.env1();
    DataStream<X> xs =
        blockingSource(
            env,
            Arrays.asList(new X("a", 10), new X("b", 100)),
            TypeInformation.of(X.class),
            "x-source");
    DataStream<Y> ys =
        blockingSource(
            env,
            Arrays.asList(new Y("a", 20), new Y("b", 200)),
            TypeInformation.of(Y.class),
            "y-source");

    xs.connect(ys)
        .keyBy(X::getKey, Y::getKey)
        .process(new KeyedTwoInputOperator())
        .name("multi-input-join")
        .uid("multi-input-join")
        .sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>())
        .name("sink")
        .uid("sink");

    JobID jobId1 = cluster.submitJob1().get();
    return cluster.waitForStateAndSavepoint(jobId1, "a", "b");
  }

  private String runJob1_3InputsAccumulateXYStateThenSavepoint(SavepointTestCluster cluster)
      throws Exception {
    cluster.prepareForJob1();
    StreamExecutionEnvironment env = cluster.env1();
    DataStream<X> xs =
        blockingSource(
            env,
            Arrays.asList(new X("a", 10), new X("b", 100)),
            TypeInformation.of(X.class),
            "x-source");
    DataStream<Y> ys =
        blockingSource(
            env,
            Arrays.asList(new Y("a", 20), new Y("b", 200)),
            TypeInformation.of(Y.class),
            "y-source");
    DataStream<Z> zs = emptySource(env, TypeInformation.of(Z.class), "z-source");

    KeyedMultiInputOperatorBuilder<String, Out> builder =
        new KeyedMultiInputOperatorBuilder<>(
            env, KeyedThreeInputOperator.class, TypeInformation.of(Out.class), Types.STRING);
    builder.addInput(xs, X::getKey).addInput(ys, Y::getKey).addInput(zs, Z::getKey);
    builder
        .build("multi-input-join")
        .sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>())
        .name("sink")
        .uid("sink");

    JobID jobId1 = cluster.submitJob1().get();
    return cluster.waitForStateAndSavepoint(jobId1, "a", "b");
  }

  private List<Out> runJob2_3InputsRestoreSendZCollectOutput(
      SavepointTestCluster cluster, String savepointPath) throws Exception {
    TestListResultSink<Out> resultSink = new TestListResultSink<>();
    StreamExecutionEnvironment env = cluster.env2();
    DataStream<X> xs = emptySource(env, TypeInformation.of(X.class), "x-source-2");
    DataStream<Y> ys = emptySource(env, TypeInformation.of(Y.class), "y-source-2");
    DataStream<Z> zs =
        env.fromData(new Z("a", 30), new Z("b", 300)).name("z-source-2").uid("z-source-2");

    KeyedMultiInputOperatorBuilder<String, Out> builder =
        new KeyedMultiInputOperatorBuilder<>(
            env, KeyedThreeInputOperator.class, TypeInformation.of(Out.class), Types.STRING);
    builder.addInput(xs, X::getKey).addInput(ys, Y::getKey).addInput(zs, Z::getKey);
    builder.build("multi-input-join").addSink(resultSink).name("sink-2").uid("sink-2");

    cluster.submitJob2AndWait(savepointPath);
    return resultSink.getResult();
  }

  private List<Out> runJob2_4InputsRestoreSendZAndWCollectOutput(
      SavepointTestCluster cluster, String savepointPath) throws Exception {
    TestListResultSink<Out> resultSink = new TestListResultSink<>();
    StreamExecutionEnvironment env = cluster.env2();
    DataStream<X> xs = emptySource(env, TypeInformation.of(X.class), "x-source-2");
    DataStream<Y> ys = emptySource(env, TypeInformation.of(Y.class), "y-source-2");
    DataStream<Z> zs =
        env.fromData(new Z("a", 30), new Z("b", 300)).name("z-source-2").uid("z-source-2");
    DataStream<W> ws =
        env.fromData(new W("a", 40), new W("b", 400)).name("w-source-2").uid("w-source-2");

    KeyedMultiInputOperatorBuilder<String, Out> builder =
        new KeyedMultiInputOperatorBuilder<>(
            env, KeyedFourInputOperator.class, TypeInformation.of(Out.class), Types.STRING);
    builder
        .addInput(xs, X::getKey)
        .addInput(ys, Y::getKey)
        .addInput(zs, Z::getKey)
        .addInput(ws, W::getKey);
    builder.build("multi-input-join").addSink(resultSink).name("sink-2").uid("sink-2");

    cluster.submitJob2AndWait(savepointPath);
    return resultSink.getResult();
  }

  private static <T extends Serializable> DataStream<T> blockingSource(
      StreamExecutionEnvironment env, List<T> items, TypeInformation<T> typeInfo, String name) {
    return env.addSource(new FiniteTestSource<>(NEVER_EXIT, items), typeInfo).name(name).uid(name);
  }

  private static <T> DataStream<T> emptySource(
      StreamExecutionEnvironment env, TypeInformation<T> typeInfo, String name) {
    return env.fromCollection(Collections.emptyList(), typeInfo).name(name).uid(name);
  }
}
