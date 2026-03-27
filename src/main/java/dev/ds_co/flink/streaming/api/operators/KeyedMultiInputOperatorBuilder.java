package dev.ds_co.flink.streaming.api.operators;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.MultipleConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;

public class KeyedMultiInputOperatorBuilder<K, OUT> {

  private final StreamExecutionEnvironment env;
  private final StreamOperatorFactory<OUT> factory;
  private final TypeInformation<OUT> outType;
  private final TypeInformation<K> keyType;

  private final List<DataStream<?>> streams = new ArrayList<>();
  private final List<KeySelector<?, K>> keySelectors = new ArrayList<>();

  public KeyedMultiInputOperatorBuilder(
      StreamExecutionEnvironment env,
      Class<? extends StreamOperator<OUT>> operatorClass,
      TypeInformation<OUT> outType,
      TypeInformation<K> keyType) {
    this.env = env;
    this.factory = new MultiInputOperatorFactory<>(operatorClass);
    this.outType = outType;
    this.keyType = keyType;
  }

  public <T> KeyedMultiInputOperatorBuilder<K, OUT> addInput(
      DataStream<T> stream, KeySelector<T, K> selector) {
    if (stream instanceof KeyedStream) {
      throw new IllegalArgumentException(
          "addInput(DataStream, KeySelector) expects an unkeyed DataStream, but you passed a KeyedStream.");
    }

    streams.add(stream.keyBy(selector));
    keySelectors.add(selector);
    return this;
  }

  public <T> KeyedMultiInputOperatorBuilder<K, OUT> addInput(KeyedStream<T, K> keyedStream) {
    streams.add(keyedStream);
    keySelectors.add(keyedStream.getKeySelector());
    return this;
  }

  public SingleOutputStreamOperator<OUT> build(String uid) {
    if (streams.isEmpty()) {
      throw new IllegalStateException("At least one input must be added before calling build()");
    }

    final int parallelism = env.getParallelism();

    KeyedMultipleInputTransformation<OUT> t =
        new KeyedMultipleInputTransformation<>(uid, factory, outType, parallelism, keyType);

    for (int i = 0; i < streams.size(); i++) {
      DataStream<?> stream = streams.get(i);
      KeySelector<?, K> keySel = keySelectors.get(i);
      t.addInput(stream.getTransformation(), keySel);
    }

    t.setUid(uid);

    MultipleConnectedStreams multi = new MultipleConnectedStreams(env);
    SingleOutputStreamOperator<OUT> op = multi.transform(t);

    return op;
  }
}
