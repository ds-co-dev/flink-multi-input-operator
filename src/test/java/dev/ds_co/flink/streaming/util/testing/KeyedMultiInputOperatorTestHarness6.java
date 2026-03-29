package dev.ds_co.flink.streaming.util.testing;

import dev.ds_co.flink.streaming.api.operators.KeyedMultiInputOperator6;
import dev.ds_co.flink.streaming.api.operators.MultiInputOperatorFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;

public class KeyedMultiInputOperatorTestHarness6<K, IN1, IN2, IN3, IN4, IN5, IN6, OUT>
    extends AbstractStreamOperatorTestHarness<OUT> {

  public KeyedMultiInputOperatorTestHarness6(
      Class<? extends KeyedMultiInputOperator6<IN1, IN2, IN3, IN4, IN5, IN6, OUT>> operatorClass,
      KeySelector<IN1, K> keySelector1,
      KeySelector<IN2, K> keySelector2,
      KeySelector<IN3, K> keySelector3,
      KeySelector<IN4, K> keySelector4,
      KeySelector<IN5, K> keySelector5,
      KeySelector<IN6, K> keySelector6,
      TypeInformation<K> keyTypeInfo)
      throws Exception {
    this(
        operatorClass,
        keySelector1,
        keySelector2,
        keySelector3,
        keySelector4,
        keySelector5,
        keySelector6,
        keyTypeInfo,
        1,
        1,
        0);
  }

  public KeyedMultiInputOperatorTestHarness6(
      Class<? extends KeyedMultiInputOperator6<IN1, IN2, IN3, IN4, IN5, IN6, OUT>> operatorClass,
      KeySelector<IN1, K> keySelector1,
      KeySelector<IN2, K> keySelector2,
      KeySelector<IN3, K> keySelector3,
      KeySelector<IN4, K> keySelector4,
      KeySelector<IN5, K> keySelector5,
      KeySelector<IN6, K> keySelector6,
      TypeInformation<K> keyTypeInfo,
      int maxParallelism,
      int numSubtasks,
      int subtaskIndex)
      throws Exception {
    super(
        new MultiInputOperatorFactory<>(operatorClass), maxParallelism, numSubtasks, subtaskIndex);

    ClosureCleaner.clean(keySelector1, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector2, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector3, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector4, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector5, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector6, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);

    config.setStatePartitioner(0, keySelector1);
    config.setStatePartitioner(1, keySelector2);
    config.setStatePartitioner(2, keySelector3);
    config.setStatePartitioner(3, keySelector4);
    config.setStatePartitioner(4, keySelector5);
    config.setStatePartitioner(5, keySelector6);
    config.setStateKeySerializer(
        keyTypeInfo.createSerializer(executionConfig.getSerializerConfig()));
    config.serializeAllConfigs();
  }

  private MultipleInputStreamOperator<OUT> getMultiInputOperator() {
    StreamOperator<?> op = operator;
    if (op instanceof MultipleInputStreamOperator) {
      return (MultipleInputStreamOperator<OUT>) op;
    }
    throw new IllegalStateException(
        "Operator is not a MultipleInputStreamOperator: " + op.getClass().getName());
  }

  private void processElement(int inputIndex, StreamRecord<?> record) throws Exception {
    MultipleInputStreamOperator<OUT> op = getMultiInputOperator();
    List<org.apache.flink.streaming.api.operators.Input> inputs = op.getInputs();
    if (inputIndex < 0 || inputIndex >= inputs.size()) {
      throw new IllegalArgumentException(
          "Input index " + inputIndex + " out of range [0, " + inputs.size() + ")");
    }
    inputs.get(inputIndex).processElement(record);
  }

  public void processElement1(StreamRecord<IN1> record) throws Exception {
    processElement(0, record);
  }

  public void processElement2(StreamRecord<IN2> record) throws Exception {
    processElement(1, record);
  }

  public void processElement3(StreamRecord<IN3> record) throws Exception {
    processElement(2, record);
  }

  public void processElement4(StreamRecord<IN4> record) throws Exception {
    processElement(3, record);
  }

  public void processElement5(StreamRecord<IN5> record) throws Exception {
    processElement(4, record);
  }

  public void processElement6(StreamRecord<IN6> record) throws Exception {
    processElement(5, record);
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<OUT> extractOutputValues() {
    List<OUT> result = new ArrayList<>();
    for (Object record : getOutput()) {
      if (record instanceof StreamRecord) {
        result.add((OUT) ((StreamRecord<?>) record).getValue());
      }
    }
    return result;
  }
}
