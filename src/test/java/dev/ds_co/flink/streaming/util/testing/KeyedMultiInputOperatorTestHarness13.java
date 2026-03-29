package dev.ds_co.flink.streaming.util.testing;

import dev.ds_co.flink.streaming.api.operators.KeyedMultiInputOperator13;
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

public class KeyedMultiInputOperatorTestHarness13<
        K, IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8, IN9, IN10, IN11, IN12, IN13, OUT>
    extends AbstractStreamOperatorTestHarness<OUT> {

  public KeyedMultiInputOperatorTestHarness13(
      Class<
              ? extends
                  KeyedMultiInputOperator13<
                      IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8, IN9, IN10, IN11, IN12, IN13, OUT>>
          operatorClass,
      KeySelector<IN1, K> keySelector1,
      KeySelector<IN2, K> keySelector2,
      KeySelector<IN3, K> keySelector3,
      KeySelector<IN4, K> keySelector4,
      KeySelector<IN5, K> keySelector5,
      KeySelector<IN6, K> keySelector6,
      KeySelector<IN7, K> keySelector7,
      KeySelector<IN8, K> keySelector8,
      KeySelector<IN9, K> keySelector9,
      KeySelector<IN10, K> keySelector10,
      KeySelector<IN11, K> keySelector11,
      KeySelector<IN12, K> keySelector12,
      KeySelector<IN13, K> keySelector13,
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
        keySelector7,
        keySelector8,
        keySelector9,
        keySelector10,
        keySelector11,
        keySelector12,
        keySelector13,
        keyTypeInfo,
        1,
        1,
        0);
  }

  public KeyedMultiInputOperatorTestHarness13(
      Class<
              ? extends
                  KeyedMultiInputOperator13<
                      IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8, IN9, IN10, IN11, IN12, IN13, OUT>>
          operatorClass,
      KeySelector<IN1, K> keySelector1,
      KeySelector<IN2, K> keySelector2,
      KeySelector<IN3, K> keySelector3,
      KeySelector<IN4, K> keySelector4,
      KeySelector<IN5, K> keySelector5,
      KeySelector<IN6, K> keySelector6,
      KeySelector<IN7, K> keySelector7,
      KeySelector<IN8, K> keySelector8,
      KeySelector<IN9, K> keySelector9,
      KeySelector<IN10, K> keySelector10,
      KeySelector<IN11, K> keySelector11,
      KeySelector<IN12, K> keySelector12,
      KeySelector<IN13, K> keySelector13,
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
    ClosureCleaner.clean(keySelector7, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector8, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector9, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector10, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector11, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector12, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector13, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);

    config.setStatePartitioner(0, keySelector1);
    config.setStatePartitioner(1, keySelector2);
    config.setStatePartitioner(2, keySelector3);
    config.setStatePartitioner(3, keySelector4);
    config.setStatePartitioner(4, keySelector5);
    config.setStatePartitioner(5, keySelector6);
    config.setStatePartitioner(6, keySelector7);
    config.setStatePartitioner(7, keySelector8);
    config.setStatePartitioner(8, keySelector9);
    config.setStatePartitioner(9, keySelector10);
    config.setStatePartitioner(10, keySelector11);
    config.setStatePartitioner(11, keySelector12);
    config.setStatePartitioner(12, keySelector13);
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

  public void processElement7(StreamRecord<IN7> record) throws Exception {
    processElement(6, record);
  }

  public void processElement8(StreamRecord<IN8> record) throws Exception {
    processElement(7, record);
  }

  public void processElement9(StreamRecord<IN9> record) throws Exception {
    processElement(8, record);
  }

  public void processElement10(StreamRecord<IN10> record) throws Exception {
    processElement(9, record);
  }

  public void processElement11(StreamRecord<IN11> record) throws Exception {
    processElement(10, record);
  }

  public void processElement12(StreamRecord<IN12> record) throws Exception {
    processElement(11, record);
  }

  public void processElement13(StreamRecord<IN13> record) throws Exception {
    processElement(12, record);
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
