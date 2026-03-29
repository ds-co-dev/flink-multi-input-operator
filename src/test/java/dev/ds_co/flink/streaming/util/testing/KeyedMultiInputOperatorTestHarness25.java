package dev.ds_co.flink.streaming.util.testing;

import dev.ds_co.flink.streaming.api.operators.KeyedMultiInputOperator25;
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

public class KeyedMultiInputOperatorTestHarness25<
        K,
        IN1,
        IN2,
        IN3,
        IN4,
        IN5,
        IN6,
        IN7,
        IN8,
        IN9,
        IN10,
        IN11,
        IN12,
        IN13,
        IN14,
        IN15,
        IN16,
        IN17,
        IN18,
        IN19,
        IN20,
        IN21,
        IN22,
        IN23,
        IN24,
        IN25,
        OUT>
    extends AbstractStreamOperatorTestHarness<OUT> {

  public KeyedMultiInputOperatorTestHarness25(
      Class<
              ? extends
                  KeyedMultiInputOperator25<
                      IN1,
                      IN2,
                      IN3,
                      IN4,
                      IN5,
                      IN6,
                      IN7,
                      IN8,
                      IN9,
                      IN10,
                      IN11,
                      IN12,
                      IN13,
                      IN14,
                      IN15,
                      IN16,
                      IN17,
                      IN18,
                      IN19,
                      IN20,
                      IN21,
                      IN22,
                      IN23,
                      IN24,
                      IN25,
                      OUT>>
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
      KeySelector<IN14, K> keySelector14,
      KeySelector<IN15, K> keySelector15,
      KeySelector<IN16, K> keySelector16,
      KeySelector<IN17, K> keySelector17,
      KeySelector<IN18, K> keySelector18,
      KeySelector<IN19, K> keySelector19,
      KeySelector<IN20, K> keySelector20,
      KeySelector<IN21, K> keySelector21,
      KeySelector<IN22, K> keySelector22,
      KeySelector<IN23, K> keySelector23,
      KeySelector<IN24, K> keySelector24,
      KeySelector<IN25, K> keySelector25,
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
        keySelector14,
        keySelector15,
        keySelector16,
        keySelector17,
        keySelector18,
        keySelector19,
        keySelector20,
        keySelector21,
        keySelector22,
        keySelector23,
        keySelector24,
        keySelector25,
        keyTypeInfo,
        1,
        1,
        0);
  }

  public KeyedMultiInputOperatorTestHarness25(
      Class<
              ? extends
                  KeyedMultiInputOperator25<
                      IN1,
                      IN2,
                      IN3,
                      IN4,
                      IN5,
                      IN6,
                      IN7,
                      IN8,
                      IN9,
                      IN10,
                      IN11,
                      IN12,
                      IN13,
                      IN14,
                      IN15,
                      IN16,
                      IN17,
                      IN18,
                      IN19,
                      IN20,
                      IN21,
                      IN22,
                      IN23,
                      IN24,
                      IN25,
                      OUT>>
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
      KeySelector<IN14, K> keySelector14,
      KeySelector<IN15, K> keySelector15,
      KeySelector<IN16, K> keySelector16,
      KeySelector<IN17, K> keySelector17,
      KeySelector<IN18, K> keySelector18,
      KeySelector<IN19, K> keySelector19,
      KeySelector<IN20, K> keySelector20,
      KeySelector<IN21, K> keySelector21,
      KeySelector<IN22, K> keySelector22,
      KeySelector<IN23, K> keySelector23,
      KeySelector<IN24, K> keySelector24,
      KeySelector<IN25, K> keySelector25,
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
    ClosureCleaner.clean(keySelector14, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector15, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector16, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector17, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector18, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector19, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector20, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector21, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector22, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector23, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector24, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);
    ClosureCleaner.clean(keySelector25, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, false);

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
    config.setStatePartitioner(13, keySelector14);
    config.setStatePartitioner(14, keySelector15);
    config.setStatePartitioner(15, keySelector16);
    config.setStatePartitioner(16, keySelector17);
    config.setStatePartitioner(17, keySelector18);
    config.setStatePartitioner(18, keySelector19);
    config.setStatePartitioner(19, keySelector20);
    config.setStatePartitioner(20, keySelector21);
    config.setStatePartitioner(21, keySelector22);
    config.setStatePartitioner(22, keySelector23);
    config.setStatePartitioner(23, keySelector24);
    config.setStatePartitioner(24, keySelector25);
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

  public void processElement14(StreamRecord<IN14> record) throws Exception {
    processElement(13, record);
  }

  public void processElement15(StreamRecord<IN15> record) throws Exception {
    processElement(14, record);
  }

  public void processElement16(StreamRecord<IN16> record) throws Exception {
    processElement(15, record);
  }

  public void processElement17(StreamRecord<IN17> record) throws Exception {
    processElement(16, record);
  }

  public void processElement18(StreamRecord<IN18> record) throws Exception {
    processElement(17, record);
  }

  public void processElement19(StreamRecord<IN19> record) throws Exception {
    processElement(18, record);
  }

  public void processElement20(StreamRecord<IN20> record) throws Exception {
    processElement(19, record);
  }

  public void processElement21(StreamRecord<IN21> record) throws Exception {
    processElement(20, record);
  }

  public void processElement22(StreamRecord<IN22> record) throws Exception {
    processElement(21, record);
  }

  public void processElement23(StreamRecord<IN23> record) throws Exception {
    processElement(22, record);
  }

  public void processElement24(StreamRecord<IN24> record) throws Exception {
    processElement(23, record);
  }

  public void processElement25(StreamRecord<IN25> record) throws Exception {
    processElement(24, record);
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
