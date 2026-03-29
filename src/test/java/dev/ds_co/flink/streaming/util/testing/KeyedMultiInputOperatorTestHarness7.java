package dev.ds_co.flink.streaming.util.testing;

import dev.ds_co.flink.streaming.api.operators.KeyedMultiInputOperator7;
import dev.ds_co.flink.streaming.api.operators.MultiInputOperatorFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MultiInputStreamOperatorTestHarness;

public class KeyedMultiInputOperatorTestHarness7<K, IN1, IN2, IN3, IN4, IN5, IN6, IN7, OUT>
    extends MultiInputStreamOperatorTestHarness<OUT> {

  public KeyedMultiInputOperatorTestHarness7(
      Class<? extends KeyedMultiInputOperator7<IN1, IN2, IN3, IN4, IN5, IN6, IN7, OUT>>
          operatorClass,
      KeySelector<IN1, K> keySelector1,
      KeySelector<IN2, K> keySelector2,
      KeySelector<IN3, K> keySelector3,
      KeySelector<IN4, K> keySelector4,
      KeySelector<IN5, K> keySelector5,
      KeySelector<IN6, K> keySelector6,
      KeySelector<IN7, K> keySelector7,
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
        keyTypeInfo,
        1,
        1,
        0);
  }

  public KeyedMultiInputOperatorTestHarness7(
      Class<? extends KeyedMultiInputOperator7<IN1, IN2, IN3, IN4, IN5, IN6, IN7, OUT>>
          operatorClass,
      KeySelector<IN1, K> keySelector1,
      KeySelector<IN2, K> keySelector2,
      KeySelector<IN3, K> keySelector3,
      KeySelector<IN4, K> keySelector4,
      KeySelector<IN5, K> keySelector5,
      KeySelector<IN6, K> keySelector6,
      KeySelector<IN7, K> keySelector7,
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

    config.setStatePartitioner(0, keySelector1);
    config.setStatePartitioner(1, keySelector2);
    config.setStatePartitioner(2, keySelector3);
    config.setStatePartitioner(3, keySelector4);
    config.setStatePartitioner(4, keySelector5);
    config.setStatePartitioner(5, keySelector6);
    config.setStatePartitioner(6, keySelector7);
    config.setStateKeySerializer(
        keyTypeInfo.createSerializer(executionConfig.getSerializerConfig()));
    config.serializeAllConfigs();
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
