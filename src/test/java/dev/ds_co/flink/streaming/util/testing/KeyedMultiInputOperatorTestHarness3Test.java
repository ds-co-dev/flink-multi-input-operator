package dev.ds_co.flink.streaming.util.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.ds_co.flink.streaming.api.operators.testing.KeyedThreeInputOperator;
import dev.ds_co.flink.streaming.api.operators.testing.Out;
import dev.ds_co.flink.streaming.api.operators.testing.X;
import dev.ds_co.flink.streaming.api.operators.testing.Y;
import dev.ds_co.flink.streaming.api.operators.testing.Z;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class KeyedMultiInputOperatorTestHarness3Test {

  private final KeyedMultiInputOperatorTestHarness3<String, X, Y, Z, Out> harness;

  KeyedMultiInputOperatorTestHarness3Test() throws Exception {
    harness =
        new KeyedMultiInputOperatorTestHarness3<>(
            KeyedThreeInputOperator.class, X::getKey, Y::getKey, Z::getKey, Types.STRING);
    harness.setup();
    harness.open();
  }

  @AfterEach
  void tearDown() throws Exception {
    harness.close();
  }

  @Test
  void harnessJoinsThreeKeyedInputs() throws Exception {
    harness.processElement1(new StreamRecord<>(new X("a", 10)));
    harness.processElement2(new StreamRecord<>(new Y("a", 20)));
    harness.processElement3(new StreamRecord<>(new Z("a", 30)));
    List<Out> result = harness.extractOutputValues();
    assertEquals(1, result.size());
    assertEquals(new Out("a", 60), result.get(0));
  }
}
