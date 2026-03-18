package dev.ds_co.flink;

import dev.ds_co.flink.streaming.api.operators.DummyMultiInputStub;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DummyCompilationTest {
  @Test
  void dummyClassLoads() {
    assertNotNull(DummyMultiInputStub.class);
  }
}
