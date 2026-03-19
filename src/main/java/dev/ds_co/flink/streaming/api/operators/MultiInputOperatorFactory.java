package dev.ds_co.flink.streaming.api.operators;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

class MultiInputOperatorFactory<OUT> extends AbstractStreamOperatorFactory<OUT> {

  private final Class<? extends StreamOperator<OUT>> operatorClass;

  public MultiInputOperatorFactory(Class<? extends StreamOperator<OUT>> operatorClass) {
    this.operatorClass = operatorClass;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends StreamOperator<OUT>> T createStreamOperator(
      StreamOperatorParameters<OUT> params) {
    try {
      return (T) operatorClass.getConstructor(StreamOperatorParameters.class).newInstance(params);
    } catch (Exception e) {
      throw new RuntimeException("Could not instantiate operator " + operatorClass.getName(), e);
    }
  }

  @Override
  public Class<? extends StreamOperator<?>> getStreamOperatorClass(ClassLoader cl) {
    return operatorClass;
  }
}
