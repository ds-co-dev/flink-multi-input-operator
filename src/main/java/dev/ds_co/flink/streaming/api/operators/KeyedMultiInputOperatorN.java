package dev.ds_co.flink.streaming.api.operators;

import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

/**
 * Generic keyed multi-input operator for N inputs (N >= 1).
 *
 * <p>Subclasses only implement:
 *
 * <p>protected abstract void processElement( int inputIndex, Object value, Context ctx, Collector
 * out) throws Exception;
 *
 * <p>The base class will: - create one AbstractInput per input index [1...N] - wire key context +
 * Context + Collector
 */
public abstract class KeyedMultiInputOperatorN<OUT> extends BaseMultiInputKeyedOperator<OUT> {

  private final int numberOfInputs;

  protected KeyedMultiInputOperatorN(StreamOperatorParameters<OUT> params, int numberOfInputs) {
    super(params, numberOfInputs);

    this.numberOfInputs = numberOfInputs;
    for (int i = 1; i <= numberOfInputs; i++) {
      addInputHandler(createInput(i));
    }
  }

  private <IN> AbstractInput<IN, OUT> createInput(int inputIndex) {
    return new AbstractInput<IN, OUT>(this, inputIndex) {
      @Override
      public void processElement(StreamRecord<IN> record) throws Exception {
        setKeyContextElement(record);
        KeyedMultiInputOperatorN.this.processElement(
            inputIndex, record.getValue(), ctx(record), out);
      }
    };
  }

  /**
   * This method is called for each input event.
   *
   * @param inputIndex 1...N input index, that is, to which input stream the value belongs to
   * @param value the element from that input (subclass casts as needed)
   * @param ctx CoProcess-like context
   * @param out collector (TimestampedCollector under the hood)
   */
  protected abstract void processElement(
      int inputIndex, Object value, Context ctx, Collector<OUT> out) throws Exception;
}
