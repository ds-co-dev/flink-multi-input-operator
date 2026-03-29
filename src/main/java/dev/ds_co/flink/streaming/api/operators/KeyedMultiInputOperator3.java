package dev.ds_co.flink.streaming.api.operators;

import java.util.List;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

public abstract class KeyedMultiInputOperator3<IN1, IN2, IN3, OUT>
    extends BaseMultiInputKeyedOperator<OUT> {

  private static final long serialVersionUID = 1L;

  protected KeyedMultiInputOperator3(StreamOperatorParameters<OUT> params) {
    super(params, 3);
    addInputHandler(in1);
    addInputHandler(in2);
    addInputHandler(in3);
  }

  protected abstract void processElement1(IN1 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement2(IN2 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement3(IN3 value, Context ctx, Collector<OUT> out)
      throws Exception;

  private final AbstractInput<IN1, OUT> in1 =
      new AbstractInput<>(this, 1) {
        @Override
        public void processElement(StreamRecord<IN1> record) throws Exception {
          setKeyContextElement(record);
          processElement1(record.getValue(), ctx(record), out);
        }
      };

  private final AbstractInput<IN2, OUT> in2 =
      new AbstractInput<>(this, 2) {
        @Override
        public void processElement(StreamRecord<IN2> record) throws Exception {
          setKeyContextElement(record);
          processElement2(record.getValue(), ctx(record), out);
        }
      };

  private final AbstractInput<IN3, OUT> in3 =
      new AbstractInput<>(this, 3) {
        @Override
        public void processElement(StreamRecord<IN3> record) throws Exception {
          setKeyContextElement(record);
          processElement3(record.getValue(), ctx(record), out);
        }
      };

  @Override
  @SuppressWarnings("rawtypes")
  public List<Input> getInputs() {
    return super.getInputs();
  }
}
