package dev.ds_co.flink.streaming.api.operators;

import java.util.List;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

public abstract class KeyedMultiInputOperator7<IN1, IN2, IN3, IN4, IN5, IN6, IN7, OUT>
    extends BaseMultiInputKeyedOperator<OUT> {

  private static final long serialVersionUID = 1L;

  protected KeyedMultiInputOperator7(StreamOperatorParameters<OUT> params) {
    super(params, 7);
    addInputHandler(in1);
    addInputHandler(in2);
    addInputHandler(in3);
    addInputHandler(in4);
    addInputHandler(in5);
    addInputHandler(in6);
    addInputHandler(in7);
  }

  protected abstract void processElement1(IN1 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement2(IN2 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement3(IN3 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement4(IN4 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement5(IN5 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement6(IN6 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement7(IN7 value, Context ctx, Collector<OUT> out)
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

  private final AbstractInput<IN4, OUT> in4 =
      new AbstractInput<>(this, 4) {
        @Override
        public void processElement(StreamRecord<IN4> record) throws Exception {
          setKeyContextElement(record);
          processElement4(record.getValue(), ctx(record), out);
        }
      };

  private final AbstractInput<IN5, OUT> in5 =
      new AbstractInput<>(this, 5) {
        @Override
        public void processElement(StreamRecord<IN5> record) throws Exception {
          setKeyContextElement(record);
          processElement5(record.getValue(), ctx(record), out);
        }
      };

  private final AbstractInput<IN6, OUT> in6 =
      new AbstractInput<>(this, 6) {
        @Override
        public void processElement(StreamRecord<IN6> record) throws Exception {
          setKeyContextElement(record);
          processElement6(record.getValue(), ctx(record), out);
        }
      };

  private final AbstractInput<IN7, OUT> in7 =
      new AbstractInput<>(this, 7) {
        @Override
        public void processElement(StreamRecord<IN7> record) throws Exception {
          setKeyContextElement(record);
          processElement7(record.getValue(), ctx(record), out);
        }
      };

  @Override
  @SuppressWarnings("rawtypes")
  public List<Input> getInputs() {
    return super.getInputs();
  }
}
