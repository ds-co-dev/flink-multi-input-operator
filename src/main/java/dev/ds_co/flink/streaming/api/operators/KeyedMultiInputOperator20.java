package dev.ds_co.flink.streaming.api.operators;

import java.util.List;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

public abstract class KeyedMultiInputOperator20<
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
        OUT>
    extends BaseMultiInputKeyedOperator<OUT> {

  private static final long serialVersionUID = 1L;

  protected KeyedMultiInputOperator20(StreamOperatorParameters<OUT> params) {
    super(params, 20);
    addInputHandler(in1);
    addInputHandler(in2);
    addInputHandler(in3);
    addInputHandler(in4);
    addInputHandler(in5);
    addInputHandler(in6);
    addInputHandler(in7);
    addInputHandler(in8);
    addInputHandler(in9);
    addInputHandler(in10);
    addInputHandler(in11);
    addInputHandler(in12);
    addInputHandler(in13);
    addInputHandler(in14);
    addInputHandler(in15);
    addInputHandler(in16);
    addInputHandler(in17);
    addInputHandler(in18);
    addInputHandler(in19);
    addInputHandler(in20);
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

  protected abstract void processElement8(IN8 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement9(IN9 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement10(IN10 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement11(IN11 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement12(IN12 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement13(IN13 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement14(IN14 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement15(IN15 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement16(IN16 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement17(IN17 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement18(IN18 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement19(IN19 value, Context ctx, Collector<OUT> out)
      throws Exception;

  protected abstract void processElement20(IN20 value, Context ctx, Collector<OUT> out)
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

  private final AbstractInput<IN8, OUT> in8 =
      new AbstractInput<>(this, 8) {
        @Override
        public void processElement(StreamRecord<IN8> record) throws Exception {
          setKeyContextElement(record);
          processElement8(record.getValue(), ctx(record), out);
        }
      };

  private final AbstractInput<IN9, OUT> in9 =
      new AbstractInput<>(this, 9) {
        @Override
        public void processElement(StreamRecord<IN9> record) throws Exception {
          setKeyContextElement(record);
          processElement9(record.getValue(), ctx(record), out);
        }
      };

  private final AbstractInput<IN10, OUT> in10 =
      new AbstractInput<>(this, 10) {
        @Override
        public void processElement(StreamRecord<IN10> record) throws Exception {
          setKeyContextElement(record);
          processElement10(record.getValue(), ctx(record), out);
        }
      };

  private final AbstractInput<IN11, OUT> in11 =
      new AbstractInput<>(this, 11) {
        @Override
        public void processElement(StreamRecord<IN11> record) throws Exception {
          setKeyContextElement(record);
          processElement11(record.getValue(), ctx(record), out);
        }
      };

  private final AbstractInput<IN12, OUT> in12 =
      new AbstractInput<>(this, 12) {
        @Override
        public void processElement(StreamRecord<IN12> record) throws Exception {
          setKeyContextElement(record);
          processElement12(record.getValue(), ctx(record), out);
        }
      };

  private final AbstractInput<IN13, OUT> in13 =
      new AbstractInput<>(this, 13) {
        @Override
        public void processElement(StreamRecord<IN13> record) throws Exception {
          setKeyContextElement(record);
          processElement13(record.getValue(), ctx(record), out);
        }
      };

  private final AbstractInput<IN14, OUT> in14 =
      new AbstractInput<>(this, 14) {
        @Override
        public void processElement(StreamRecord<IN14> record) throws Exception {
          setKeyContextElement(record);
          processElement14(record.getValue(), ctx(record), out);
        }
      };

  private final AbstractInput<IN15, OUT> in15 =
      new AbstractInput<>(this, 15) {
        @Override
        public void processElement(StreamRecord<IN15> record) throws Exception {
          setKeyContextElement(record);
          processElement15(record.getValue(), ctx(record), out);
        }
      };

  private final AbstractInput<IN16, OUT> in16 =
      new AbstractInput<>(this, 16) {
        @Override
        public void processElement(StreamRecord<IN16> record) throws Exception {
          setKeyContextElement(record);
          processElement16(record.getValue(), ctx(record), out);
        }
      };

  private final AbstractInput<IN17, OUT> in17 =
      new AbstractInput<>(this, 17) {
        @Override
        public void processElement(StreamRecord<IN17> record) throws Exception {
          setKeyContextElement(record);
          processElement17(record.getValue(), ctx(record), out);
        }
      };

  private final AbstractInput<IN18, OUT> in18 =
      new AbstractInput<>(this, 18) {
        @Override
        public void processElement(StreamRecord<IN18> record) throws Exception {
          setKeyContextElement(record);
          processElement18(record.getValue(), ctx(record), out);
        }
      };

  private final AbstractInput<IN19, OUT> in19 =
      new AbstractInput<>(this, 19) {
        @Override
        public void processElement(StreamRecord<IN19> record) throws Exception {
          setKeyContextElement(record);
          processElement19(record.getValue(), ctx(record), out);
        }
      };

  private final AbstractInput<IN20, OUT> in20 =
      new AbstractInput<>(this, 20) {
        @Override
        public void processElement(StreamRecord<IN20> record) throws Exception {
          setKeyContextElement(record);
          processElement20(record.getValue(), ctx(record), out);
        }
      };

  @Override
  @SuppressWarnings("rawtypes")
  public List<Input> getInputs() {
    return super.getInputs();
  }
}
