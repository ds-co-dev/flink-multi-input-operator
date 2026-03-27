package dev.ds_co.flink.streaming.api.operators;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public abstract class BaseMultiInputKeyedOperator<OUT> extends AbstractStreamOperatorV2<OUT>
    implements MultipleInputStreamOperator<OUT>, Triggerable<Object, VoidNamespace> {

  private static final long serialVersionUID = 1L;

  private static final long NO_TIMESTAMP = Long.MIN_VALUE;

  protected transient Collector<OUT> out;
  private transient InternalTimerService<VoidNamespace> timerService;

  private final List<Input> inputs = new ArrayList<>();

  protected BaseMultiInputKeyedOperator(StreamOperatorParameters<OUT> params, int numberOfInputs) {
    super(params, numberOfInputs);
  }

  @Override
  public void open() throws Exception {
    super.open();
    out = new TimestampedCollector<>(output);
    this.timerService =
        getInternalTimerService("multi-input-timers", VoidNamespaceSerializer.INSTANCE, this);
  }

  //
  // Context
  //

  public class Context {

    private final long timestamp;

    private Context(long timestamp) {
      this.timestamp = timestamp;
    }

    public long timestamp() {
      return timestamp;
    }

    public boolean hasTimestamp() {
      return timestamp != NO_TIMESTAMP;
    }

    @SuppressWarnings("unchecked")
    public <K> K getCurrentKey(Class<K> clazz) {
      return (K) BaseMultiInputKeyedOperator.this.getCurrentKey();
    }

    public void registerProcessingTimeTimer(long timestamp) {
      timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, timestamp);
    }

    public void registerEventTimeTimer(long timestamp) {
      timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, timestamp);
    }

    public void deleteProcessingTimeTimer(long timestamp) {
      timerService.deleteProcessingTimeTimer(VoidNamespace.INSTANCE, timestamp);
    }

    public void deleteEventTimeTimer(long timestamp) {
      timerService.deleteEventTimeTimer(VoidNamespace.INSTANCE, timestamp);
    }

    public <X> void output(OutputTag<X> tag, X value) {
      BaseMultiInputKeyedOperator.this.output.collect(tag, new StreamRecord<>(value, timestamp));
    }
  }

  /** Timer callback context. */
  public class OnTimerContext extends Context {
    private final TimeDomain domain;

    private OnTimerContext(long timestamp, TimeDomain domain) {
      super(timestamp);
      this.domain = domain;
    }

    public TimeDomain timeDomain() {
      return domain;
    }
  }

  protected Context ctx(StreamRecord<?> record) {
    return new Context(record.hasTimestamp() ? record.getTimestamp() : NO_TIMESTAMP);
  }

  //
  // Timer callbacks
  //

  @Override
  public void onProcessingTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {
    onTimer(new OnTimerContext(timer.getTimestamp(), TimeDomain.PROCESSING_TIME));
  }

  @Override
  public void onEventTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {
    onTimer(new OnTimerContext(timer.getTimestamp(), TimeDomain.EVENT_TIME));
  }

  protected void onTimer(OnTimerContext ctx) throws Exception {}

  //
  // Input registration
  //

  protected <IN> void addInputHandler(AbstractInput<IN, OUT> in) {
    inputs.add(in);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public List<Input> getInputs() {
    return inputs;
  }
}
