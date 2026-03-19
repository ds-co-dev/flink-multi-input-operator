package dev.ds_co.flink.streaming.api.operators;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

public abstract class BaseMultiInputKeyedOperator<OUT> extends AbstractStreamOperatorV2<OUT>
    implements MultipleInputStreamOperator<OUT>, Triggerable<Object, VoidNamespace> {

  private static final long serialVersionUID = 1L;

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

    private final Long timestamp;

    private Context(long ts) {
      this.timestamp = (ts == Long.MIN_VALUE ? null : ts);
    }

    public Long timestamp() {
      return timestamp;
    }

    @SuppressWarnings("unchecked")
    public <K> K getCurrentKey(Class<K> clazz) {
      return (K) BaseMultiInputKeyedOperator.this.getCurrentKey();
    }

    public void registerProcessingTimeTimer(long ts) {
      timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, ts);
    }

    public void registerEventTimeTimer(long ts) {
      timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, ts);
    }

    public void deleteProcessingTimeTimer(long ts) {
      timerService.deleteProcessingTimeTimer(VoidNamespace.INSTANCE, ts);
    }

    public void deleteEventTimeTimer(long ts) {
      timerService.deleteEventTimeTimer(VoidNamespace.INSTANCE, ts);
    }
  }

  /** Timer callback context. */
  public class OnTimerContext extends Context {
    private final TimeDomain domain;

    private OnTimerContext(long ts, TimeDomain domain) {
      super(ts);
      this.domain = domain;
    }

    public TimeDomain timeDomain() {
      return domain;
    }
  }

  protected Context ctx(StreamRecord<?> record) {
    return new Context(record.getTimestamp());
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
