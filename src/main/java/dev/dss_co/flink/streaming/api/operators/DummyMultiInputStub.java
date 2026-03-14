package dev.dss_co.flink.streaming.api.operators;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

public class DummyMultiInputStub extends AbstractStreamOperatorV2<Object> {
    public DummyMultiInputStub(StreamOperatorParameters<Object> params) {
        super(params, 1);
    }
}
