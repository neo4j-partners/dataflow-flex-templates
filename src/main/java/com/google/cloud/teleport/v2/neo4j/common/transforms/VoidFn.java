package com.google.cloud.teleport.v2.neo4j.common.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

public class VoidFn extends DoFn<Row, Void> {

    @ProcessElement
    public void processElement(ProcessContext context) {
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) {
        //context.output(VoidCoder.of(), BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE);
    }
}
