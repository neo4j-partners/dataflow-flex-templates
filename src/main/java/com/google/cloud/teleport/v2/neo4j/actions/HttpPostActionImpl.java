package com.google.cloud.teleport.v2.neo4j.actions;

import com.google.cloud.teleport.v2.neo4j.common.model.job.Action;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Http POST action handler.
 */
public class HttpPostActionImpl extends PTransform<PCollection<Row>, PCollection<Row>> {
    private static final Logger LOG = LoggerFactory.getLogger(HttpPostActionImpl.class);
    Action action;
    PCollection<Row> emptyReturn;

    public HttpPostActionImpl(Action action, PCollection<Row> emptyReturn) {
        this.action = action;
        this.emptyReturn = emptyReturn;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {


        return this.emptyReturn;
    }
}
