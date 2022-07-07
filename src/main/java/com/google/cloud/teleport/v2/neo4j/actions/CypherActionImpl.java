package com.google.cloud.teleport.v2.neo4j.actions;

import com.google.cloud.teleport.v2.neo4j.common.model.job.Action;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Cypher runner action handler.
 */
public class CypherActionImpl extends PTransform<PCollection<Row>, PCollection<Row>> {
    private static final Logger LOG = LoggerFactory.getLogger(CypherActionImpl.class);
    Action action;
    PCollection<Row> emptyReturn;
    public CypherActionImpl(Action action,PCollection<Row> emptyReturn) {
        this.action=action;
        this.emptyReturn=emptyReturn;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {


        return this.emptyReturn;
    }
}
