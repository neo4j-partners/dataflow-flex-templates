package com.google.cloud.teleport.v2.neo4j.common.transforms;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloneFn extends DoFn<Row, Row> {

    private static final Logger LOG = LoggerFactory.getLogger(CloneFn.class);

    private final Schema schema;

    public CloneFn(Schema sourceSchema) {
        this.schema = sourceSchema;
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) {
        Row input = processContext.element();
        Row row = Row.withSchema(schema).attachValues(input.getValues());
        processContext.output(row);
    }
}
