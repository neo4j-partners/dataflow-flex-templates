package com.google.cloud.teleport.v2.neo4j.providers.text;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Transforms list of object to PCollection<Row>.
 */

public class ListOfStringToRowFn extends DoFn<List<Object>, Row> {

    private static final Logger LOG = LoggerFactory.getLogger(ListOfStringToRowFn.class);

    private final Schema schema;

    public ListOfStringToRowFn(Schema sourceSchema) {
        this.schema = sourceSchema;
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) {

        List<Object> strCols = processContext.element();
        if (this.schema.getFieldCount() != strCols.size()) {
            LOG.error("Row field count mismatch, expecting: " + this.schema.getFieldCount() + ", row: " + StringUtils.join(strCols.size(), ","));
        } else {
            Row row = Row.withSchema(this.schema).addValues(strCols).build();
            processContext.output(row);
        }
    }
}
