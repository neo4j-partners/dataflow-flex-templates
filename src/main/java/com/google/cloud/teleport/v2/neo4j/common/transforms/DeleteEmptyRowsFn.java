package com.google.cloud.teleport.v2.neo4j.common.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

import java.util.List;

/**
 * Delete empty rows transformation.
 */
public class DeleteEmptyRowsFn extends DoFn<Row, Row> {

    @ProcessElement
    public void processElement(ProcessContext processContext) {
        Row inputRow = processContext.element();
        if (allNull(inputRow.getValues())) {
            //do not include
        } else {
            processContext.output(inputRow);
        }
    }

    private boolean allNull(List<Object> values) {
        for (Object val : values) {
            if (val != null) {
                return false;
            }
        }
        return true;
    }
}

