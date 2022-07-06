package com.google.cloud.teleport.v2.neo4j.common.transforms;

import com.google.cloud.teleport.v2.neo4j.common.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.common.utils.DataCastingUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create typed Rows from String rows (from text files).
 */
public class CastExpandTargetRowFn extends DoFn<Row, Row> {

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static final Logger LOG = LoggerFactory.getLogger(CastExpandTargetRowFn.class);
    private final Target target;
    private final Schema targetSchema;

    public CastExpandTargetRowFn(Target target, Schema targetSchema) {
        this.target = target;
        this.targetSchema = targetSchema;
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) {
        Row inputRow = processContext.element();
        //transform
        List<Object> castVals = DataCastingUtils.sourceTextToTargetObjects(inputRow, target);
        if (targetSchema.getFieldCount() != castVals.size()) {
            LOG.error("Unable to parse line.  Expecting " + targetSchema.getFieldCount() + " fields, found " + castVals.size());
        } else {
            Row targetRow = Row.withSchema(targetSchema).attachValues(castVals);
            processContext.output(targetRow);
        }
    }
}
