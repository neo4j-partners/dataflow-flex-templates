package com.google.cloud.teleport.v2.neo4j.common.transforms;

import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.utils.DataCastingUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CastExpandTargetRowFn extends DoFn<Row, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(CastExpandTargetRowFn.class);

  final static Gson gson = new GsonBuilder().setPrettyPrinting().create();

  private final Target target;
  private final Schema targetSchema;

  public CastExpandTargetRowFn(Target target, Schema targetSchema) {
    this.target = target;
    this.targetSchema= targetSchema;
  }

  @ProcessElement
  public void processElement(ProcessContext processContext) {
    Row inputRow = processContext.element();
    //transform
    List<Object> castVals = DataCastingUtils.sourceTextToTargetObjects(inputRow, target);
    if (targetSchema.getFieldCount()!=castVals.size()){
      LOG.error("Unable to parse line.  Expecting "+targetSchema.getFieldCount()+" fields, found "+castVals.size());
    } else {
      Row targetRow = Row.withSchema(targetSchema).attachValues(castVals);
      processContext.output(targetRow);
    }
  }
}