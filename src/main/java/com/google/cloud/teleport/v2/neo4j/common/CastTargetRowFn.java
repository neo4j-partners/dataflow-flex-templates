package com.google.cloud.teleport.v2.neo4j.common;

import com.google.cloud.teleport.v2.neo4j.common.model.Targets;
import com.google.cloud.teleport.v2.neo4j.common.utils.TextParserUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CastTargetRowFn extends DoFn<Row, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(CastTargetRowFn.class);

  private Targets target;
  private Schema targetSchema;

  public CastTargetRowFn(Targets target, Schema targetSchema) {
    this.target = target;
    this.targetSchema = targetSchema;
  }

  @ProcessElement
  public void processElement(ProcessContext processContext) {
    Row inputRow = processContext.element();
    //transform
    Row outputRow = TextParserUtils.castRow(inputRow, target.mappings, targetSchema);
    if (outputRow!=null) {
      processContext.output(outputRow);
    } else {
      LOG.error("Row could not be parsed");
    }
  }
}
