package com.google.cloud.teleport.v2.neo4j.common.transforms;

import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.utils.DataCastingUtils;
import com.google.cloud.teleport.v2.neo4j.common.utils.TextParserUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CastTargetStringRowFn extends DoFn<Row, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(CastTargetStringRowFn.class);

  final static Gson gson = new GsonBuilder().setPrettyPrinting().create();

  private Target target;
  private Schema targetSchema;

  public CastTargetStringRowFn(Target target, Schema targetSchema) {
    this.target = target;
    this.targetSchema = targetSchema;
  }

  @ProcessElement
  public void processElement(ProcessContext processContext) {
    Row inputRow = processContext.element();
    //transform
    Row outputRow = DataCastingUtils.txtRowToTargetRow(inputRow, target.mappings, targetSchema);
    if (outputRow!=null) {
      processContext.output(outputRow);
    } else {
      LOG.error("Row could not be parsed");
    }
  }
}
