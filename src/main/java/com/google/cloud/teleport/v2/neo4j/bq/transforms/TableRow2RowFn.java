package com.google.cloud.teleport.v2.neo4j.bq.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.neo4j.common.utils.DataCastingUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class TableRow2RowFn extends DoFn<TableRow, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(TableRow2RowFn.class);

  private  com.google.cloud.bigquery.Schema bigQuerySchema;
  private Schema beamSchema;
  public TableRow2RowFn(com.google.cloud.bigquery.Schema bigQuerySchema, Schema beamSchema) {
    this.bigQuerySchema = bigQuerySchema;
    this.beamSchema = beamSchema;
  }

  @ProcessElement
  public void processElement(ProcessContext processContext) {
    TableRow tableRow = processContext.element();
    LOG.info("Processing row with values: "+tableRow.values().size()+", schema cols: "+ bigQuerySchema.getFields().size());
    Collection<Object> beamValues = DataCastingUtils.bigQueryToBeamValues(bigQuerySchema, tableRow.values());
    Row row = Row.withSchema(beamSchema).attachValues(beamValues);
    processContext.output(row);
  }
}
