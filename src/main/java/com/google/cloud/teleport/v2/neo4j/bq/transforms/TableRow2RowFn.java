package com.google.cloud.teleport.v2.neo4j.bq.transforms;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableRow2RowFn extends DoFn<TableRow, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(TableRow2RowFn.class);

  private Schema schema;

  public TableRow2RowFn(Schema sourceSchema) {
    this.schema = sourceSchema;
  }

  @ProcessElement
  public void processElement(ProcessContext processContext) {
    TableRow tableRow = processContext.element();
    LOG.info("Processing row with values: "+tableRow.values().size()+", schema cols: "+schema.getFieldCount());
    Row row = Row.withSchema(schema).attachValues(tableRow.values());
    processContext.output(row);
  }
}
