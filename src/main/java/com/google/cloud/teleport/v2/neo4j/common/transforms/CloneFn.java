package com.google.cloud.teleport.v2.neo4j.common.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.neo4j.bq.transforms.TableRow2RowFn;
import com.google.cloud.teleport.v2.neo4j.common.model.Source;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.SourceType;
import com.google.cloud.teleport.v2.neo4j.common.utils.TextParserUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

  public class CloneFn extends DoFn<Row, Row> {

    private static final Logger LOG = LoggerFactory.getLogger(com.google.cloud.teleport.v2.neo4j.common.transforms.CloneFn.class);

    private Schema schema;

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
