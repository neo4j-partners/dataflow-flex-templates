package com.google.cloud.teleport.v2.neo4j.text.transforms;

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

public class LineParsingFn extends DoFn<String, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(LineParsingFn.class);

  private Source source;
  private Schema schema;
  private CSVFormat csvFormat;

  public LineParsingFn(Source source, Schema sourceSchema, CSVFormat csvFormat) {
    this.source = source;
    this.schema = sourceSchema;
    this.csvFormat = csvFormat;
  }

  @ProcessElement
  public void processElement(ProcessContext processContext) {

    if (this.source.sourceType== SourceType.text){

      String line = processContext.element();
      //transform
      List<Object> strCols = TextParserUtils.parseDelimitedLine( csvFormat, line);
      if (strCols.size()>0) {
        Row row = Row.withSchema(this.schema).addValues(strCols).build();
        //LOG.info("Processed row, field count: "+row.getSchema().getFieldCount()+", values: "+strCols.size());
        processContext.output(row);
      } else {
        LOG.error("Row was empty!");
      }
    } else {
      LOG.error("Unhandled source type: "+source.sourceType);
    }
  }
}
