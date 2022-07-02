package com.google.cloud.teleport.v2.neo4j.providers.text;

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

public class LineToRowFn extends DoFn<String, Row> {

    private static final Logger LOG = LoggerFactory.getLogger(LineToRowFn.class);

    private final Source source;
    private final Schema schema;
    private final CSVFormat csvFormat;

    public LineToRowFn(Source source, Schema sourceSchema, CSVFormat csvFormat) {
        this.source = source;
        this.schema = sourceSchema;
        this.csvFormat = csvFormat;
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) {

        if (this.source.sourceType == SourceType.text) {

            String line = processContext.element();
            //Note: parser must return objects
            List<Object> strCols = TextParserUtils.parseDelimitedLine(csvFormat, line);
            if (strCols.size() > 0) {
                if (this.schema.getFieldCount() != strCols.size()) {
                    LOG.error("Unable to parse line.  Expecting " + this.schema.getFieldCount() + " fields, found " + strCols.size());
                } else {
                    Row row = Row.withSchema(this.schema).attachValues(strCols);
                    processContext.output(row);
                }
            } else {
                LOG.error("Row was empty!");
            }
        } else {
            LOG.error("Unhandled source type: " + source.sourceType);
        }
    }
}
