package com.google.cloud.teleport.v2.neo4j.providers.jdbc;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.neo4j.common.model.OptionsParams;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcToRow extends PTransform<PBegin, PCollection<Row>>{
    private static final Logger LOG = LoggerFactory.getLogger(JdbcToRow.class);
    JdbcSpec bqQuerySpec;
    OptionsParams optionsParams;

    public JdbcToRow(OptionsParams optionsParams, JdbcSpec bqQuerySpec){
        this.optionsParams=optionsParams;
        this.bqQuerySpec=bqQuerySpec;
    }

    @Override
    public PCollection<Row> expand(PBegin input) {

        String rewrittenSql=this.bqQuerySpec.SQL;
        LOG.info("Reading BQ with query: " + rewrittenSql);

        PCollection<TableRow> sourceRows = input.apply(bqQuerySpec.readDescription,BigQueryIO.readTableRowsWithSchema()
                .fromQuery(rewrittenSql)
                .usingStandardSql()
                .withTemplateCompatibility());

        Schema beamSchema = sourceRows.getSchema();
        Coder<Row> rowCoder = SchemaCoder.of(beamSchema);
        LOG.info("Beam schema: {}", beamSchema);
        PCollection<Row> beamRows =
                sourceRows.apply(bqQuerySpec.castDescription,
                                MapElements
                                        .into(TypeDescriptor.of(Row.class))
                                        .via(sourceRows.getToRowFunction()))
                        .setCoder(rowCoder);
        return beamRows;
    }



}
