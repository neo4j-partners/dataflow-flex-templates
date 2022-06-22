package com.google.cloud.teleport.v2.neo4j.providers.bq;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.neo4j.common.model.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
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

public class BqQueryToRow extends PTransform<PBegin, PCollection<Row>>{
    private static final Logger LOG = LoggerFactory.getLogger(BqQueryToRow.class);
    BqQuerySpec bqQuerySpec;
    OptionsParams optionsParams;

    public BqQueryToRow(    OptionsParams optionsParams, BqQuerySpec bqQuerySpec){
        this.optionsParams=optionsParams;
        this.bqQuerySpec=bqQuerySpec;
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
        PCollection<TableRow> sourceRows = input.apply(bqQuerySpec.readDescription,BigQueryIO.readTableRowsWithSchema()
                .fromQuery(getRewritten(this.bqQuerySpec.SQL))
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

    private String getRewritten(String sql) {
        if (StringUtils.isNotEmpty(optionsParams.readQuery)){
            LOG.info("Overriding source uri with run-time option");
            sql=optionsParams.readQuery;
        }
        return ModelUtils.replaceTokens(sql,optionsParams.tokenMap);
    }

}
