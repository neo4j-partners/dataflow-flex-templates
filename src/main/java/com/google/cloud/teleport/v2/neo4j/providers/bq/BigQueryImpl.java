package com.google.cloud.teleport.v2.neo4j.providers.bq;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.v2.neo4j.common.model.*;
import com.google.cloud.teleport.v2.neo4j.common.utils.BeamUtils;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import com.google.cloud.teleport.v2.neo4j.providers.IProvider;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class BigQueryImpl implements IProvider {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryImpl.class);

    final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    private JobSpecRequest jobSpec;
    private OptionsParams optionsParams;

    public BigQueryImpl() {
    }

    @Override
    public void configure(OptionsParams optionsParams, JobSpecRequest jobSpecRequest) {
        this.jobSpec = jobSpecRequest;
        this.optionsParams = optionsParams;
    }

    @Override
    public boolean supportsSqlPushDown() {
        return true;
    }

     @Override
    public List<String> validateJobSpec() {
        //no specific validations currently

        return new ArrayList<>();
    }

    @Override
    public Schema getSourceBeamSchema(Source source) {

        String BASE_SQL = getBaseQuery(source);
        LOG.info("Reading from BQ with query: " + BASE_SQL);

        ////////////////////////////
        // Dry run won't return schema so use regular query
        // We need fieldSet for SQL generation later
        String ZERO_ROW_SQL = "SELECT * FROM (" + BASE_SQL + ") LIMIT 0";
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(ZERO_ROW_SQL).build();

        Schema initSchema = null;
        try {
            LOG.info("Metadata query: " + ZERO_ROW_SQL);
            TableResult zeroRowQueryResult = bigquery.query(queryConfig);
            initSchema = BeamUtils.toBeamSchema(zeroRowQueryResult.getSchema());
        } catch (Exception e) {
            String errMsg = "Error running query: " + e.getMessage();
            LOG.error(errMsg);
            throw new RuntimeException(errMsg);
        }

        return initSchema;
    }

    @Override
    public PCollection<Row> getSourceBeamRows(Pipeline pipeline, Source source, Schema sourceSchema) {
        // will never be querying source beam rows
        String readDescription = "Read from BQ " + source.name;
        String castDescription = "Cast to BeamRow " + source.name;
        String BASE_SQL = getBaseQuery(source);
        return queryBigQuery( pipeline, readDescription, castDescription, BASE_SQL);
    }


    @Override
    public PCollection<Row> getTargetBeamRows(Pipeline pipeline, Source source, Schema sourceSchema, PCollection<Row> sourceBeamRows, Target target) {
        Set<String> sourceFieldSet = ModelUtils.getBeamFieldSet(sourceSchema);
        String BASE_SQL = getBaseQuery(source);
        String TARGET_SPECIFIC_SQL = ModelUtils.getTargetSql(sourceFieldSet, target, true, BASE_SQL);
        String readDescription = target.sequence + ": Read from BQ " + target.name;
        String castDescription = target.sequence + ": Cast to BeamRow " + target.name;
        return queryBigQuery( pipeline, readDescription, castDescription, TARGET_SPECIFIC_SQL);
    }

    private PCollection<Row> queryBigQuery(Pipeline pipeline, String readDescription, String castDescription, String SQL){
        PCollection<TableRow> sourceRows =
                pipeline.apply(readDescription, BigQueryIO.readTableRowsWithSchema()
                        .fromQuery(SQL)
                        .usingStandardSql()
                        .withTemplateCompatibility());

        Schema beamSchema = sourceRows.getSchema();
        Coder<Row> rowCoder = SchemaCoder.of(beamSchema);
        LOG.info("Beam schema: {}", beamSchema);
        PCollection<Row> beamRows =
                sourceRows.apply(castDescription,
                                MapElements
                                        .into(TypeDescriptor.of(Row.class))
                                        .via(sourceRows.getToRowFunction()))
                        .setCoder(rowCoder);
        return beamRows;
    }

    private String getBaseQuery(Source source) {
        String BASE_SQL = source.query;
        if (StringUtils.isNotEmpty(optionsParams.readQuery)) {
            LOG.info("Overriding source query with run-time option");
            BASE_SQL = optionsParams.readQuery;
        }
        return ModelUtils.replaceTokens(BASE_SQL, optionsParams.tokenMap);
    }
}
