package com.google.cloud.teleport.v2.neo4j.providers.bq;

import com.google.cloud.teleport.v2.neo4j.common.model.*;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import com.google.cloud.teleport.v2.neo4j.providers.IProvider;
import com.google.cloud.teleport.v2.neo4j.providers.SourceQuerySpec;
import com.google.cloud.teleport.v2.neo4j.providers.TargetQuerySpec;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
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
    public PTransform<PBegin, PCollection<Row>> querySourceBeamRows(SourceQuerySpec sourceQuerySpec) {
        return new BqQueryToRow(optionsParams,getSourceQueryBeamSpec(sourceQuerySpec));
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> queryTargetBeamRows(TargetQuerySpec targetQuerySpec) {
        return new BqQueryToRow(optionsParams,getTargetQueryBeamSpec(targetQuerySpec));
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> queryMetadata(Source source) {
        return new BqQueryToRow(optionsParams,getMetadataQueryBeamSpec(source));
    }

    public BqQuerySpec getMetadataQueryBeamSpec(Source source) {

        String BASE_SQL = getBaseQuery(source);
        LOG.info("Reading from BQ with query: " + BASE_SQL);

        ////////////////////////////
        // Dry run won't return schema so use regular query
        // We need fieldSet for SQL generation later
        String ZERO_ROW_SQL = "SELECT * FROM (" + BASE_SQL + ") LIMIT 0";
        return BqQuerySpec.builder()
                .readDescription("Read from BQ " + source.name)
                .castDescription("Cast to BeamRow " + source.name)
                .SQL(ZERO_ROW_SQL)
                .build();
    }

    public BqQuerySpec getSourceQueryBeamSpec(SourceQuerySpec sourceQuerySpec) {
        return BqQuerySpec.builder()
                .castDescription("Cast to BeamRow " + sourceQuerySpec.source.name)
                .readDescription("Read from BQ " +  sourceQuerySpec.source.name)
                .SQL(getBaseQuery( sourceQuerySpec.source))
                .build();
    }

    public BqQuerySpec getTargetQueryBeamSpec(TargetQuerySpec targetQuerySpec) {
        Set<String> sourceFieldSet = ModelUtils.getBeamFieldSet( targetQuerySpec.sourceBeamSchema);
        String BASE_SQL = getBaseQuery(targetQuerySpec.source);
        String TARGET_SPECIFIC_SQL = ModelUtils.getTargetSql(sourceFieldSet, targetQuerySpec.target, true, BASE_SQL);
        return BqQuerySpec.builder()
                .readDescription(targetQuerySpec.target.sequence + ": Read from BQ " + targetQuerySpec.target.name)
                .castDescription(targetQuerySpec.target.sequence + ": Cast to BeamRow " + targetQuerySpec.target.name)
                .SQL(TARGET_SPECIFIC_SQL)
                .build();
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
