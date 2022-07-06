package com.google.cloud.teleport.v2.neo4j.providers.bq;

import com.google.cloud.teleport.v2.neo4j.common.model.helpers.SourceQuerySpec;
import com.google.cloud.teleport.v2.neo4j.common.model.helpers.SqlQuerySpec;
import com.google.cloud.teleport.v2.neo4j.common.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.common.model.job.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.common.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import com.google.cloud.teleport.v2.neo4j.providers.IProvider;
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

/**
 * Provider implementation for reading and writing BigQuery.
 */
public class BigQueryImpl implements IProvider {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryImpl.class);

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

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
        return new BqQueryToRow(optionsParams, getSourceQueryBeamSpec(sourceQuerySpec));
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> queryTargetBeamRows(TargetQuerySpec targetQuerySpec) {
        return new BqQueryToRow(optionsParams, getTargetQueryBeamSpec(targetQuerySpec));
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> queryMetadata(Source source) {
        return new BqQueryToRow(optionsParams, getMetadataQueryBeamSpec(source));
    }

    /**
     * Returns zero rows metadata query based on original query.
     *
     * @param source
     * @return helper object includes metadata and SQL
     */
    public SqlQuerySpec getMetadataQueryBeamSpec(Source source) {

        final String baseQuery = getBaseQuery(source);

        ////////////////////////////
        // Dry run won't return schema so use regular query
        // We need fieldSet for SQL generation later
        final String zeroRowSql = "SELECT * FROM (" + baseQuery + ") LIMIT 0";
        LOG.info("Reading BQ metadata with query: " + zeroRowSql);

        return SqlQuerySpec.builder()
                .readDescription("Read from BQ " + source.name)
                .castDescription("Cast to BeamRow " + source.name)
                .sql(zeroRowSql)
                .build();
    }

    /**
     * Returns base source query from source helper object.
     *
     * @param sourceQuerySpec
     * @return helper object includes metadata and SQL
     */
    public SqlQuerySpec getSourceQueryBeamSpec(SourceQuerySpec sourceQuerySpec) {
        return SqlQuerySpec.builder()
                .castDescription("Cast to BeamRow " + sourceQuerySpec.source.name)
                .readDescription("Read from BQ " + sourceQuerySpec.source.name)
                .sql(getBaseQuery(sourceQuerySpec.source))
                .build();
    }

    /**
     * Returns target query from helper object which includes source and target.
     *
     * @param targetQuerySpec
     * @return helper object includes metadata and SQL
     */
    public SqlQuerySpec getTargetQueryBeamSpec(TargetQuerySpec targetQuerySpec) {
        Set<String> sourceFieldSet = ModelUtils.getBeamFieldSet(targetQuerySpec.sourceBeamSchema);
        final String baseSql = getBaseQuery(targetQuerySpec.source);
        final String targetSpecificSql = ModelUtils.getTargetSql(sourceFieldSet,
                targetQuerySpec.target,
                true,
                baseSql);
        return SqlQuerySpec.builder()
                .readDescription(targetQuerySpec.target.sequence + ": Read from BQ " + targetQuerySpec.target.name)
                .castDescription(targetQuerySpec.target.sequence + ": Cast to BeamRow " + targetQuerySpec.target.name)
                .sql(targetSpecificSql)
                .build();
    }

    private String getBaseQuery(Source source) {
        String baseSql = source.query;
        if (StringUtils.isNotEmpty(optionsParams.readQuery)) {
            LOG.info("Overriding source query with run-time option");
            baseSql = optionsParams.readQuery;
        }
        return baseSql;
    }


}
