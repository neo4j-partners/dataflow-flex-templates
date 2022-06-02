package com.google.cloud.teleport.v2.neo4j.bq.bq.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface BigQueryToNeo4jImportOptions extends DataflowPipelineOptions {

    @Description("Path to job specification")
    @Default.String("gs://dataflow-experiments-gs/dataflow-job-specs/testing/common/jobSpec.json")
    @Validation.Required
    String getJobSpecUri();
    void setJobSpecUri(String value);

    @Description("Path to Neo4j connection metadata")
    @Default.String("gs://dataflow-experiments-gs/dataflow-job-specs/testing/common/neo4jConnection.json")
    @Validation.Required
    String getNeo4jConnectionUri();
    void setNeo4jConnectionUri(String value);

    @Description("SQL query in standard SQL to pull data from BigQuery")
    String getReadQuery();
    void setReadQuery(String value);

}

