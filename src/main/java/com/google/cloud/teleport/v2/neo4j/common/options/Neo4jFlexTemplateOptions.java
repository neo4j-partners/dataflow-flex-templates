package com.google.cloud.teleport.v2.neo4j.common.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface Neo4jFlexTemplateOptions extends PipelineOptions {

    @Description("Path to job specification")
    @Default.String("gs://neo4j-dataflow/job-specs/testing/common/bq-northwind-jobspec.json")
    @Validation.Required
    String getJobSpecUri();
    void setJobSpecUri(String value);

    @Description("Path to Neo4j connection metadata")
    @Default.String("gs://neo4j-dataflow/job-specs/testing/common/auradb-free-connection.json")
    @Validation.Required
    String getNeo4jConnectionUri();
    void setNeo4jConnectionUri(String value);
}
