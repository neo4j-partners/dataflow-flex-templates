package com.google.cloud.teleport.v2.neo4j.text.options;

import org.apache.beam.sdk.options.*;

public interface TextToNeo4jImportOptions extends PipelineOptions {

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

    @Description("The GCS location of the text you'd like to process")
    @Default.String("gs://neo4j-sandbox/mock-customers/customers-1k-noheader.txt")
    String getInputFilePattern();
    void setInputFilePattern(String value);

}

