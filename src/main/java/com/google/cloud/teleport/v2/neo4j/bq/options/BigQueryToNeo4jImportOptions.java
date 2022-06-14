package com.google.cloud.teleport.v2.neo4j.bq.options;

import com.google.cloud.teleport.v2.neo4j.common.options.Neo4jFlexTemplateOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface BigQueryToNeo4jImportOptions extends Neo4jFlexTemplateOptions {

    @Description("SQL query in standard SQL to pull data from BigQuery")
    String getReadQuery();
    void setReadQuery(String value);

}

