package com.google.cloud.teleport.v2.neo4j.common.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface Neo4jFlexTemplateOptions extends PipelineOptions {

    @Description("Path to job specification")
    @Validation.Required
    String getJobSpecUri();
    void setJobSpecUri(String value);

    @Description("Path to Neo4j connection metadata")
    @Validation.Required
    String getNeo4jConnectionUri();
    void setNeo4jConnectionUri(String value);

    @Description("Options JSON (see documentation)")
    String getOptionsJson();
    void setOptionsJson(String value);

    @Description("Read query")
    @Default.String("")
    String getReadQuery();
    void setReadQuery(String value);


    @Description("Input file pattern")
    @Default.String("")
    String getInputFilePattern();
    void setInputFilePattern(String value);
}
