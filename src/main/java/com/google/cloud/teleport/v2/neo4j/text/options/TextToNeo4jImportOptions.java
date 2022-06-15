package com.google.cloud.teleport.v2.neo4j.text.options;

import com.google.cloud.teleport.v2.neo4j.common.options.Neo4jFlexTemplateOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface TextToNeo4jImportOptions extends Neo4jFlexTemplateOptions {

    @Description("The GCS location of the text you'd like to process")
    @Default.String("gs://neo4j-sandbox/mock-customers/customers-1k-noheader.txt")
    String getInputFilePattern();
    void setInputFilePattern(String value);

}

