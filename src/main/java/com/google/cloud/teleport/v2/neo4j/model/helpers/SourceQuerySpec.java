package com.google.cloud.teleport.v2.neo4j.model.helpers;

import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import lombok.Builder;
import org.apache.beam.sdk.schemas.Schema;

/**
 * Convenience object for passing Source metadata and PCollection schema together.
 */
@Builder
public class SourceQuerySpec {
    public Source source;
    public Schema sourceSchema;
}
