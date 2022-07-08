package com.google.cloud.teleport.v2.neo4j.model.helpers;

import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import lombok.Builder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Convenience object for passing Source metadata, Target metadata, PCollection schema, and nullable source rows, together.
 */
@Builder
public class TargetQuerySpec {
    public Source source;
    public Schema sourceBeamSchema;
    public PCollection<Row> nullableSourceRows;
    public Target target;
}
