package com.google.cloud.teleport.v2.neo4j.providers;

import com.google.cloud.teleport.v2.neo4j.common.model.Source;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import lombok.Builder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

@Builder
public class TargetQuerySpec {
    public Source source;
    public Schema sourceBeamSchema;
    public PCollection<Row> nullableSourceRows;
    public Target target;
}
