package com.google.cloud.teleport.v2.neo4j.providers;

import com.google.cloud.teleport.v2.neo4j.common.model.Source;
import lombok.Builder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PInput;

@Builder
public class SourceQuerySpec {
    public Source source;
    public Schema sourceSchema;
}
