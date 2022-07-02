package com.google.cloud.teleport.v2.neo4j.providers;


import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.common.model.Source;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.List;

public interface IProvider {
    void configure(OptionsParams optionsParams, JobSpecRequest jobSpecRequest);

    boolean supportsSqlPushDown();

    List<String> validateJobSpec();

    PTransform<PBegin, PCollection<Row>> querySourceBeamRows(SourceQuerySpec sourceQuerySpec);

    PTransform<PBegin, PCollection<Row>> queryTargetBeamRows(TargetQuerySpec targetQuerySpec);

    PTransform<PBegin, PCollection<Row>> queryMetadata(Source source);

}
