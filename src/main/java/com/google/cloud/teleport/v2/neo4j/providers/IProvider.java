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
    public void configure(OptionsParams optionsParams, JobSpecRequest jobSpecRequest);
    public boolean supportsSqlPushDown() ;
    public List<String> validateJobSpec();

    public PTransform<PBegin, PCollection<Row>> querySourceBeamRows(SourceQuerySpec sourceQuerySpec);
    public PTransform<PBegin, PCollection<Row>> queryTargetBeamRows(TargetQuerySpec targetQuerySpec);
    public PTransform<PBegin, PCollection<Row>> queryMetadata(Source source);

}
