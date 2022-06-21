package com.google.cloud.teleport.v2.neo4j.providers;


import com.google.cloud.teleport.v2.neo4j.common.model.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.List;

public interface IProvider {
    public void configure(OptionsParams optionsParams, JobSpecRequest jobSpecRequest);
    public boolean supportsSqlPushDown() ;
    public List<String> validateJobSpec();
    public Schema getSourceBeamSchema(Source source);
    public PCollection<Row> getSourceBeamRows(Pipeline pipeline, Source source, Schema sourceSchema);
    public PCollection<Row> getTargetBeamRows(Pipeline pipeline, Source source, Schema sourceSchema, PCollection<Row> sourceBeamRows, Target target);


}
