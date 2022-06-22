package com.google.cloud.teleport.v2.neo4j.providers.text;

import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.common.model.Source;
import com.google.cloud.teleport.v2.neo4j.providers.IProvider;
import com.google.cloud.teleport.v2.neo4j.providers.SourceQuerySpec;
import com.google.cloud.teleport.v2.neo4j.providers.TargetQuerySpec;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TextImpl implements IProvider {

    private static final Logger LOG = LoggerFactory.getLogger(TextImpl.class);

    public TextImpl(){}
    private JobSpecRequest jobSpec;
    private OptionsParams optionsParams;
    @Override
    public void configure(OptionsParams optionsParams, JobSpecRequest jobSpecRequest) {
        this.optionsParams=optionsParams;
        this.jobSpec=jobSpecRequest;
    }

    @Override
    public boolean supportsSqlPushDown() {
        return false;
    }

    @Override
    public List<String> validateJobSpec() {
        //no specific validations currently

        return new ArrayList<>();
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> querySourceBeamRows(SourceQuerySpec sourceQuerySpec) {
        return new TextSourceFileToRow(optionsParams,sourceQuerySpec);
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> queryTargetBeamRows(TargetQuerySpec targetQuerySpec) {
        return new TextTargetToRow(optionsParams,targetQuerySpec);
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> queryMetadata(Source source) {
        return new TextSourceFileMetadataToRow(optionsParams,source);
    }




}
