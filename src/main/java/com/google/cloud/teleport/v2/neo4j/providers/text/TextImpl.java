package com.google.cloud.teleport.v2.neo4j.providers.text;

import com.google.cloud.teleport.v2.neo4j.model.helpers.SourceQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.providers.Provider;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provider implementation for reading and writing Text files.
 */
public class TextImpl implements Provider {

    private static final Logger LOG = LoggerFactory.getLogger(TextImpl.class);
    private JobSpec jobSpec;
    private OptionsParams optionsParams;

    public TextImpl() {
    }

    @Override
    public void configure(OptionsParams optionsParams, JobSpec jobSpecRequest) {
        this.optionsParams = optionsParams;
        this.jobSpec = jobSpecRequest;
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
        return new TextSourceFileToRow(optionsParams, sourceQuerySpec);
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> queryTargetBeamRows(TargetQuerySpec targetQuerySpec) {
        return new TextTargetToRow(optionsParams, targetQuerySpec);
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> queryMetadata(final Source source) {
        return new TextSourceFileMetadataToRow(optionsParams, source);
    }


}
