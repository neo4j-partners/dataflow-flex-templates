package com.google.cloud.teleport.v2.neo4j.providers.text;

import com.google.cloud.teleport.v2.neo4j.common.model.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.common.model.Source;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextSourceFileMetadataToRow extends PTransform<PBegin, PCollection<Row>> {
    private static final Logger LOG = LoggerFactory.getLogger(TextSourceFileMetadataToRow.class);
    Source source;
    OptionsParams optionsParams;

    public TextSourceFileMetadataToRow(OptionsParams optionsParams, Source source) {
        this.optionsParams = optionsParams;
        this.source = source;
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
        Schema schema = source.getTextFileSchema();
        return input.apply(Create.empty(schema));
    }

}
