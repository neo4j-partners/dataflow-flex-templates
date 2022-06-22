package com.google.cloud.teleport.v2.neo4j.providers.text;

import com.google.cloud.teleport.v2.neo4j.common.model.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.common.model.Source;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import com.google.cloud.teleport.v2.neo4j.providers.SourceQuerySpec;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextSourceFileToRow extends PTransform<PBegin, PCollection<Row>>{
    private static final Logger LOG = LoggerFactory.getLogger(TextSourceFileToRow.class);
    SourceQuerySpec sourceQuerySpec;
    OptionsParams optionsParams;

    public TextSourceFileToRow(OptionsParams optionsParams, SourceQuerySpec sourceQuerySpec){
       this.optionsParams=optionsParams;
        this.sourceQuerySpec=sourceQuerySpec;
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
        Source source=sourceQuerySpec.source;
        Schema beamTextSchema = sourceQuerySpec.sourceSchema;
        String dataFileUri= getRewritten(source);

        if (StringUtils.isNotBlank(dataFileUri)) {
            LOG.info("Ingesting file: " + dataFileUri + ".");
            return input
                    .apply("Read " + source.name + " data: " + dataFileUri, TextIO.read().from(dataFileUri))
                    .apply("Parse lines into string columns.", ParDo.of(new LineToRowFn(source, beamTextSchema, source.csvFormat)))
                    .setRowSchema(beamTextSchema);
        } else if (source.inline != null) {
            LOG.info("Processing " + source.inline.size() + " rows inline.");
            return input
                    .apply("Ingest inline dataset: " + source.name, Create.of(source.inline))
                    .apply("Parse lines into string columns.", ParDo.of(new ListOfStringToRowFn(beamTextSchema)))
                    .setRowSchema(beamTextSchema);
        } else {
            throw new RuntimeException("Data not found.");
        }
    }

    private String getRewritten(Source source) {

        String dataFileUri=source.uri;
        if (StringUtils.isNotEmpty(optionsParams.inputFilePattern)){
            LOG.info("Overriding source uri with run-time option");
            dataFileUri=optionsParams.inputFilePattern;
        }
        return ModelUtils.replaceTokens(dataFileUri,optionsParams.tokenMap);
    }

}
