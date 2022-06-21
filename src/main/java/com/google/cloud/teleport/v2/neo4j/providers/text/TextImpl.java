package com.google.cloud.teleport.v2.neo4j.providers.text;

import com.google.cloud.teleport.v2.neo4j.providers.IProvider;
import com.google.cloud.teleport.v2.neo4j.providers.text.LineToRowFn;
import com.google.cloud.teleport.v2.neo4j.providers.text.StringListToRowFn;
import com.google.cloud.teleport.v2.neo4j.common.model.*;
import com.google.cloud.teleport.v2.neo4j.common.transforms.CastExpandTargetRowFn;
import com.google.cloud.teleport.v2.neo4j.common.utils.BeamUtils;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
    public Schema getSourceBeamSchema(Source source) {
        return  source.getTextFileSchema();
    }


    @Override
    public PCollection<Row> getSourceBeamRows(Pipeline pipeline, Source source, Schema sourceSchema) {

        PCollection<Row> beamTextRows;
        Schema beamTextSchema = getSourceBeamSchema(source);

        String dataFileUri=getDataUri(source);

        if (StringUtils.isNotBlank(dataFileUri)) {
            LOG.info("Ingesting file: " + dataFileUri + ".");
            beamTextRows = pipeline
                    .apply("Read " + source.name + " data: " + dataFileUri, TextIO.read().from(dataFileUri))
                    .apply("Parse lines into string columns.", ParDo.of(new LineToRowFn(source, beamTextSchema, source.csvFormat)))
                    .setRowSchema(beamTextSchema);
        } else if (source.inline != null) {
            LOG.info("Processing " + source.inline.size() + " rows inline.");
            beamTextRows = pipeline
                    .apply("Ingest inline dataset: " + source.name, Create.of(source.inline))
                    .apply("Parse lines into string columns.", ParDo.of(new StringListToRowFn(source, beamTextSchema)))
                    .setRowSchema(beamTextSchema);
        } else {
            throw new RuntimeException("Data not found.");
        }
        return beamTextRows;
    }


    @Override
    public PCollection<Row> getTargetBeamRows(Pipeline pipeline, Source source, Schema sourceSchema, PCollection<Row> sourceBeamRows, Target target) {

        Schema beamTextSchema = getSourceBeamSchema(source);
        String dataFileUri=getDataUri(source);
        final Schema targetSchema = BeamUtils.toBeamSchema(target);
        final Set<String> sourceFieldSet = ModelUtils.getBeamFieldSet(sourceSchema);
        final DoFn<Row, Row> castToTargetRow = new CastExpandTargetRowFn(target,targetSchema);

        // conditionally apply sql to rows..
        if (ModelUtils.targetHasTransforms(target)) {
            String SQL = ModelUtils.getTargetSql(sourceFieldSet, target, false);
            LOG.info("Target schema: {}",targetSchema);
            LOG.info("Executing SQL on PCOLLECTION: " + SQL);
            PCollection<Row> sqlDataRow = sourceBeamRows
                    .apply(target.sequence + ": SQLTransform " + target.name, SqlTransform.query(SQL));
            LOG.info("Sql final schema: {}",sqlDataRow.getSchema());
            return sqlDataRow.apply(target.sequence + ": Cast " + target.name + " rows", ParDo.of(castToTargetRow))
                    .setRowSchema(targetSchema);
        } else {
            LOG.info("Target schema: {}",targetSchema);
            return sourceBeamRows
                    .apply(target.sequence + ": Cast " + target.name + " rows", ParDo.of(castToTargetRow))
                    .setRowSchema(targetSchema);
        }

    }
    private String getDataUri(Source source) {

        String dataFileUri=source.uri;
        if (StringUtils.isNotEmpty(optionsParams.inputFilePattern)){
            LOG.info("Overriding source uri with run-time option");
            dataFileUri=optionsParams.inputFilePattern;
        }
        return ModelUtils.replaceTokens(dataFileUri,optionsParams.tokenMap);
    }
}
