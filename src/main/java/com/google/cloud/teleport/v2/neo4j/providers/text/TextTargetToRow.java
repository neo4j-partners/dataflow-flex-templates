package com.google.cloud.teleport.v2.neo4j.providers.text;

import com.google.cloud.teleport.v2.neo4j.common.model.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.common.model.Source;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.transforms.CastExpandTargetRowFn;
import com.google.cloud.teleport.v2.neo4j.common.utils.BeamUtils;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import com.google.cloud.teleport.v2.neo4j.providers.TargetQuerySpec;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class TextTargetToRow extends PTransform<PBegin, PCollection<Row>>{
    private static final Logger LOG = LoggerFactory.getLogger(TextTargetToRow.class);
    TargetQuerySpec targetQuerySpec;
    OptionsParams optionsParams;

    public TextTargetToRow(OptionsParams optionsParams, TargetQuerySpec targetQuerySpec){
       this.optionsParams=optionsParams;
        this.targetQuerySpec =targetQuerySpec;
    }

    @Override
    public PCollection<Row> expand(PBegin input) {

        PCollection<Row> sourceBeamRows = targetQuerySpec.nullableSourceRows;
        Schema sourceSchema = targetQuerySpec.nullableSourceRows.getSchema();
        final Set<String> sourceFieldSet = ModelUtils.getBeamFieldSet(sourceSchema);

        Target target=targetQuerySpec.target;
        final Schema targetSchema = BeamUtils.toBeamSchema(target);
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

    private String getRewritten(Source source) {

        String dataFileUri=source.uri;
        if (StringUtils.isNotEmpty(optionsParams.inputFilePattern)){
            LOG.info("Overriding source uri with run-time option");
            dataFileUri=optionsParams.inputFilePattern;
        }
        return ModelUtils.replaceTokens(dataFileUri,optionsParams.tokenMap);
    }

}
