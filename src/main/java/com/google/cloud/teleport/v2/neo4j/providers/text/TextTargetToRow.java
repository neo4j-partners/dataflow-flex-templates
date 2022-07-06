package com.google.cloud.teleport.v2.neo4j.providers.text;

import com.google.cloud.teleport.v2.neo4j.common.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.common.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.common.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.common.transforms.CastExpandTargetRowFn;
import com.google.cloud.teleport.v2.neo4j.common.utils.BeamUtils;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import java.util.Set;
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

/**
 * Transform that takes a TargetQuerySpec object and products a PCollection.
 * For text providers, nullableSourceBeamRows will never be null.
 * This class applies in-memory SQL transforms on source rows.
 * Beam SQLTransform does not support ORDER BY nicely, therefore ordering must be forbidden.
 */

public class TextTargetToRow extends PTransform<PBegin, PCollection<Row>> {
    private static final Logger LOG = LoggerFactory.getLogger(TextTargetToRow.class);
    TargetQuerySpec targetQuerySpec;
    OptionsParams optionsParams;

    public TextTargetToRow(OptionsParams optionsParams, TargetQuerySpec targetQuerySpec) {
        this.optionsParams = optionsParams;
        this.targetQuerySpec = targetQuerySpec;
    }

    @Override
    public PCollection<Row> expand(PBegin input) {

        PCollection<Row> sourceBeamRows = targetQuerySpec.nullableSourceRows;
        Schema sourceSchema = targetQuerySpec.sourceBeamSchema;
        final Set<String> sourceFieldSet = ModelUtils.getBeamFieldSet(sourceSchema);

        Target target = targetQuerySpec.target;
        final Schema targetSchema = BeamUtils.toBeamSchema(target);
        final DoFn<Row, Row> castToTargetRow = new CastExpandTargetRowFn(target, targetSchema);

        // conditionally apply sql to rows..
        if (ModelUtils.targetHasTransforms(target)) {
            String sql = getRewritten(ModelUtils.getTargetSql(sourceFieldSet, target, false));
            LOG.info("Target schema: {}", targetSchema);
            LOG.info("Executing SQL on PCOLLECTION: " + sql);
            PCollection<Row> sqlDataRow = sourceBeamRows
                    .apply(target.sequence + ": SQLTransform " + target.name, SqlTransform.query(sql));
            LOG.info("Sql final schema: {}", sqlDataRow.getSchema());
            return sqlDataRow.apply(target.sequence + ": Cast " + target.name + " rows", ParDo.of(castToTargetRow))
                    .setRowSchema(targetSchema);
        } else {
            LOG.info("Target schema: {}", targetSchema);
            return sourceBeamRows
                    .apply(target.sequence + ": Cast " + target.name + " rows", ParDo.of(castToTargetRow))
                    .setRowSchema(targetSchema);
        }
    }

    private String getRewritten(String sql) {
        return ModelUtils.replaceVariableTokens(sql, optionsParams.tokenMap);
    }


}
