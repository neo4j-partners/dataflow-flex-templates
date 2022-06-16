package com.google.cloud.teleport.v2.neo4j.common.transforms;


import com.google.cloud.teleport.v2.neo4j.common.database.CypherGenerator;
import com.google.cloud.teleport.v2.neo4j.common.database.DirectConnect;
import com.google.cloud.teleport.v2.neo4j.common.model.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.common.utils.BeamSchemaUtils;
import com.google.cloud.teleport.v2.neo4j.common.utils.DataCastingUtils;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.neo4j.Neo4jIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.neo4j.driver.Config;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TargetWriterTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
    private static final Logger LOG = LoggerFactory.getLogger(TargetWriterTransform.class);

    JobSpecRequest jobSpec;
    ConnectionParams neoConnection;
    Target target;

    boolean requery=false;
    boolean allowSort=false;

    public TargetWriterTransform(JobSpecRequest jobSpec, ConnectionParams neoConnection, Target target,boolean requery, boolean allowSort) {
        this.jobSpec=jobSpec;
        this.neoConnection=neoConnection;
        this.target=target;
        this.requery=requery;
        this.allowSort=allowSort;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {

        /////////////////////////////////
        // Target schema transform
        final Schema targetSchema = BeamSchemaUtils.toBeamSchema(target);
        final DoFn<Row, Row> castToTargetRow = new CastTargetStringRowFn(target, targetSchema);

        // indices and constraints
        List<String> cyphers = CypherGenerator.getNodeIndexAndConstraintsCypherStatements(target);
        if (cyphers.size() > 0) {
            DirectConnect neo4jDirectConnect = new DirectConnect(neoConnection);
            LOG.info("Adding "+cyphers.size() +" indices and constraints");
            for (String cypher : cyphers) {
                LOG.info("Executing cypher: " + cypher);
                try {
                    neo4jDirectConnect.executeOnNeo4j(
                            cypher,
                            true);
                } catch (Exception e) {
                    LOG.error("Error executing cypher: " + cypher + ", " + e.getMessage());
                }
            }
        }

        //set batch sizes
        int batchSize = jobSpec.config.nodeBatchSize;
        int parallelism = jobSpec.config.nodeParallelism;

        if (target.type == TargetType.relationship) {
            batchSize = jobSpec.config.edgeBatchSize;
            parallelism = jobSpec.config.edgeParallelism;
        }

        // data loading
        String unwindCypher = CypherGenerator.getUnwindCreateCypher(target);
        LOG.info("Unwind cypher: " + unwindCypher);

        /////////////////////////
        // Batch load data rows using Matt's Neo4jIO connector
        /////////////////////////
        final Neo4jIO.DriverConfiguration driverConfiguration =
                Neo4jIO.DriverConfiguration.create(neoConnection.serverUrl, neoConnection.username, neoConnection.password)
                        .withConfig(Config.defaultConfig());

        final SessionConfig sessionConfig = SessionConfig.builder()
                .withDatabase(neoConnection.database)
                .build();

        final PTransform writeNeo4jIO = Neo4jIO.<Row>writeUnwind()
                .withCypher(unwindCypher)
                .withBatchSize(batchSize)
                .withSessionConfig(sessionConfig)
                .withUnwindMapName("rows")
                .withParametersFunction(
                        row -> {
                            return DataCastingUtils.rowToNeo4jDataMap(row, target);
                        })
                .withDriverConfiguration(driverConfiguration);

        PCollection<Row> output =null;

        String SQL = ModelUtils.getTargetSql(ModelUtils.getBeamFieldSet(input.getSchema()), target,allowSort);
        // conditionally apply sql to rows..
        if (!SQL.equals(ModelUtils.DEFAULT_STAR_QUERY) && requery) {
            LOG.info("Executing SQL: "+SQL);
            PCollection<Row> castData=input.apply(target.sequence + ": SQLTransform " + target.name, SqlTransform.query(SQL))
                                    .apply(target.sequence + ": Cast " + target.name + " rows", ParDo.of(castToTargetRow))
                                    .setRowSchema(targetSchema);
            LOG.info("Target fieldNames: "+ StringUtils.join(targetSchema.getFieldNames(),","));
            POutput doneButNotBlocking=castData.apply(target.sequence + ": Neo4j write " + target.name, writeNeo4jIO);
            //This is the best we can do without Neo4jIO returning PCollection<Row> (no rows)
            output = input.getPipeline().apply(Create.empty(input.getCoder()));

        } else {
            PCollection<Row> castData=input.apply(target.sequence + ": Cast " + target.name + " rows", ParDo.of(castToTargetRow))
                    .setRowSchema(targetSchema);
            LOG.info("Target fieldNames: "+ StringUtils.join(targetSchema.getFieldNames(),","));
            POutput doneButNotBlocking=castData.apply(target.sequence + ": Neo4j write " + target.name, writeNeo4jIO);
            //Is this the best we can do here to block as long as possible?
            output = input.getPipeline().apply(Create.empty(input.getCoder()));
        }

        return output;
    }


}
