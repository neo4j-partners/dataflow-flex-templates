package com.google.cloud.teleport.v2.neo4j.common.database;


import com.google.cloud.teleport.v2.neo4j.common.model.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.common.transforms.CastTargetStringRowFn;
import com.google.cloud.teleport.v2.neo4j.common.utils.BeamSchemaUtils;
import com.google.cloud.teleport.v2.neo4j.common.utils.DataCastingUtils;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.neo4j.Neo4jIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.neo4j.driver.Config;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils.getRelationshipKeyField;

public class TargetWriter {
    private static final Logger LOG = LoggerFactory.getLogger(TargetWriter.class);

    private static Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public static void resetNeo4j(ConnectionParams neoConnection) {
        // Direct connect utility...
        DirectConnect neo4jDirectConnect = new DirectConnect(neoConnection.serverUrl, neoConnection.database, neoConnection.username, neoConnection.password);
        LOG.info("Resetting database");
        try {
            LOG.info("Executing cypher: " + ModelUtils.CYPHER_DELETE_ALL);
            neo4jDirectConnect.executeOnNeo4j(
                    ModelUtils.CYPHER_DELETE_ALL,
                    true);
        } catch (Exception e) {
            LOG.error("Error executing cypher: " + ModelUtils.CYPHER_DELETE_ALL + ", " + e.getMessage());
        }
        neo4jDirectConnect = null;
    }


    public static POutput castRowsWriteNeo4j(JobSpecRequest jobSpec,
                                             ConnectionParams neoConnection, Target target, PCollection<Row> sqlTransformedSource) {

        // The Neo4j driver configuration
        final Neo4jIO.DriverConfiguration driverConfiguration =
                Neo4jIO.DriverConfiguration.create(neoConnection.serverUrl, neoConnection.username, neoConnection.password)
                        .withConfig(Config.defaultConfig());

        final SessionConfig sessionConfig = SessionConfig.builder()
                .withDatabase(neoConnection.database)
                .build();
        /////////////////////////////////
        // Target schema transform
        final Schema targetSchema = BeamSchemaUtils.toBeamSchema(target);
        final DoFn<Row, Row> castToTargetRow = new CastTargetStringRowFn(target, targetSchema);
        PCollection<Row> targetRowsCollection = sqlTransformedSource.apply(target.sequence + ": Cast " + target.name + " rows", ParDo.of(castToTargetRow));
        targetRowsCollection.setCoder(SchemaCoder.of(targetSchema));
        targetRowsCollection.setRowSchema(targetSchema);

        // indices and constraints
        List<String> cyphers = CypherGenerator.getNodeIndexAndConstraintsCypherStatements(target);
        if (cyphers.size() > 0) {
            DirectConnect neo4jDirectConnect = new DirectConnect(neoConnection.serverUrl, neoConnection.database, neoConnection.username, neoConnection.password);
            LOG.info("Resetting database");

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

        return targetRowsCollection.apply(target.sequence + ": Neo4j write " + target.name, writeNeo4jIO);
    }
}
