package com.google.cloud.teleport.v2.neo4j.common.database;


import com.google.cloud.teleport.v2.neo4j.common.model.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.common.transforms.CastTargetStringRowFn;
import com.google.cloud.teleport.v2.neo4j.common.utils.BeamSchemaUtils;
import com.google.cloud.teleport.v2.neo4j.common.utils.DataCastingUtils;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.sdk.io.neo4j.Neo4jIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.neo4j.driver.Config;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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


    public static PCollection<Void>  castRowsWriteNeo4j( PCollection<Void> waitOnCollection, JobSpecRequest jobSpec,
                                                        ConnectionParams neoConnection, Target target, PCollection<Row> untypedSource) {
        /////////////////////////////////
        // Target schema transform
        final Schema targetSchema = BeamSchemaUtils.toBeamSchema(target);
        final DoFn<Row, Row> castToTargetRow = new CastTargetStringRowFn(target, targetSchema);
        PCollection<Row> typedSource;
        if (waitOnCollection!=null){
            //This Wait.on is causing an error:
            /*
[ERROR] Failed to execute goal org.codehaus.mojo:exec-maven-plugin:3.0.0:java (default-cli) on project googlecloud-to-neo4j: An exception occured while executing the Java class. Unable to return a default Coder for Wait.OnSignal/Wait/Map/ParMultiDo(Anonymous).output [PCollection@433060791]. Correct one of the following root causes:
[ERROR]   No Coder has been manually specified;  you may do so using .setCoder().
[ERROR]   Inferring a Coder from the CoderRegistry failed: Cannot provide a coder for a Beam Row. Please provide a schema instead using PCollection.setRowSchema.
[ERROR]   Using the default output Coder from the producing PTransform failed: PTransform.getOutputCoder called.
[ERROR] -> [Help 1]
             */
            typedSource = untypedSource.apply(Wait.on(waitOnCollection)).apply(target.sequence + ": Cast " + target.name + " rows", ParDo.of(castToTargetRow));
            typedSource.setRowSchema(targetSchema);
        } else {
            typedSource = untypedSource.apply(target.sequence + ": Cast " + target.name + " rows", ParDo.of(castToTargetRow));
            typedSource.setRowSchema(targetSchema);
        }
        POutput output= writeNeo4j( jobSpec, neoConnection,  target, typedSource);
        //we're not actually delaying until the writeNeo4j is done because there is no idiom for this in DataFlow
        return typedSource.apply(MapElements.into(TypeDescriptors.voids()).via(whatever -> (Void) null));
    }

    private static POutput writeNeo4j( JobSpecRequest jobSpec,
                                                        ConnectionParams neoConnection, Target target, PCollection<Row> typedSource) {

        // The Neo4j driver configuration
        final Neo4jIO.DriverConfiguration driverConfiguration =
                Neo4jIO.DriverConfiguration.create(neoConnection.serverUrl, neoConnection.username, neoConnection.password)
                        .withConfig(Config.defaultConfig());

        final SessionConfig sessionConfig = SessionConfig.builder()
                .withDatabase(neoConnection.database)
                .build();
        // indices and constraints
        List<String> cyphers = CypherGenerator.getNodeIndexAndConstraintsCypherStatements(target);
        if (cyphers.size() > 0) {
            DirectConnect neo4jDirectConnect = new DirectConnect(neoConnection.serverUrl, neoConnection.database, neoConnection.username, neoConnection.password);
            LOG.info("Adding indices and constraints");
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

        return typedSource.apply(target.sequence + ": Neo4j write " + target.name, writeNeo4jIO);

    }
}
