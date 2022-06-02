package com.google.cloud.teleport.v2.neo4j.common;


import com.google.cloud.teleport.v2.neo4j.common.model.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.Targets;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.common.transforms.CloneFn;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.io.neo4j.Neo4jIO;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.neo4j.driver.Config;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.extensions.sql.*;

import java.util.List;

public class Neo4JTargetWriter {
    private static final Logger LOG = LoggerFactory.getLogger(Neo4JTargetWriter.class);
    
    public static void writeTargets(JobSpecRequest jobSpec,
                                    ConnectionParams neoConnection,
                                    Schema sourceSchema,
                                    PCollection<Row> sourceRowsCollection){

        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        // NEO4J setup
        LOG.info("Writing "+jobSpec.targets.size()+" targets to Neo4j");
        // The Neo4j driver configuration
        final Neo4jIO.DriverConfiguration driverConfiguration =
                Neo4jIO.DriverConfiguration.create(neoConnection.serverUrl, neoConnection.username, neoConnection.password)
                        .withConfig(Config.defaultConfig());

        final SessionConfig sessionConfig=SessionConfig.builder()
                .withDatabase(neoConnection.database)
                .build();

        // Direct connect utility...
        Neo4jDirectConnect neo4jDirectConnect = new Neo4jDirectConnect(neoConnection.serverUrl, neoConnection.database, neoConnection.username, neoConnection.password);

        if (jobSpec.config.resetDb){
            LOG.info("Resetting database");
            try {
                LOG.info("Executing cypher: " + Neo4jUtils.CYPHER_DELETE_ALL);
                neo4jDirectConnect.executeOnNeo4j(
                        Neo4jUtils.CYPHER_DELETE_ALL,
                        true);
            } catch (Exception e) {
                LOG.error("Error executing cypher: " + Neo4jUtils.CYPHER_DELETE_ALL + ", " + e.getMessage());
            }
        }

        // Now write these rows to Neo4j Customer nodes
        int targetNum=0;
        for (Targets target : jobSpec.targets) {
            if (target.active) {
                String targetName=target.name;
                targetNum++;
                if (StringUtils.isEmpty(targetName)){
                    targetName = "Target "+targetNum;
                }
                LOG.info("==================================================");
                LOG.info("Writing target "+targetName+": "+ gson.toJson(target));

                String SQL = Neo4jUtils.getTargetSql( sourceSchema, target);

                PCollection<Row> sqlTransformedSource = null;
                // conditionally apply sql to rows..
                if(!SQL.equals(Neo4jUtils.DEFAULT_STAR_QUERY)){
                    LOG.info("Applying SQL transformation to "+targetName+": "+SQL);
                    sqlTransformedSource = sourceRowsCollection.apply(targetNum+": SQLTransform "+targetName,  SqlTransform.query(SQL));
                } else {
                    LOG.info("Skipping SQL transformation to "+targetName);
                    final DoFn<Row,  Row> cloneFn = new CloneFn(sourceSchema);
                    sqlTransformedSource = sourceRowsCollection.apply(targetNum+": Cloning: "+targetName, ParDo.of(cloneFn));
                }

                /////////////////////////////////
                // Target schema transform
                final Schema targetSchema = BeamSchemaUtils.toNeo4jTargetSchema(target);
                final DoFn<Row,  Row> castToTargetRow = new CastTargetRowFn(target, targetSchema);
                PCollection<Row> targetRowsCollection = sqlTransformedSource.apply(targetNum+": Cast "+targetName+" rows", ParDo.of(castToTargetRow));
                targetRowsCollection.setCoder(SchemaCoder.of(targetSchema));
                targetRowsCollection.setRowSchema(targetSchema);

                // indices and constraints
                List<String> cyphers = CypherGenerator.getNodeIndexAndConstraintsCypherStatements(target);
                for (String cypher : cyphers) {
                    LOG.info("Executing cypher: " + cypher);
                    try {
                        neo4jDirectConnect.executeOnNeo4j(
                                cypher,
                                true);
                    } catch (Exception e) {
                        LOG.error("Error executing cypher: " + cypher+", "+e.getMessage());
                    }
                }

                //set batch sizes
                int batchSize=jobSpec.config.nodeBatchSize;
                int parallelism=jobSpec.config.nodeParallelism;

                if (target.type== TargetType.relationship){
                    batchSize=jobSpec.config.edgeBatchSize;
                    parallelism=jobSpec.config.edgeParallelism;
                }

                // data loading
                String unwindCypher = CypherGenerator.getUnwindCreateCypher(target);
                LOG.info("Unwind cypher: "+unwindCypher);
                /////////////////////////
                // Batch load data rows using Matt's Neo4jIO connector
                /////////////////////////
                final PTransform writeNeo4jIO=Neo4jIO.<Row>writeUnwind()
                        .withCypher(unwindCypher)
                        .withBatchSize(batchSize)
                        .withSessionConfig(sessionConfig)
                        .withUnwindMapName("rows")
                        .withParametersFunction(
                                row -> {
                                    return CypherGenerator.getUnwindRowDataMapper(row, target);
                                })
                        .withDriverConfiguration(driverConfiguration);

                targetRowsCollection.apply(targetNum+": Neo4j write "+targetName, writeNeo4jIO);
                //TODO: not sure how to do parallelism here
                /*
                if (parallelism>1){
                    targetRowsCollection.apply("Neo4j "+targetName, ParDo.of(writeNeo4jIO));
                } else {
                    targetRowsCollection.apply("Neo4j "+targetName, writeNeo4jIO);
                }
                 */
            } else {
                LOG.info("Target is inactive: "+target.name);
            }
        }
    }
}
