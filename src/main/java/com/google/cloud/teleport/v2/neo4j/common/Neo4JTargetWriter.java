package com.google.cloud.teleport.v2.neo4j.common;


import com.google.cloud.teleport.v2.neo4j.common.model.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.Targets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.io.neo4j.Neo4jIO;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.neo4j.driver.Config;
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

        // Direct connect utility...
        Neo4jDirectConnect neo4jDirectConnect = new Neo4jDirectConnect(neoConnection.serverUrl, neoConnection.database, neoConnection.username, neoConnection.password);

        if (jobSpec.commands.resetDb){
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
        int targetNum=1;
        for (Targets target : jobSpec.targets) {
            if (target.active) {
                String targetName=target.name;
                if (StringUtils.isEmpty(targetName)){
                    targetName = "Target "+targetNum;
                }
                targetNum++;
                LOG.info("Writing target "+targetName+": "+ gson.toJson(target));

                String SQL = Neo4jUtils.getTargetSql( sourceSchema, target);
                LOG.info("SQL: "+SQL);

                PCollection<Row> sqlTransformedSource = null;
                // conditionally apply sql to rows..
                if(!SQL.equals(Neo4jUtils.DEFAULT_STAR_QUERY)){
                    LOG.info("Applying SQL transformation to "+targetName);
                    sqlTransformedSource = sourceRowsCollection.apply("Applying SQL to "+targetName,  SqlTransform.query(SQL));
                    sqlTransformedSource.setCoder(SerializableCoder.of(Row.class));
                } else {
                    LOG.info("Skipping SQL transformation to "+targetName);
                    //TODO: a simple clone transform would work here, how is that done?
                    sqlTransformedSource = sourceRowsCollection.apply("Applying SQL to "+targetName,  SqlTransform.query(SQL));
                    sqlTransformedSource.setCoder(SerializableCoder.of(Row.class));
                }

                /////////////////////////////////
                // Target schema transform
                final Schema targetSchema = BeamSchemaUtils.toNeo4jTargetSchema(target);
                final DoFn<Row,  Row> castToTargetRow = new CastTargetRowFn(target, targetSchema);
                PCollection<Row> targetRowsCollection = sqlTransformedSource.apply("Cast "+targetName+" rows", ParDo.of(castToTargetRow));
                targetRowsCollection.setCoder(SerializableCoder.of(Row.class));
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

                // data loading
                String unwindCypher = CypherGenerator.getUnwindCreateCypher(target);
                LOG.info("Unwind cypher: "+unwindCypher);
                /////////////////////////
                // Batch load data rows using Matt's Neo4jIO connector
                /////////////////////////
                targetRowsCollection.apply(
                        Neo4jIO.<Row>writeUnwind()
                                .withCypher(unwindCypher)
                                .withBatchSize(5000)
                                .withUnwindMapName("rows")
                                .withParametersFunction(
                                        row -> {
                                            return CypherGenerator.getUnwindRowDataMapper(row,target);
                                        })
                                .withDriverConfiguration(driverConfiguration));
            } else {
                LOG.info("Target is inactive: "+target.name);
            }
        }
    }
}
