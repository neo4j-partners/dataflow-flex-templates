package com.google.cloud.teleport.v2.neo4j.common.transforms;


import com.google.cloud.teleport.v2.neo4j.common.database.CypherGenerator;
import com.google.cloud.teleport.v2.neo4j.common.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.common.model.Config;
import com.google.cloud.teleport.v2.neo4j.common.model.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.common.utils.DataCastingUtils;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class Neo4jRowWriterTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
    private static final Logger LOG = LoggerFactory.getLogger(Neo4jRowWriterTransform.class);

    JobSpecRequest jobSpec;
    ConnectionParams neoConnection;
    Target target;


    public Neo4jRowWriterTransform(JobSpecRequest jobSpec, ConnectionParams neoConnection, Target target) {
        this.jobSpec = jobSpec;
        this.neoConnection = neoConnection;
        this.target = target;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {

        Config config=jobSpec.config;
        // indices and constraints
        List<String> cyphers = CypherGenerator.getNodeIndexAndConstraintsCypherStatements(config, target);
        if (cyphers.size() > 0) {
            Neo4jConnection neo4jDirectConnect = new Neo4jConnection(neoConnection);
            LOG.info("Adding " + cyphers.size() + " indices and constraints");
            for (String cypher : cyphers) {
                LOG.info("Executing cypher: " + cypher);
                try {
                    neo4jDirectConnect.executeCypher(
                            cypher);
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

        /*
        final Neo4jIO.DriverConfiguration driverConfiguration =
                Neo4jIO.DriverConfiguration.create(neoConnection.serverUrl, neoConnection.username, neoConnection.password)
                        .withConfig(Config.defaultConfig());

        final SessionConfig sessionConfig = SessionConfig.builder()
                .withDatabase(neoConnection.database)
                .build();

        // This IO is not blocking since it currently does not return PCollection<Row>
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
        */

        Neo4jConnection neo4jConnection = new Neo4jConnection(neoConnection);
        Row emptyRow=Row.nullRow(input.getSchema());

        Neo4jBlockingUnwindFn neo4jUnwindFn =
                new Neo4jBlockingUnwindFn
                        (
                                neo4jConnection,
                                emptyRow,
                                unwindCypher,
                                batchSize,
                                false,
                                "rows",
                                getRowCastingFunction()
                        );

        PCollection<Row> output = input.apply(target.sequence + ": Neo4j write " + target.name, ParDo.of(neo4jUnwindFn)).setCoder(RowCoder.of(input.getSchema()));
        return output;
    }

    private SerializableFunction<Row, Map<String, Object>> getRowCastingFunction() {
        return (row) -> {
            return DataCastingUtils.rowToNeo4jDataMap(row, target);
        };
    }

}
