package com.google.cloud.teleport.v2.neo4j.transforms;

import com.google.cloud.teleport.v2.neo4j.database.CypherGenerator;
import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.Config;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Neo4j write transformation.
 */
public class Neo4jRowWriterTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
    private static final Logger LOG = LoggerFactory.getLogger(Neo4jRowWriterTransform.class);
    JobSpec jobSpec;
    ConnectionParams neoConnection;
    Target target;

    public Neo4jRowWriterTransform(JobSpec jobSpec, ConnectionParams neoConnection, Target target) {
        this.jobSpec = jobSpec;
        this.neoConnection = neoConnection;
        this.target = target;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {

        Config config = jobSpec.config;
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

        if (target.type == TargetType.edge) {
            batchSize = jobSpec.config.edgeBatchSize;
            parallelism = jobSpec.config.edgeParallelism;
        }

        // data loading
        String unwindCypher = CypherGenerator.getUnwindCreateCypher(target);
        LOG.info("Unwind cypher: " + unwindCypher);

        Neo4jConnection neo4jConnection = new Neo4jConnection(neoConnection);
        Row emptyRow = Row.nullRow(input.getSchema());

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

        PCollection<Row> output = input
                .apply("Create KV pairs", CreateKvTransform.of(parallelism))
                .apply(target.sequence + ": Neo4j write " + target.name, ParDo.of(neo4jUnwindFn))
                .setRowSchema(input.getSchema());

        return output;
    }

    private SerializableFunction<Row, Map<String, Object>> getRowCastingFunction() {
        return (row) -> {
            return DataCastingUtils.rowToNeo4jDataMap(row, target);
        };
    }


}
