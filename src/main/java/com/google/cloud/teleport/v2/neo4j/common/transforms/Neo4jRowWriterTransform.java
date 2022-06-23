package com.google.cloud.teleport.v2.neo4j.common.transforms;

import com.google.cloud.teleport.v2.neo4j.common.database.CypherGenerator;
import com.google.cloud.teleport.v2.neo4j.common.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.common.model.Config;
import com.google.cloud.teleport.v2.neo4j.common.model.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.AvroType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.common.utils.AvroSinkWithJodaDatesConversion;
import com.google.cloud.teleport.v2.neo4j.common.utils.BeamUtils;
import com.google.cloud.teleport.v2.neo4j.common.utils.DataCastingUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.Sink;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
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

        if (!StringUtils.isEmpty(jobSpec.config.auditGsUri)) {

            String auditFilePath = jobSpec.config.auditGsUri ;
            //audit
            org.apache.avro.Schema targetAvroSchema = AvroUtils.toAvroSchema(BeamUtils.toBeamSchema(target));

            Sink<GenericRecord> sink;
            if (jobSpec.config.avroType==AvroType.parquet) {
                sink = ParquetIO.sink(targetAvroSchema).withCompressionCodec(CompressionCodecName.SNAPPY);
            } else if (jobSpec.config.avroType==AvroType.avro) {
                sink = new AvroSinkWithJodaDatesConversion<>(targetAvroSchema);
            } else {
                    throw new UnsupportedOperationException(
                            "Output format is not implemented: " + jobSpec.config.avroType);
            }

            PCollection<GenericRecord> genericInput=input.apply("Row to Generic record",
                            MapElements.into(new TypeDescriptor<GenericRecord>() {}).via(AvroUtils.getRowToGenericRecordFunction(targetAvroSchema)))
                    .setCoder(AvroCoder.of(GenericRecord.class, targetAvroSchema)) ;
            genericInput.apply(target.sequence + ": Logging " + target.name,
                    FileIO.<GenericRecord>write()
                    .via(sink)
                        .to(auditFilePath)
                        .withPrefix( input.getPipeline().getOptions().getJobName())
                        .withSuffix("."+jobSpec.config.avroType)
            );
        }
        return output;
    }

    private SerializableFunction<Row, Map<String, Object>> getRowCastingFunction() {
        return (row) -> {
            return DataCastingUtils.rowToNeo4jDataMap(row, target);
        };
    }


}
