package com.google.cloud.teleport.v2.neo4j.transforms;

import com.google.cloud.teleport.v2.neo4j.model.enums.AvroType;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.utils.AvroSinkWithJodaDatesConversion;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Log rows to parquet.
 */
public class GcsLogTransform extends PTransform<PCollection<Row>, POutput> {

    private static final Logger LOG = LoggerFactory.getLogger(GcsLogTransform.class);

    JobSpec jobSpec;
    Target target;

    public GcsLogTransform(JobSpec jobSpecRequest, Target target) {
        this.target = target;
        this.jobSpec = jobSpecRequest;
    }


    @Override
    public POutput expand(PCollection<Row> input) {

        String auditFilePath = jobSpec.config.auditGsUri;
        if (!StringUtils.endsWith(auditFilePath, "/")) {
            auditFilePath += "/";
        }
        //audit
        org.apache.avro.Schema targetAvroSchema = AvroUtils.toAvroSchema(input.getSchema());

        FileIO.Sink<GenericRecord> sink;
        if (jobSpec.config.avroType == AvroType.parquet) {
            sink = ParquetIO.sink(targetAvroSchema).withCompressionCodec(CompressionCodecName.SNAPPY);
        } else if (jobSpec.config.avroType == AvroType.avro) {
            sink = new AvroSinkWithJodaDatesConversion<>(targetAvroSchema);
        } else {
            throw new UnsupportedOperationException(
                    "Output format is not implemented: " + jobSpec.config.avroType);
        }
        LOG.info("Logging to " + auditFilePath + " with prefix: " + input.getPipeline().getOptions().getJobName());
        PCollection<GenericRecord> genericInput = input.apply(target.sequence + ": Log xform " + target.name,
                Convert.to(GenericRecord.class));
        return genericInput.apply(target.sequence + ": Log write " + target.name,
                FileIO.<GenericRecord>write()
                        .via(sink)
                        .to(auditFilePath)
                        .withPrefix(input.getPipeline().getOptions().getJobName())
                        .withSuffix("." + jobSpec.config.avroType)
        );

    }
}
