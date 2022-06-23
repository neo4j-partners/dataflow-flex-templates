package com.google.cloud.teleport.v2.neo4j.common.transforms;

import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.AvroType;
import com.google.cloud.teleport.v2.neo4j.common.utils.AvroSinkWithJodaDatesConversion;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsLogTransform extends PTransform<PCollection<Row>, POutput> {

    private static final Logger LOG = LoggerFactory.getLogger(GcsLogTransform.class);

    JobSpecRequest jobSpec;
    Target target;

    public GcsLogTransform(JobSpecRequest jobSpecRequest, Target target) {
        this.target = target;
        this.jobSpec = jobSpecRequest;
    }


    @Override
    public POutput expand(PCollection<Row> input) {

        String auditFilePath = jobSpec.config.auditGsUri;
        if (!StringUtils.endsWith(auditFilePath,"/")){
            auditFilePath+="/";
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
        LOG.info("Logging to "+auditFilePath+" with prefix: "+input.getPipeline().getOptions().getJobName());
        PCollection<GenericRecord> genericInput = input.apply(target.sequence + ": Log xform " + target.name,
                        MapElements.into(new TypeDescriptor<GenericRecord>() {
                        }).via(AvroUtils.getRowToGenericRecordFunction(targetAvroSchema)))
                .setCoder(AvroCoder.of(GenericRecord.class, targetAvroSchema));
        return genericInput.apply(target.sequence + ": Log write " + target.name,
                FileIO.<GenericRecord>write()
                        .via(sink)
                        .to(auditFilePath)
                        .withPrefix(input.getPipeline().getOptions().getJobName())
                        .withSuffix("." + jobSpec.config.avroType)
        );

    }
}
