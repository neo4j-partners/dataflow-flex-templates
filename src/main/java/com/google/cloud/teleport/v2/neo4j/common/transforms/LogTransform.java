package com.google.cloud.teleport.v2.neo4j.common.transforms;

import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.AvroType;
import com.google.cloud.teleport.v2.neo4j.common.utils.AvroSinkWithJodaDatesConversion;
import com.google.cloud.teleport.v2.neo4j.common.utils.BeamUtils;
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
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class LogTransform extends PTransform<PCollection<Row>, POutput> {

    JobSpecRequest jobSpec;
    Target target;

    public LogTransform(JobSpecRequest jobSpecRequest, Target target) {
        this.target = target;
        this.jobSpec = jobSpecRequest;
    }


    @Override
    public POutput expand(PCollection<Row> input) {

        String auditFilePath = jobSpec.config.auditGsUri;
        //audit
        org.apache.avro.Schema targetAvroSchema = AvroUtils.toAvroSchema(BeamUtils.toBeamSchema(target));

        FileIO.Sink<GenericRecord> sink;
        if (jobSpec.config.avroType == AvroType.parquet) {
            sink = ParquetIO.sink(targetAvroSchema).withCompressionCodec(CompressionCodecName.SNAPPY);
        } else if (jobSpec.config.avroType == AvroType.avro) {
            sink = new AvroSinkWithJodaDatesConversion<>(targetAvroSchema);
        } else {
            throw new UnsupportedOperationException(
                    "Output format is not implemented: " + jobSpec.config.avroType);
        }

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
