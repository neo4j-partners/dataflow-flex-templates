package com.google.cloud.teleport.v2.neo4j.common.transforms;

import com.google.common.base.MoreObjects;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Create KvTransform to control Beam parallelism.
 */
public class CreateKvTransform
        extends PTransform<PCollection<Row>, PCollection<KV<Integer, Row>>> {

    private static final Logger LOG = LoggerFactory.getLogger(CreateKvTransform.class);
    private static final Integer DEFAULT_PARALLELISM = 1;
    private final Integer requestedKeys;

    private CreateKvTransform(Integer requestedKeys) {
        this.requestedKeys = requestedKeys;
    }

    public static CreateKvTransform of(Integer requestedKeys) {
        return new CreateKvTransform(requestedKeys);
    }

    @Override
    public PCollection<KV<Integer, Row>> expand(PCollection<Row> input) {
        return input
                .apply("Inject Keys", ParDo.of(new CreateKeysFn(this.requestedKeys)))
                .setCoder(KvCoder.of(BigEndianIntegerCoder.of(), input.getCoder()));
    }

    private class CreateKeysFn extends DoFn<Row, KV<Integer, Row>> {
        private final Integer specifiedParallelism;
        private Integer calculatedParallelism;

        CreateKeysFn(Integer specifiedParallelism) {
            this.specifiedParallelism = specifiedParallelism;
        }

        @Setup
        public void setup() {

            if (calculatedParallelism == null) {

                if (specifiedParallelism != null) {
                    calculatedParallelism = specifiedParallelism;
                }

                calculatedParallelism = MoreObjects.firstNonNull(calculatedParallelism, DEFAULT_PARALLELISM);

                LOG.info("Parallelism set to: {}", calculatedParallelism);
            }
        }

        @ProcessElement
        public void processElement(ProcessContext context) {
            context.output(
                    KV.of(ThreadLocalRandom.current().nextInt(calculatedParallelism), context.element()));
        }
    }
}
