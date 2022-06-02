package com.google.cloud.teleport.v2.neo4j.text;

import com.google.cloud.teleport.v2.neo4j.common.JobSpecOptimizer;
import com.google.cloud.teleport.v2.neo4j.common.Neo4JTargetWriter;
import com.google.cloud.teleport.v2.neo4j.common.Validations;
import com.google.cloud.teleport.v2.neo4j.common.model.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.Source;
import com.google.cloud.teleport.v2.neo4j.text.options.TextToNeo4jImportOptions;
import com.google.cloud.teleport.v2.neo4j.text.transforms.LineParsingFn;
import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

public class TextToNeo4j {


    private static final Logger LOG = LoggerFactory.getLogger(TextToNeo4j.class);


    TextToNeo4jImportOptions pipelineOptions;
    ConnectionParams neoConnection;
    JobSpecRequest jobSpec;
    String dataFileUri;

    public static void main(final String[] args) {
        final TextToNeo4jImportOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation()
                        .as(TextToNeo4jImportOptions.class);

        LOG.info("Job: " + options.getJobName());
        final TextToNeo4j fileToNeo4j = new TextToNeo4j(options);
        fileToNeo4j.run();
    }


    public TextToNeo4j(final TextToNeo4jImportOptions pipelineOptions) {
        this.pipelineOptions = pipelineOptions;
    }

    public void run() {

        final Gson gson = new Gson();

        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

        // Execute a pipeline on Dataflow
        Pipeline pipeline = Pipeline.create(pipelineOptions);
        FileSystems.setDefaultPipelineOptions(pipelineOptions);

        final String jobName = pipelineOptions.getJobName() + "-" + System.currentTimeMillis();
        pipelineOptions.setJobName(jobName);

        // Create a pipeline using the options

        //TODO: check if connectionUri provided
        final String neoConnectionUri = pipelineOptions.getNeo4jConnectionUri();
        this.neoConnection = new ConnectionParams(neoConnectionUri);
        LOG.info("NeoConnection: " + gson.toJson(this.neoConnection));

        if (StringUtils.isEmpty(pipelineOptions.getJobSpecUri())) {
            throw new RuntimeException("Job spec URI not provided.");
        }

        final String jobSpecUri = pipelineOptions.getJobSpecUri();
        this.jobSpec = new JobSpecRequest(jobSpecUri);
        LOG.info("JobSpec: " + gson.toJson(this.jobSpec));

        Validations validations = JobSpecOptimizer.validateAndOptimize(jobSpec);
        if (validations.errors) {
            for (String msg : validations.validationMessages) {
                LOG.error(msg);
            }
            throw new RuntimeException("Errors found validating jobSpec.  Please see logs for more details.");
        }


        if (this.jobSpec.source == null) {
            throw new RuntimeException("Jobspec source is required for text imports.");
        }
        Source dataSource = this.jobSpec.source;

        if (!StringUtils.isEmpty(pipelineOptions.getInputFilePattern())) {
            this.dataFileUri = pipelineOptions.getInputFilePattern();
        } else if (!StringUtils.isEmpty(dataSource.textFile.uri)) {
            this.dataFileUri = dataSource.textFile.uri;
            LOG.info("Override jobSpec dataFile: " + this.dataFileUri);
        } else {
            LOG.error("Could not determine data file.");
            throw new RuntimeException("Could not determine data file");
        }

        LOG.info("Using data file: " + this.dataFileUri);

        //////// new stuff start
        LOG.info("Data pipeline options: " + pipelineOptions);

        final PBegin begin = pipeline.begin();
        // Read all the lines from a file

        //////////////
        // Text file import setup
        final PCollection<String> linesCollection =
                begin.apply("Read " + dataSource.name + " data: " + this.dataFileUri,
                        TextIO.read().from(this.dataFileUri));

        final Schema sourceSchema = dataSource.textFile.getTextFileSchemaData();
        LOG.info("Source schema field count: " + sourceSchema.getFieldCount()+", fields: "+StringUtils.collectionToCommaDelimitedString(sourceSchema.getFieldNames()));
        final DoFn<String, Row> lineToRow = new LineParsingFn(dataSource, sourceSchema, dataSource.textFile.csvFormat);
        final PCollection<Row> sourceRowsCollection = linesCollection.apply("Parse lines into string columns.", ParDo.of(lineToRow));
        sourceRowsCollection.setCoder(SerializableCoder.of(Row.class));
        sourceRowsCollection.setRowSchema(sourceSchema);

        // we have the source now write targets
        Neo4JTargetWriter.writeTargets(jobSpec,
                neoConnection,
                sourceSchema,
                sourceRowsCollection);

        // Now run this using the dataflow runner
        final DataflowRunner dataflowRunner = DataflowRunner.fromOptions(pipelineOptions);
        final DataflowPipelineJob job = dataflowRunner.run(pipeline);
        final PipelineResult.State state = job.waitUntilFinish();
        LOG.info("Final state : " + state);
    }

}