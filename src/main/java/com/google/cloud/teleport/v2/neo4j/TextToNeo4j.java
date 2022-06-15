package com.google.cloud.teleport.v2.neo4j;

import com.google.cloud.teleport.v2.neo4j.common.InputValidator;
import com.google.cloud.teleport.v2.neo4j.common.database.TargetWriter;
import com.google.cloud.teleport.v2.neo4j.common.model.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.Source;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import com.google.cloud.teleport.v2.neo4j.text.options.TextToNeo4jImportOptions;
import com.google.cloud.teleport.v2.neo4j.text.transforms.LineParsingFn;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
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

import java.util.List;

public class TextToNeo4j {

    private static final Logger LOG = LoggerFactory.getLogger(TextToNeo4j.class);
    ConnectionParams neo4jConnection;
    JobSpecRequest jobSpec;
    String dataFileUri;
    Pipeline pipeline;

    public static void main(final String[] args) {
        final TextToNeo4jImportOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation()
                        .as(TextToNeo4jImportOptions.class);

        LOG.info("Job: " + options.getJobName());
        final TextToNeo4j fileToNeo4j = new TextToNeo4j(options);
        fileToNeo4j.run();
    }


    public TextToNeo4j(final TextToNeo4jImportOptions pipelineOptions) {
        ////////////////////////////
        // We need to initialize pipeline in order to create context for Gs and Bq file system
        final String jobName = pipelineOptions.getJobName() + "-" + System.currentTimeMillis();
        pipelineOptions.setJobName(jobName);
        this.pipeline = Pipeline.create(pipelineOptions);
        FileSystems.setDefaultPipelineOptions(pipelineOptions);


        List<String> validationMessages = InputValidator.validateNeo4jPipelineOptions(pipelineOptions);
        if (validationMessages.size() > 0) {
            for (String msg : validationMessages) {
                LOG.error(msg);
            }
            throw new RuntimeException("Errors found validating pipeline input.  Please see logs for more details.");
        }

        this.neo4jConnection = new ConnectionParams(pipelineOptions.getNeo4jConnectionUri());
        this.jobSpec = new JobSpecRequest(pipelineOptions.getJobSpecUri());

        InputValidator.refactorJobSpec(jobSpec);

        ///////////////////////////////////
        // Text input specific options and validation
        if (this.jobSpec.source == null) {
            String errMsg = "JobSpec source is required for text imports.";
            LOG.error(errMsg);
            throw new RuntimeException(errMsg);
        }
        if (!StringUtils.isEmpty(pipelineOptions.getInputFilePattern())) {
            this.dataFileUri = pipelineOptions.getInputFilePattern();
        } else if (!StringUtils.isEmpty(this.jobSpec.source.textFile.uri)) {
            this.dataFileUri = this.jobSpec.source.textFile.uri;
            LOG.info("Using source from jobSpec dataFile: " + this.dataFileUri);
        } else {
            String errMsg = "Could not determine source data file.";
            LOG.error(errMsg);
            throw new RuntimeException(errMsg);
        }

    }

    public void run() {

        final Gson gson = new GsonBuilder().setPrettyPrinting().create();

        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

        Source dataSource = this.jobSpec.source;

        LOG.info("Using data file: " + this.dataFileUri);

        final PBegin begin = pipeline.begin();
        // Read all the lines from a file

        ////////////////////////////
        // Text file import
        final PCollection<String> linesCollection =
                begin.apply("Read " + dataSource.name + " data: " + this.dataFileUri,
                        TextIO.read().from(this.dataFileUri));

        final Schema beamSchema = dataSource.textFile.getTextFileSchemaData();
        LOG.info("Source schema field count: " + beamSchema.getFieldCount() + ", fields: " + StringUtils.join(beamSchema.getFieldNames(), ","));
        final DoFn<String, Row> lineToRow = new LineParsingFn(dataSource, beamSchema, dataSource.textFile.csvFormat);
        final PCollection<Row> beamRows = linesCollection.apply("Parse lines into string columns.", ParDo.of(lineToRow));
        beamRows.setCoder(SerializableCoder.of(Row.class));
        beamRows.setRowSchema(beamSchema);

        ////////////////////////////
        // Reset db
        if (jobSpec.config.resetDb) {
            TargetWriter.resetNeo4j(this.neo4jConnection);
        }

        ////////////////////////////
        // Write neo4j
        LOG.info("Found " + jobSpec.targets.size() + " candidate targets");

        PCollection<Void> waitOnCollection=null;
        // Now write these rows to Neo4j Customer nodes
        for (Target target : jobSpec.targets) {
            if (target.active) {
                LOG.info("Writing target " + target.name + ": " + gson.toJson(target));
                String SQL = ModelUtils.getTargetSql(ModelUtils.getBeamFieldSet(beamSchema), target,false);
                //https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/src/main/java/com/google/cloud/teleport/splunk/SplunkEventWriter.java
                // conditionally apply sql to rows..
                if (!SQL.equals(ModelUtils.DEFAULT_STAR_QUERY)) {
                    LOG.info("Applying SQL transformation to " + target.name + ": " + SQL);
                    PCollection<Row> sqlTransformedSource = beamRows.apply(target.sequence + ": SQLTransform " + target.name, SqlTransform.query(SQL));
                    waitOnCollection=TargetWriter.castRowsWriteNeo4j(waitOnCollection,jobSpec, neo4jConnection, target, sqlTransformedSource);
                    //serializing Neo4j operations

                } else {
                    LOG.info("Skipping SQL transformation for " + target.name);
                    waitOnCollection=TargetWriter.castRowsWriteNeo4j(waitOnCollection,jobSpec, neo4jConnection, target, beamRows);
                    //serializing Neo4j operations
                }

            } else {
                LOG.info("Target is inactive, not processing: " + target.name);
            }

        }
        // For a Dataflow Flex Template, do NOT waitUntilFinish().
        pipeline.run();
    }
}