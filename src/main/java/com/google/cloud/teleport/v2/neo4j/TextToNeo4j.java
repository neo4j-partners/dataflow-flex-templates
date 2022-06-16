package com.google.cloud.teleport.v2.neo4j;

import com.google.cloud.teleport.v2.neo4j.common.InputOptimizer;
import com.google.cloud.teleport.v2.neo4j.common.InputValidator;
import com.google.cloud.teleport.v2.neo4j.common.database.DirectConnect;
import com.google.cloud.teleport.v2.neo4j.common.transforms.Neo4jRowWriterTransform;
import com.google.cloud.teleport.v2.neo4j.common.model.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.Source;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.SourceType;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import com.google.cloud.teleport.v2.neo4j.text.options.TextToNeo4jImportOptions;
import com.google.cloud.teleport.v2.neo4j.text.transforms.LineToRowFn;
import com.google.cloud.teleport.v2.neo4j.text.transforms.StringListToRowFn;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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

        InputOptimizer.refactorJobSpec(jobSpec);

        ///////////////////////////////////
        // Text input specific options and validation
        if (this.jobSpec.source == null) {
            String errMsg = "JobSpec source is required for text imports.";
            LOG.error(errMsg);
            throw new RuntimeException(errMsg);
        }
        if (this.jobSpec.source.sourceType != SourceType.text) {
            String errMsg = "Currently unhandled source type: " + this.jobSpec.source.sourceType;
            LOG.error(errMsg);
            throw new RuntimeException(errMsg);
        }
        if (!StringUtils.isEmpty(pipelineOptions.getInputFilePattern())) {
            this.dataFileUri = pipelineOptions.getInputFilePattern();
        } else if (!StringUtils.isEmpty(this.jobSpec.source.uri)) {
            this.dataFileUri = this.jobSpec.source.uri;
            LOG.info("Using source from jobSpec dataFile: " + this.dataFileUri);
        } else if (this.jobSpec.source.inline != null) {
            LOG.info("Using inline data.");
        } else {
            String errMsg = "Could not determine source data file.";
            LOG.error(errMsg);
            throw new RuntimeException(errMsg);
        }

    }

    public void run() {

        final Gson gson = new GsonBuilder().setPrettyPrinting().create();

        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

        Source source = this.jobSpec.source;

        LOG.info("Using data file: " + this.dataFileUri);

        final PBegin begin = pipeline.begin();
        // Read all the lines from a file

        ////////////////////////////
        // Text file import
        PCollection<Row> beamRows = null;
        Schema beamSchema = null;

        if (source.sourceType == SourceType.text) {
            beamSchema = source.getTextFileSchema();
            LOG.info("Source schema field count: " + beamSchema.getFieldCount() + ", fields: " + StringUtils.join(beamSchema.getFieldNames(), ","));


            if (StringUtils.isNotBlank(this.dataFileUri)) {
                LOG.info("Ingesting file: " + this.dataFileUri + ".");
                beamRows = begin
                        .apply("Read " + source.name + " data: " + this.dataFileUri, TextIO.read().from(this.dataFileUri))
                        .apply("Parse lines into string columns.", ParDo.of(new LineToRowFn(source, beamSchema, source.csvFormat)))
                        .setRowSchema(beamSchema);
            } else if (source.inline != null) {
                LOG.info("Processing " + source.inline.size() + " rows inline.");
                beamRows = begin
                        .apply("Ingest inline dataset: " + source.name, Create.of(source.inline))
                        .apply("Parse lines into string columns.", ParDo.of(new StringListToRowFn(source, beamSchema)))
                        .setRowSchema(beamSchema);
            } else {
                throw new RuntimeException("Data not found.");
            }
        } else {
            throw new RuntimeException("Unhandled source type: " + source.sourceType);
        }

        ////////////////////////////
        // Reset db
        if (jobSpec.config.resetDb) {
            DirectConnect directConnect = new DirectConnect(this.neo4jConnection);
            directConnect.resetNeo4j();
        }

        if ( ModelUtils.nodesOnly(jobSpec) || ModelUtils.relationshipsOnly(jobSpec)){
            for (Target target : jobSpec.getActiveTargets()) {
                Neo4jRowWriterTransform targetWriterTransform = new Neo4jRowWriterTransform(jobSpec, neo4jConnection, target,true,false);
                PCollection<Row> returnVoid=beamRows.apply(target.sequence + ": Writing Neo4j " + target.name, targetWriterTransform);
            }
        } else {
            ////////////////////////////
            // Write node targets
            List<Target> nodeTargets = jobSpec.getActiveNodeTargets();
            List<PCollection<Row>> blockingList = new ArrayList<>();
            for (Target target : nodeTargets) {
                LOG.info("Processing node: "+target.name);
                Neo4jRowWriterTransform targetWriterTransform = new Neo4jRowWriterTransform(jobSpec, neo4jConnection, target,true,false);
                PCollection<Row> returnEmpty=beamRows.apply(target.sequence + ": Writing Neo4j " + target.name, targetWriterTransform);
                blockingList.add(returnEmpty);
            }

            ///////////////////////////////////////////
            //Block until nodes are collected...
            PCollection<Row> unblocking = PCollectionList.of(blockingList).apply("Block", Flatten.pCollections());

            ////////////////////////////
            // Write relationship targets
            List<Target> relationshipTargets = jobSpec.getActiveRelationshipTargets();
            for (Target target : relationshipTargets) {
                Neo4jRowWriterTransform targetWriterTransform = new Neo4jRowWriterTransform(jobSpec, neo4jConnection, target,true,false);
                List<PCollection<Row>> waitForUnblocked = new ArrayList<>();
                waitForUnblocked.add(unblocking);
                waitForUnblocked.add(beamRows);
                PCollection<Row> unblockedBeamRows=PCollectionList.of(waitForUnblocked).apply(target.sequence+": Unblock "+target.name, Flatten.pCollections());
                //  beamRows.apply(Wait.on(blocked));
                PCollection<Row> returnVoid=unblockedBeamRows.apply(target.sequence + ": Writing Neo4j " + target.name, targetWriterTransform);
            }
        }

        // For a Dataflow Flex Template, do NOT waitUntilFinish().
        pipeline.run();
    }
}