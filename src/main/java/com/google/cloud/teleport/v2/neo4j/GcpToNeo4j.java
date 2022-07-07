/**
 * Copyright (C) 2021 Google LLC
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.neo4j;

import com.google.cloud.teleport.v2.neo4j.actions.ActionFactory;
import com.google.cloud.teleport.v2.neo4j.common.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.common.model.InputRefactoring;
import com.google.cloud.teleport.v2.neo4j.common.model.InputValidator;
import com.google.cloud.teleport.v2.neo4j.common.model.connection.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.ActionExecuteAfter;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.ArtifactType;
import com.google.cloud.teleport.v2.neo4j.common.model.helpers.JobSpecMapper;
import com.google.cloud.teleport.v2.neo4j.common.model.helpers.OptionsParamsMapper;
import com.google.cloud.teleport.v2.neo4j.common.model.helpers.SourceQuerySpec;
import com.google.cloud.teleport.v2.neo4j.common.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.common.model.job.*;
import com.google.cloud.teleport.v2.neo4j.common.transforms.GcsLogTransform;
import com.google.cloud.teleport.v2.neo4j.common.transforms.Neo4jRowWriterTransform;
import com.google.cloud.teleport.v2.neo4j.common.utils.BeamBlock;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import providers.IProvider;
import providers.ProviderFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.List;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow template which reads BigQuery data and writes it to Neo4j. The source data can be
 * either a BigQuery table or an SQL query.
 */
public class GcpToNeo4j {

    private static final Logger LOG = LoggerFactory.getLogger(GcpToNeo4j.class);

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public OptionsParams optionsParams;
    ConnectionParams neo4jConnection;
    JobSpec jobSpec;
    Pipeline pipeline;

    /**
     * Main class for template.  Initializes job using run-time on pipelineOptions.
     *
     * @pipelineOptions framework supplied arguments
     */
    public GcpToNeo4j(final Neo4jFlexTemplateOptions pipelineOptions) {
        ////////////////////////////
        // We need to initialize pipeline in order to create context for Gs and Bq file system
        final String jobName = pipelineOptions.getJobName() + "-" + System.currentTimeMillis();
        pipelineOptions.setJobName(jobName);

        // Set pipeline options
        this.pipeline = Pipeline.create(pipelineOptions);
        FileSystems.setDefaultPipelineOptions(pipelineOptions);
        this.optionsParams = OptionsParamsMapper.fromPipelineOptions(pipelineOptions);
        // Validate pipeline
        processValidations(InputValidator.validateNeo4jPipelineOptions(pipelineOptions));

        this.neo4jConnection = new ConnectionParams(pipelineOptions.getNeo4jConnectionUri());
        // Validate connection
        processValidations(InputValidator.validateNeo4jConnection(this.neo4jConnection));

        this.jobSpec = JobSpecMapper.fromUri(pipelineOptions.getJobSpecUri());
        // Validate job spec
        processValidations(InputValidator.validateJobSpec(this.jobSpec));

        ///////////////////////////////////
        // Refactor job spec
        InputRefactoring inputRefactoring = new InputRefactoring(this.optionsParams);
        // Variable substitution
        inputRefactoring.refactorJobSpec(this.jobSpec);
        // Optimizations
        inputRefactoring.optimizeJobSpec(this.jobSpec);

        ///////////////////////////////////
        // Text input specific options and validation
        if (this.jobSpec.sources.size() == 0) {
            String errMsg = "JobSpec source is required.";
            LOG.error(errMsg);
            throw new RuntimeException(errMsg);
        }
        // Source specific validations
        for (Source source : jobSpec.getSourceList()) {
            //get provider implementation for source
            IProvider providerImpl = ProviderFactory.of(source.sourceType);
            providerImpl.configure(optionsParams, jobSpec);
            processValidations(providerImpl.validateJobSpec());
        }

        LOG.info(gson.toJson(jobSpec));
    }

    /**
     * Raises RuntimeExceptions for validation errors.
     *
     * @param validationMessages
     */
    private void processValidations(List<String> validationMessages) {
        StringBuffer sb = new StringBuffer();
        if (validationMessages.size() > 0) {
            for (String msg : validationMessages) {
                LOG.error(msg);
                sb.append(msg);
            }
            throw new RuntimeException("Errors found validating pipeline input.  Please see logs for more details: " + sb);
        }
    }

    /**
     * Runs a pipeline which reads data from various sources and writes it to Neo4j.
     *
     * @param args arguments to the pipeline
     */
    public static void main(final String[] args) {
        final Neo4jFlexTemplateOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation()
                        .as(Neo4jFlexTemplateOptions.class);

        LOG.info("Job: " + options.getJobSpecUri());
        final GcpToNeo4j bqToNeo4jTemplate = new GcpToNeo4j(options);
        bqToNeo4jTemplate.run();
    }

    public void run() {

        ////////////////////////////
        // Reset db
        if (jobSpec.config.resetDb) {
            Neo4jConnection directConnect = new Neo4jConnection(this.neo4jConnection);
            directConnect.resetDatabase();
        }

        ////////////////////////////
        // Initialize wait-on collection queue...
        RowCoder fooCoder = RowCoder.of(Schema.of(Schema.Field.of("foo", Schema.FieldType.STRING)));
        PCollection<Row> seedCollection = pipeline.apply("Floating Queue", Create.empty(TypeDescriptor.of(Row.class)).withCoder(fooCoder));

        // Creating serialization handle
        BeamBlock blockingQueue = new BeamBlock(seedCollection);

        ////////////////////////////
        // Process sources
        for (Source source : jobSpec.getSourceList()) {

            //get provider implementation for source
            IProvider providerImpl = ProviderFactory.of(source.sourceType);
            providerImpl.configure(optionsParams, jobSpec);
            //TODO: delay source query until preloads are complete
            //PBegin begin = pipeline.apply("Querying " +source.name, Wait.on(blockingQueue.waitOnCollection(source.executeAfter, source.executeAfterName, source.name + " query")));
            PCollection<Row> sourceMetadata=pipeline.apply("Source metadata", providerImpl.queryMetadata(source));
            Schema sourceBeamSchema = sourceMetadata.getSchema();
            blockingQueue.addToQueue(ArtifactType.source, false, source.name, seedCollection, sourceMetadata);
            PCollection nullableSourceBeamRows = null;

            ////////////////////////////
            // Optimization: if single source query, reuse this PCollection rather than write it again
            boolean targetsHaveTransforms = ModelUtils.targetsHaveTransforms(jobSpec, source);
            if (!targetsHaveTransforms || !providerImpl.supportsSqlPushDown()) {
                SourceQuerySpec sourceQuerySpec = SourceQuerySpec.builder().source(source).sourceSchema(sourceBeamSchema).build();
                nullableSourceBeamRows = pipeline.apply("Common query", providerImpl.querySourceBeamRows(sourceQuerySpec)).setRowSchema(sourceBeamSchema);
            }

            ////////////////////////////
            // Optimization: if we're not mixing nodes and edges, then run in parallel
            // For relationship updates, max workers should be max 2.

            ////////////////////////////
            // No optimization possible so write nodes then edges.
            // Write node targets
            List<Target> nodeTargets = jobSpec.getActiveNodeTargetsBySource(source.name);
            for (Target target : nodeTargets) {
                TargetQuerySpec targetQuerySpec = TargetQuerySpec.builder()
                        .source(source)
                        .sourceBeamSchema(sourceBeamSchema)
                        .nullableSourceRows(nullableSourceBeamRows)
                        .target(target)
                        .build();
                PCollection<Row> preInsertBeamRows;
                if (ModelUtils.targetHasTransforms(target)) {
                    preInsertBeamRows = pipeline.apply(target.sequence + ": Nodes query " + target.name, providerImpl.queryTargetBeamRows(targetQuerySpec));
                } else {
                    preInsertBeamRows = nullableSourceBeamRows;
                }
                Neo4jRowWriterTransform targetWriterTransform = new Neo4jRowWriterTransform(jobSpec, neo4jConnection, target);

                PCollection<Row> emptyReturn = null;
                // We don't need to add an explicit queue step under these conditions.
                // Implicitly, the job will queue until after its source is complete.
                if (target.executeAfter == ActionExecuteAfter.start
                        || target.executeAfter == ActionExecuteAfter.sources
                        || target.executeAfter == ActionExecuteAfter.async) {
                    emptyReturn = preInsertBeamRows.apply(target.sequence + ": Writing Neo4j " + target.name, targetWriterTransform);
                } else {
                    emptyReturn = preInsertBeamRows
                            .apply(target.sequence + ": Unblocking " + target.name, Wait.on(blockingQueue.waitOnCollection(target.executeAfter, target.executeAfterName, source.name + " nodes"))).setCoder(preInsertBeamRows.getCoder())
                            .apply(target.sequence + ": Writing Neo4j " + target.name, targetWriterTransform);
                }
                if (!StringUtils.isEmpty(jobSpec.config.auditGsUri)) {
                    GcsLogTransform logTransform = new GcsLogTransform(jobSpec, target);
                    preInsertBeamRows.apply(target.sequence + ": Logging " + target.name, logTransform);
                }
                blockingQueue.addToQueue(ArtifactType.node, false, target.name, emptyReturn, preInsertBeamRows);
            }

            ////////////////////////////
            // Write relationship targets
            List<Target> relationshipTargets = jobSpec.getActiveRelationshipTargetsBySource(source.name);
            for (Target target : relationshipTargets) {
                TargetQuerySpec targetQuerySpec = TargetQuerySpec.builder()
                        .source(source)
                        .nullableSourceRows(nullableSourceBeamRows)
                        .sourceBeamSchema(sourceBeamSchema)
                        .target(target)
                        .build();
                PCollection<Row> preInsertBeamRows;
                if (ModelUtils.targetHasTransforms(target)) {
                    preInsertBeamRows = pipeline.apply(target.sequence + ": Edges query " + target.name, providerImpl.queryTargetBeamRows(targetQuerySpec));
                } else {
                    preInsertBeamRows = nullableSourceBeamRows;
                }
                Neo4jRowWriterTransform targetWriterTransform = new Neo4jRowWriterTransform(jobSpec, neo4jConnection, target);
                PCollection<Row> emptyReturn = null;
                // We don't need to add an explicit queue step under these conditions
                // Implicitly, the job will queue until after its source is complete.
                if (target.executeAfter == ActionExecuteAfter.start
                        || target.executeAfter == ActionExecuteAfter.sources
                        || target.executeAfter == ActionExecuteAfter.async) {

                    emptyReturn = preInsertBeamRows
                            .apply(target.sequence + ": Writing Neo4j " + target.name, targetWriterTransform);

                } else {
                    emptyReturn = preInsertBeamRows
                            .apply(target.sequence + ": Unblocking " + target.name, Wait.on(blockingQueue.waitOnCollection(target.executeAfter, target.executeAfterName, source.name + " nodes"))).setCoder(preInsertBeamRows.getCoder())
                            .apply(target.sequence + ": Writing Neo4j " + target.name, targetWriterTransform);
                }
                if (!StringUtils.isEmpty(jobSpec.config.auditGsUri)) {
                    GcsLogTransform logTransform = new GcsLogTransform(jobSpec, target);
                    preInsertBeamRows.apply(target.sequence + ": Logging " + target.name, logTransform);
                }
                //serialize relationships
                blockingQueue.addToQueue(ArtifactType.edge, false, target.name, emptyReturn, preInsertBeamRows);
            }
        }

        ////////////////////////////
        // Process actions (first pass)
        runActions(seedCollection, jobSpec.actions, blockingQueue);

        // For a Dataflow Flex Template, do NOT waitUntilFinish().
        pipeline.run();

    }

    private void runActions(PCollection<Row> seedCollection, List<Action> actions, BeamBlock blockingQueue) {
        for (Action action : actions) {
            LOG.info("Registering action: " + gson.toJson(action));
            ArtifactType artifactType = ArtifactType.action;
            if (action.executeAfter == ActionExecuteAfter.source) {
                artifactType = ArtifactType.source;
            } else if (action.executeAfter == ActionExecuteAfter.node) {
                artifactType = ArtifactType.node;
            } else if (action.executeAfter == ActionExecuteAfter.edge) {
                artifactType = ArtifactType.edge;
            }
            LOG.info("Executing delayed action: " + action.name);
            // Get targeted execution context
            PCollection<Row> executionContext = blockingQueue.getContextCollection(artifactType, action.executeAfterName);
            PTransform<PCollection<Row>, PCollection<Row>> actionImpl = ActionFactory.of(action, executionContext);
            PCollection<Row> finished = executionContext.apply(action.name + ": Unblocking", Wait.on(
                            blockingQueue.waitOnCollection(action.executeAfter, action.executeAfterName, action.name)
                    )).setCoder(executionContext.getCoder())
                    .apply("Action " + action.name, actionImpl);
            blockingQueue.addToQueue(ArtifactType.action, action.executeAfter==ActionExecuteAfter.start, action.name, seedCollection, finished);
        }
    }

}
