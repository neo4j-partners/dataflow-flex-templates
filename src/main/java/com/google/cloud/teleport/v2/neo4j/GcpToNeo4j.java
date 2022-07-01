/*
 * Copyright (C) 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.neo4j;

import com.google.cloud.teleport.v2.neo4j.common.InputRefactoring;
import com.google.cloud.teleport.v2.neo4j.common.InputValidator;
import com.google.cloud.teleport.v2.neo4j.common.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.common.model.*;
import com.google.cloud.teleport.v2.neo4j.common.options.Neo4jFlexTemplateOptions;
import com.google.cloud.teleport.v2.neo4j.common.transforms.GcsLogTransform;
import com.google.cloud.teleport.v2.neo4j.common.transforms.Neo4jRowWriterTransform;
import com.google.cloud.teleport.v2.neo4j.common.utils.BeamBlock;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import com.google.cloud.teleport.v2.neo4j.providers.IProvider;
import com.google.cloud.teleport.v2.neo4j.providers.ProviderFactory;
import com.google.cloud.teleport.v2.neo4j.providers.SourceQuerySpec;
import com.google.cloud.teleport.v2.neo4j.providers.TargetQuerySpec;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Dataflow template which reads BigQuery data and writes it to Neo4j. The source data can be
 * either a BigQuery table or an SQL query.
 */
public class GcpToNeo4j {

    private static final Logger LOG = LoggerFactory.getLogger(GcpToNeo4j.class);
    ConnectionParams neo4jConnection;
    JobSpecRequest jobSpec;

    public OptionsParams optionsParams;
    Pipeline pipeline;

    /**
     * Runs a pipeline which reads data from BigQuery and writes it to Bigtable.
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

    public GcpToNeo4j(final Neo4jFlexTemplateOptions pipelineOptions) {
        ////////////////////////////
        // We need to initialize pipeline in order to create context for Gs and Bq file system
        final String jobName = pipelineOptions.getJobName() + "-" + System.currentTimeMillis();
        pipelineOptions.setJobName(jobName);
        this.pipeline = Pipeline.create(pipelineOptions);
        FileSystems.setDefaultPipelineOptions(pipelineOptions);
        this.optionsParams = new OptionsParams(pipelineOptions);

        List<String> validationMessages = InputValidator.validateNeo4jPipelineOptions(pipelineOptions);
        StringBuffer sb = new StringBuffer();
        if (validationMessages.size() > 0) {
            for (String msg : validationMessages) {
                LOG.error(msg);
                sb.append(msg);
            }
            throw new RuntimeException("Errors found validating pipeline input.  Please see logs for more details: " + sb.toString());
        }
        this.neo4jConnection = new ConnectionParams(pipelineOptions.getNeo4jConnectionUri());
        this.jobSpec = new JobSpecRequest(pipelineOptions.getJobSpecUri());
        validationMessages.addAll(InputValidator.validateNeo4jConnection(this.neo4jConnection));
        validationMessages.addAll(InputValidator.validateJobSpec(this.jobSpec));

        InputRefactoring inputRefactoring=new InputRefactoring(this.optionsParams);
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
            List<String> sourceValidationMessages = providerImpl.validateJobSpec();
            if (sourceValidationMessages.size() > 0) {
                for (String msg : validationMessages) {
                    LOG.error(msg);
                }
                throw new RuntimeException("Errors found validating pipeline input for " + source.name + ".  Please see logs for more details.");
            }
        }
    }


    public void run() {

        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

        ////////////////////////////
        // Reset db
        if (jobSpec.config.resetDb) {
            Neo4jConnection directConnect = new Neo4jConnection(this.neo4jConnection);
            directConnect.resetDatabase();
        }

        BeamBlock blockingQueue = BeamBlock.create("Serial");
        for (Source source : jobSpec.getSourceList()) {

            //get provider implementation for source
            IProvider providerImpl = ProviderFactory.of(source.sourceType);
            providerImpl.configure(optionsParams, jobSpec);
            PCollection<Row> sourceMetadata = pipeline.apply("Source metadata", providerImpl.queryMetadata(source));
            Schema sourceBeamSchema = sourceMetadata.getSchema();
            PCollection nullableSourceBeamRows = null;

            ////////////////////////////
            // Optimization: if single source query, reuse this PCollection rather than write it again
            boolean targetsHaveTransforms = ModelUtils.targetsHaveTransforms(jobSpec, source);
            if (!targetsHaveTransforms || !providerImpl.supportsSqlPushDown() ) {
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
                    preInsertBeamRows = pipeline.apply(target.sequence + ": Target nodes query " + target.name, providerImpl.queryTargetBeamRows(targetQuerySpec));
                } else {
                    preInsertBeamRows = nullableSourceBeamRows;
                }
                Neo4jRowWriterTransform targetWriterTransform = new Neo4jRowWriterTransform(jobSpec, neo4jConnection, target);
                PCollection<Row> emptyReturn = preInsertBeamRows.apply(target.sequence + ": Writing Neo4j " + target.name, targetWriterTransform);
                if (!StringUtils.isEmpty(jobSpec.config.auditGsUri)) {
                    GcsLogTransform logTransform=new GcsLogTransform(jobSpec,target);
                    preInsertBeamRows.apply(target.sequence + ": Logging " + target.name, logTransform);
                }
                blockingQueue.addEmptyBlockingCollection(emptyReturn);
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
                    preInsertBeamRows = pipeline.apply(target.sequence + ": Target edges query " + target.name, providerImpl.queryTargetBeamRows(targetQuerySpec));
                } else {
                    preInsertBeamRows = nullableSourceBeamRows;
                }
                Neo4jRowWriterTransform targetWriterTransform = new Neo4jRowWriterTransform(jobSpec, neo4jConnection, target);
                PCollection<Void> unblockedVoid=blockingQueue.waitOnVoidCollection(target.sequence + ": ").setCoder(VoidCoder.of());
                PCollection<Row> emptyReturn = preInsertBeamRows
                        .apply(Wait.on(unblockedVoid))
                        .apply(target.sequence + ": Writing Neo4j " + target.name, targetWriterTransform);
                if (!StringUtils.isEmpty(jobSpec.config.auditGsUri)) {
                    GcsLogTransform logTransform=new GcsLogTransform(jobSpec,target);
                    preInsertBeamRows.apply(target.sequence + ": Logging " + target.name, logTransform);
                }
                //serialize relationships
                blockingQueue.addEmptyBlockingCollection(emptyReturn);
            }


            ////////////////////////////
            // Write neo4j
            LOG.info("Found " + jobSpec.targets.size() + " candidate targets");

        }

        // For a Dataflow Flex Template, do NOT waitUntilFinish().
        pipeline.run();

    }

}
