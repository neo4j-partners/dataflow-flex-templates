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

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.v2.neo4j.bq.options.BigQueryToNeo4jImportOptions;
import com.google.cloud.teleport.v2.neo4j.common.InputOptimizer;
import com.google.cloud.teleport.v2.neo4j.common.InputValidator;
import com.google.cloud.teleport.v2.neo4j.common.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.common.transforms.Neo4jRowWriterTransform;
import com.google.cloud.teleport.v2.neo4j.common.model.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Dataflow template which reads BigQuery data and writes it to Neo4j. The source data can be
 * either a BigQuery table or an SQL query.
 */
public class BigQueryToNeo4j {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryToNeo4j.class);
    ConnectionParams neo4jConnection;
    JobSpecRequest jobSpec;
    String BASE_SQL;
    Pipeline pipeline;

    /**
     * Runs a pipeline which reads data from BigQuery and writes it to Bigtable.
     *
     * @param args arguments to the pipeline
     */

    public static void main(final String[] args) {
        final BigQueryToNeo4jImportOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation()
                        .as(BigQueryToNeo4jImportOptions.class);

        LOG.info("Job: " + options.getJobSpecUri());
        final BigQueryToNeo4j bqToNeo4jTemplate = new BigQueryToNeo4j(options);
        bqToNeo4jTemplate.run();
    }

    public BigQueryToNeo4j(final BigQueryToNeo4jImportOptions pipelineOptions) {
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
        if (StringUtils.isEmpty(pipelineOptions.getReadQuery())) {
            String errMsg = "Could not determine BQ read query.";
            LOG.error(errMsg);
            throw new RuntimeException(errMsg);
        }
        BASE_SQL = pipelineOptions.getReadQuery().trim();
        //check for unsupported SQL
    }


    public void run() {

        final Gson gson = new GsonBuilder().setPrettyPrinting().create();

        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

        LOG.info("Reading from BQ with query: " + BASE_SQL);

        ////////////////////////////
        // Reset db
        if (jobSpec.config.resetDb) {
            Neo4jConnection directConnect = new Neo4jConnection(this.neo4jConnection);
            directConnect.resetDatabase();
        }

        ////////////////////////////
        // Dry run won't return schema so use regular query
        // We need fieldSet for SQL generation later
        String ZERO_ROW_SQL = "SELECT * FROM (" + BASE_SQL + ") LIMIT 0";
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(ZERO_ROW_SQL).build();
        Set<String> fieldSet;
        try {
            LOG.info("Metadata query: " + ZERO_ROW_SQL);
            TableResult zeroRowQueryResult = bigquery.query(queryConfig);
            fieldSet = ModelUtils.getBqFieldSet(zeroRowQueryResult.getSchema());
        } catch (Exception e) {
            String errMsg = "Error running query: " + e.getMessage();
            LOG.error(errMsg);
            throw new RuntimeException(errMsg);
        }

        PCollection beamRows = null;

        ////////////////////////////
        // Optimization: if single source query, reuse this PRecordset rather than write it again
        boolean singleSourceQuery = ModelUtils.singleSourceSpec(jobSpec);
        if (singleSourceQuery) {
            beamRows = queryBq(0, "common", BASE_SQL);
        }

        ////////////////////////////
        // Optimization: if we're not mixing nodes and edges, then run in parallel
        // For relationship updates, max workers should be max 2.
        if (ModelUtils.nodesOnly(jobSpec) || ModelUtils.relationshipsOnly(jobSpec)) {
            for (Target target : jobSpec.getActiveTargets()) {
                if (!singleSourceQuery) {
                    String TARGET_SPECIFIC_SQL = ModelUtils.getTargetSql(fieldSet, target, true, BASE_SQL);
                    LOG.info("Target (" + target.name + ") specific sql: " + TARGET_SPECIFIC_SQL);
                    beamRows = queryBq(target, TARGET_SPECIFIC_SQL);
                }
                Neo4jRowWriterTransform targetWriterTransform = new Neo4jRowWriterTransform(jobSpec, neo4jConnection, target);
                beamRows.apply(target.sequence + ": Writing Neo4j " + target.name, targetWriterTransform);
            }
        } else {
            ////////////////////////////
            // No optimization possible so write nodes then edges.
            // Write node targets
            List<Target> nodeTargets = jobSpec.getActiveNodeTargets();
            List<PCollection<Row>> blockingList = new ArrayList<>();
            for (Target target : nodeTargets) {
                Neo4jRowWriterTransform targetWriterTransform = new Neo4jRowWriterTransform(jobSpec, neo4jConnection, target);
                if (!singleSourceQuery) {
                    String TARGET_SPECIFIC_SQL = ModelUtils.getTargetSql(fieldSet, target, true, BASE_SQL);
                    LOG.info("Node target (" + target.name + ") specific sql: " + TARGET_SPECIFIC_SQL);
                    beamRows = queryBq(target, TARGET_SPECIFIC_SQL);
                }
                // Return empty PCollection to sequence operations
                PCollection<Row> emptyReturn = ((PCollection) beamRows.apply(target.sequence + ": Writing Neo4j " + target.name, targetWriterTransform));
                blockingList.add(emptyReturn);
            }

            ///////////////////////////////////////////
            //Block until nodes are collected...
            PCollection<Row> blocked = PCollectionList.of(blockingList).apply("Block", Flatten.pCollections());

            ////////////////////////////
            // Write relationship targets
            List<Target> relationshipTargets = jobSpec.getActiveRelationshipTargets();
            for (Target target : relationshipTargets) {
                Neo4jRowWriterTransform targetWriterTransform = new Neo4jRowWriterTransform(jobSpec, neo4jConnection, target);
                if (!singleSourceQuery) {
                    String TARGET_SPECIFIC_SQL = ModelUtils.getTargetSql(fieldSet, target, true, BASE_SQL);
                    LOG.info("Relationship target (" + target.name + ") specific sql: " + TARGET_SPECIFIC_SQL);
                    beamRows = queryBq(target, TARGET_SPECIFIC_SQL);
                }
                List<PCollection<Row>> unblockedList = new ArrayList<>();
                unblockedList.add(beamRows);
                unblockedList.add(blocked);
                PCollection<Row> unblockedBeamRows = PCollectionList.of(unblockedList).apply(target.sequence + ": Unblock", Flatten.pCollections());
                PCollection<Row> returnVoid = unblockedBeamRows.apply(target.sequence + ": Writing Neo4j " + target.name, targetWriterTransform);
            }
        }

        ////////////////////////////
        // Write neo4j
        LOG.info("Found " + jobSpec.targets.size() + " candidate targets");

        // For a Dataflow Flex Template, do NOT waitUntilFinish().
        pipeline.run();

    }

    private PCollection<Row> queryBq(Target target, String SQL) {
        return queryBq(target.sequence, target.name, SQL);
    }

    private PCollection<Row> queryBq(int sequence, String desrciption, String SQL) {
        PCollection<TableRow> sourceRows =
                pipeline.apply(sequence + ": Read from BQ " + desrciption, BigQueryIO.readTableRowsWithSchema()
                        .fromQuery(SQL)
                        .usingStandardSql()
                        .withTemplateCompatibility());

        Schema beamSchema = sourceRows.getSchema();
        Coder<Row> rowCoder = SchemaCoder.of(beamSchema);
        LOG.info("Beam schema: {}", beamSchema);
        PCollection<Row> beamRows =
                sourceRows.apply(sequence + ": BQ to BeamRow " + desrciption,
                                MapElements
                                        .into(TypeDescriptor.of(Row.class))
                                        .via(sourceRows.getToRowFunction()))
                        .setCoder(rowCoder);
        return beamRows;
    }
}
