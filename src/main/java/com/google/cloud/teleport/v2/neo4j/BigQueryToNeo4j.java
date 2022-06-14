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
import com.google.cloud.bigquery.*;
import com.google.cloud.teleport.v2.neo4j.bq.options.BigQueryToNeo4jImportOptions;
import com.google.cloud.teleport.v2.neo4j.common.InputValidator;
import com.google.cloud.teleport.v2.neo4j.common.database.TargetWriter;
import com.google.cloud.teleport.v2.neo4j.common.model.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        InputValidator.refactorJobSpec(jobSpec);

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

        if (jobSpec.config.resetDb) {
            TargetWriter.resetNeo4j(neo4jConnection);
        }

        ////////////////////////////
        // Write neo4j
        LOG.info("Found " + jobSpec.targets.size() + " candidate targets");

        boolean singleSourceQuery = ModelUtils.singleSourceSpec(jobSpec);

        if (singleSourceQuery) {

            PCollection<TableRow> sourceRows =
                    pipeline.apply("Read from BQ", BigQueryIO.readTableRowsWithSchema()
                            .fromQuery(BASE_SQL)
                            .usingStandardSql()
                            .withTemplateCompatibility());

            Schema beamSchema = sourceRows.getSchema();
            Coder<Row> rowCoder = SchemaCoder.of(beamSchema);
            LOG.info("Beam schema: {}", beamSchema);
            PCollection<Row> beamRows =
                    sourceRows.apply("BQ to BeamRow",
                                    MapElements
                                            .into(TypeDescriptor.of(Row.class))
                                            .via(sourceRows.getToRowFunction()))
                            .setCoder(rowCoder);

            for (Target target : jobSpec.targets) {
                if (target.active) {
                    TargetWriter.castRowsWriteNeo4j(jobSpec, neo4jConnection, target, beamRows);
                } else {
                    LOG.info("Target " + target.name + " is inactive");
                }
            }

        } else {

            //dry run won't return schema so regulary query
            String ZERO_ROW_SQL = "SELECT * FROM ("+BASE_SQL+") LIMIT 0";
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
            QueryJobConfiguration queryConfig=QueryJobConfiguration.newBuilder(ZERO_ROW_SQL).build();
            Set<String> fieldSet;
            try {
                LOG.info("Getting metadata from query: "+ZERO_ROW_SQL);
                TableResult zeroRowQueryResult = bigquery.query(queryConfig);
                fieldSet = ModelUtils.getBqFieldSet(zeroRowQueryResult.getSchema());
            } catch (Exception e) {
                String errMsg = "Error running query: " + e.getMessage();
                LOG.error(errMsg);
                throw new RuntimeException(errMsg);
            }

            for (Target target : jobSpec.targets) {
                if (target.active) {

                    //Requery BigQuery for each target
                    String PCOLLECTION_SQL = ModelUtils.getTargetSql(fieldSet, target, true);
                    String TARGET_SQL = PCOLLECTION_SQL.replace(" PCOLLECTION", " (" + BASE_SQL + ")");

                    LOG.info("Executing target SQL: "+TARGET_SQL);

                    PCollection<TableRow> sourceRows =
                            pipeline.apply(target.sequence+": BQ "+target.name, BigQueryIO.readTableRowsWithSchema()
                                    .fromQuery(TARGET_SQL)
                                    .usingStandardSql()
                                    .withTemplateCompatibility());

                    Schema beamSchema = sourceRows.getSchema();
                    Coder<Row> rowCoder = SchemaCoder.of(beamSchema);

                    LOG.info("Beam schema: {}", beamSchema);
                    PCollection<Row> beamRows =
                            sourceRows.apply(target.sequence+": ToBeamRow "+target.name,
                                            MapElements
                                                    .into(TypeDescriptor.of(Row.class))
                                                    .via(sourceRows.getToRowFunction()))
                                    .setCoder(rowCoder);

                    POutput writeResults=TargetWriter.castRowsWriteNeo4j(jobSpec, neo4jConnection, target, beamRows);

                } else {
                    LOG.info("Target " + target.name + " is inactive");
                }
            }
        }

        // For a Dataflow Flex Template, do NOT waitUntilFinish().
        pipeline.run();

    }
}
