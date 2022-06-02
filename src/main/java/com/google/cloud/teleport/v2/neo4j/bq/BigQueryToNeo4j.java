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
package com.google.cloud.teleport.v2.neo4j.bq;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import com.google.cloud.teleport.v2.neo4j.bq.bq.options.BigQueryToNeo4jImportOptions;
import com.google.cloud.teleport.v2.neo4j.bq.transforms.TableRow2RowFn;
import com.google.cloud.teleport.v2.neo4j.common.BeamSchemaUtils;
import com.google.cloud.teleport.v2.neo4j.common.JobSpecOptimizer;
import com.google.cloud.teleport.v2.neo4j.common.Neo4JTargetWriter;
import com.google.cloud.teleport.v2.neo4j.common.Validations;
import com.google.cloud.teleport.v2.neo4j.common.model.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.springframework.util.StringUtils;

import java.util.Collections;

/**
 * Dataflow template which reads BigQuery data and writes it to Neo4j. The source data can be
 * either a BigQuery table or an SQL query.
 */
public class BigQueryToNeo4j {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryToNeo4j.class);
  BigQueryToNeo4jImportOptions pipelineOptions;
  ConnectionParams neo4jConnection;
  JobSpecRequest jobSpec;

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
    this.pipelineOptions = pipelineOptions;
  }


  public void run() {
    final Gson gson = new Gson();

    // Execute a pipeline on Dataflow
    Pipeline pipeline = Pipeline.create(pipelineOptions);
    FileSystems.setDefaultPipelineOptions(pipelineOptions);

    final String jobName = pipelineOptions.getJobName() + "-" + System.currentTimeMillis();

    // Create a pipeline using the options

    //TODO: check if connectionUri provided
    final String neoConnectionUri = pipelineOptions.getNeo4jConnectionUri();
    neo4jConnection = new ConnectionParams(neoConnectionUri);
    BigQueryToNeo4j.LOG.info("NeoConnection: " + gson.toJson(neo4jConnection));

    if (StringUtils.isEmpty(pipelineOptions.getJobSpecUri())) {
      throw new RuntimeException("Job spec URI not provided.");
    }

    final String jobSpecUri = pipelineOptions.getJobSpecUri();
    jobSpec = new JobSpecRequest(jobSpecUri);
    LOG.info("JobSpec: " + gson.toJson(jobSpec));

    Validations validations = JobSpecOptimizer.validateAndOptimize(jobSpec);
    if (validations.errors) {
      for (String msg : validations.validationMessages) {
        LOG.error(msg);
      }
      throw new RuntimeException("Errors found validating jobSpec.  Please see logs for more details.");
    }


    //////// new stuff start
    LOG.info("Data pipeline options: " + pipelineOptions);

    final PBegin begin = pipeline.begin();
    // Read all the lines from a file

    //TODO: consider adding these?
    TupleTag<Entity> successTag = new TupleTag<Entity>() {};
    TupleTag<String> failureTag = new TupleTag<String>("failures") {};
    //////////////

    org.apache.beam.sdk.schemas.Schema sourceSchema =  null;

    // TODO: can this be done more elegantly? Consider BigQueryQuerySourceDef.getBeamSchema(BigQueryOptions bqOptions)
    // Zero row query for getting metadata from connection context
    // we are not using the bulk query engine here, just the default service
    String queryZeroRows="SELECT * FROM ("+pipelineOptions.getReadQuery()+") LIMIT 0";
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    QueryJobConfiguration queryConfig=QueryJobConfiguration.newBuilder(queryZeroRows).build();

    try {
      LOG.info("Getting metadata from query: "+queryZeroRows);
      TableResult zeroRowQueryResult = bigquery.query(queryConfig);
      com.google.cloud.bigquery.Schema zeroRowQueryResultSchema = zeroRowQueryResult.getSchema();
      FieldList bqFieldList=zeroRowQueryResultSchema.getFields();
      sourceSchema= BeamSchemaUtils.fromBqFieldList( bqFieldList);
    } catch (Exception ex){
      LOG.error("Cannot parse query ");
      throw new RuntimeException("Exception parsing query: "+pipelineOptions.getReadQuery());
    }

    LOG.info("Reading from BQ with query: "+pipelineOptions.getReadQuery());

    // BQ  import setup
    PCollection<TableRow> bqRowsCollection =
            pipeline.apply(
                    "Read from BQ",
                    BigQueryIO.readTableRows()
                            .fromQuery(pipelineOptions.getReadQuery())
                            .usingStandardSql()
                            .withTemplateCompatibility());

    LOG.info("BQ Rows collection, source schema cols: "+sourceSchema.getFieldCount()+", cols: "+ StringUtils.collectionToCommaDelimitedString(sourceSchema.getFieldNames()));

    final DoFn<TableRow, Row> tableRow2RowFn = new TableRow2RowFn(sourceSchema);
    PCollection<Row> sourceRowsCollection = bqRowsCollection.apply( "Map table rows", ParDo.of(tableRow2RowFn));
    sourceRowsCollection.setRowSchema(sourceSchema);
    sourceRowsCollection.setCoder(SerializableCoder.of(Row.class));

    Neo4JTargetWriter.writeTargets( jobSpec,
            neo4jConnection,
            sourceSchema,
            sourceRowsCollection);

    // Now run this using the dataflow runner
    final DataflowRunner dataflowRunner = DataflowRunner.fromOptions(pipelineOptions);
    final DataflowPipelineJob job = dataflowRunner.run(pipeline);
    final PipelineResult.State state = job.waitUntilFinish();
    BigQueryToNeo4j.LOG.info("Final state : " + state);
  }






}
