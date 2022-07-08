package com.google.cloud.teleport.v2.neo4j.actions.transforms;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Query action handler.
 */
public class BigQueryActionTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryActionTransform.class);

    Action action;
    ActionContext context;

    public BigQueryActionTransform(Action action, ActionContext context) {
        this.action = action;
        this.context = context;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        String sql = action.options.get("sql");
        if (StringUtils.isEmpty(sql)){
            throw new RuntimeException("Options 'sql' not provided for preload query transform.");
        }
        try {

            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
            LOG.info("Query: " + sql);
            TableResult queryResult = bigquery.query(queryConfig);
            LOG.info("Result rows: " + queryResult.getTotalRows());

        } catch (Exception e) {
            LOG.error("Exception running sql " + sql + ": " + e.getMessage());
        }

        return this.context.emptyReturn;
    }
}
