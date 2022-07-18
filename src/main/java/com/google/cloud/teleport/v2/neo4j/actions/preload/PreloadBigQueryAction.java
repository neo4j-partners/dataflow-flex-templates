package com.google.cloud.teleport.v2.neo4j.actions.preload;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Query action handler.
 */
public class PreloadBigQueryAction implements PreloadAction {
    private static final Logger LOG = LoggerFactory.getLogger(PreloadBigQueryAction.class);

    Action action;
    ActionContext context;

    public void configure(Action action, ActionContext context) {
        this.action = action;
        this.context = context;
    }

    public List<String> execute() {
        List<String> msgs = new ArrayList<>();
        String sql = action.options.get("sql");
        if (StringUtils.isEmpty(sql)){
            throw new RuntimeException("Options 'sql' not provided for preload query action.");
        }
        try {

            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
            msgs.add("Query: " + sql);
            TableResult queryResult = bigquery.query(queryConfig);
            msgs.add("Result rows: " + queryResult.getTotalRows());

        } catch (Exception e) {
            LOG.error("Exception running sql " + sql + ": " + e.getMessage());
        }

        return msgs;
    }
}
