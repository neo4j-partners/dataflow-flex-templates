package com.google.cloud.teleport.v2.neo4j.actions.pojos;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.v2.neo4j.common.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.common.model.job.ActionContext;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Query action handler.
 */
public class QueryActionImpl implements IAction {
    private static final Logger LOG = LoggerFactory.getLogger(QueryActionImpl.class);

    Action action;
    ActionContext context;

    public void configure(Action action, ActionContext context) {
        this.action = action;
        this.context = context;
    }

    public List<String> execute() {
        List<String> msgs = new ArrayList<>();
        String sql = action.options.get("sql");

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
