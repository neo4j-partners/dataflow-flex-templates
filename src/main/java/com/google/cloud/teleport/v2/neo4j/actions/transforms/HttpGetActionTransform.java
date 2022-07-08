package com.google.cloud.teleport.v2.neo4j.actions.transforms;

import com.google.cloud.teleport.v2.neo4j.common.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.common.model.job.ActionContext;
import com.google.cloud.teleport.v2.neo4j.common.utils.HttpUtils;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Http GET action handler.
 */
public class HttpGetActionTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
    private static final Logger LOG = LoggerFactory.getLogger(HttpGetActionTransform.class);
    Action action;
    ActionContext context;

    public HttpGetActionTransform(Action action, ActionContext context) {
        this.action = action;
        this.context = context;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {

        try {
            CloseableHttpResponse response = HttpUtils.getHttpRespoonse(false,
                    action.options.get("uri"),
                    action.options,
                    action.headers);
            LOG.info("Request returned: " + HttpUtils.getResponseContent(response));

        } catch (Exception e) {
            LOG.error("Exception making http get request: " + e.getMessage());
        }
        return this.context.emptyReturn;
    }
}
