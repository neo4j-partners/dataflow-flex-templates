package com.google.cloud.teleport.v2.neo4j.actions.transforms;

import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import com.google.cloud.teleport.v2.neo4j.utils.HttpUtils;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Http POST action handler.
 */
public class HttpPostActionTransform extends PTransform<PCollection<Row>, PCollection<Row>> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpPostActionTransform.class);
    Action action;
    ActionContext context;

    public HttpPostActionTransform(Action action, ActionContext context) {
        this.action = action;
        this.context = context;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        String uri = action.options.get("url");
        if (StringUtils.isEmpty(uri)){
            throw new RuntimeException("Options 'uri' not provided for preload http_post action.");
        }
        try {
            CloseableHttpResponse response = HttpUtils.getHttpRespoonse(true,
                    uri,
                    action.options,
                    action.headers);
            LOG.info("Request returned: " + HttpUtils.getResponseContent(response));

        } catch (Exception e) {
            LOG.error("Exception making http get request: " + e.getMessage());
        }
        // we are not running anything that generates an output, so can return an input.
        return input;
    }
}
