package com.google.cloud.teleport.v2.neo4j.actions.preload;

import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import com.google.cloud.teleport.v2.neo4j.utils.HttpUtils;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Http GET action handler.
 */
public class PreloadHttpGetAction implements PreloadAction {
    private static final Logger LOG = LoggerFactory.getLogger(PreloadHttpGetAction.class);

    Action action;
    ActionContext context;

    public void configure(Action action, ActionContext context) {
        this.action = action;
        this.context = context;
    }

    public List<String> execute() {
        List<String> msgs = new ArrayList<>();
        String uri = action.options.get("url");
        if (StringUtils.isEmpty(uri)){
            throw new RuntimeException("Options 'uri' not provided for preload http_get action.");
        }
        try {
            CloseableHttpResponse response = HttpUtils.getHttpRespoonse(false,
                    action.options.get("url"),
                    action.options,
                    action.headers);
            LOG.info("Request returned: " + HttpUtils.getResponseContent(response));

        } catch (Exception e) {
            LOG.error("Exception making http get request: " + e.getMessage());
        }

        return msgs;
    }
}

