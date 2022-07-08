package com.google.cloud.teleport.v2.neo4j.actions.pojos;

import com.google.cloud.teleport.v2.neo4j.common.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.common.model.job.ActionContext;
import com.google.cloud.teleport.v2.neo4j.common.utils.HttpUtils;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Http GET action handler.
 */
public class HttpGetActionImpl  implements IAction {
    private static final Logger LOG = LoggerFactory.getLogger(HttpGetActionImpl.class);

    Action action;
    ActionContext context;

    public void configure(Action action, ActionContext context) {
        this.action = action;
        this.context = context;
    }

    public List<String> execute() {
        List<String> msgs=new ArrayList<>();

        try {
            CloseableHttpResponse response= HttpUtils.getHttpRespoonse(false,
                    action.options.get("uri"),
                    action.options,
                    action.headers);
            LOG.info("Request returned: "+HttpUtils.getResponseContent(response));

        } catch (Exception e){
            LOG.error("Exception making http get request: "+e.getMessage());
        }

        return msgs;
    }
}

