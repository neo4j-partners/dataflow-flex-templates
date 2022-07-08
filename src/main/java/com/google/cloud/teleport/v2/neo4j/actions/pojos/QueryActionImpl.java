package com.google.cloud.teleport.v2.neo4j.actions.pojos;

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
        List<String> msgs=new ArrayList<>();


        return msgs;
    }
}
