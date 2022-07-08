package com.google.cloud.teleport.v2.neo4j.actions.pojos;

import com.google.cloud.teleport.v2.neo4j.common.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.common.model.job.ActionContext;
import java.util.List;

public interface IAction {
    public void configure(Action action, ActionContext context);

    public List<String> execute();
}
