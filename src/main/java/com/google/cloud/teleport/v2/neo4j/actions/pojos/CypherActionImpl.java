package com.google.cloud.teleport.v2.neo4j.actions.pojos;

import com.google.cloud.teleport.v2.neo4j.common.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.common.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.common.model.job.ActionContext;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Cypher runner action handler.
 */
public class CypherActionImpl implements IAction {
    private static final Logger LOG = LoggerFactory.getLogger(CypherActionImpl.class);

    Action action;
    ActionContext context;

    public void configure(Action action, ActionContext context) {
        this.action = action;
        this.context = context;
    }

    public List<String> execute() {
        List<String> msgs = new ArrayList<>();

        Neo4jConnection directConnect = new Neo4jConnection(this.context.neo4jConnection);
        String cypher = action.options.get("cypher");
        LOG.info("Executing cypher: " + cypher);
        try {
            directConnect.executeCypher(cypher);
        } catch (Exception e) {
            LOG.error("Exception running cypher, " + cypher + ": " + e.getMessage());
        }

        return msgs;
    }
}

