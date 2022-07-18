package com.google.cloud.teleport.v2.neo4j.actions.preload;

import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Cypher runner action handler.
 */
public class PreloadCypherAction implements PreloadAction {
    private static final Logger LOG = LoggerFactory.getLogger(PreloadCypherAction.class);

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
        if (StringUtils.isEmpty(cypher)){
            throw new RuntimeException("Options 'cypher' not provided for preload cypher action.");
        }
        LOG.info("Executing cypher: " + cypher);
        try {
            directConnect.executeCypher(cypher);
        } catch (Exception e) {
            LOG.error("Exception running cypher, " + cypher + ": " + e.getMessage());
        }

        return msgs;
    }
}

