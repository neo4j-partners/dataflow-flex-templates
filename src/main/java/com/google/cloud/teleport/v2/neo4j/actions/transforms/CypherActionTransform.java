package com.google.cloud.teleport.v2.neo4j.actions.transforms;

import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Cypher runner action handler.
 */
public class CypherActionTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
    private static final Logger LOG = LoggerFactory.getLogger(CypherActionTransform.class);
    Action action;
    ActionContext context;

    public CypherActionTransform(Action action, ActionContext context) {
        this.action = action;
        this.context = context;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        Neo4jConnection directConnect = new Neo4jConnection(this.context.neo4jConnection);
        String cypher = action.options.get("cypher");
        if (StringUtils.isEmpty(cypher)){
            throw new RuntimeException("Options 'cypher' not provided for cypher action transform.");
        }
        LOG.info("Executing cypher: " + cypher);
        try {
            directConnect.executeCypher(cypher);
        } catch (Exception e) {
            LOG.error("Exception running cypher, " + cypher + ": " + e.getMessage());
        }
        // we are not running anything that generates an output, so can return an input.
        return input;
    }
}
