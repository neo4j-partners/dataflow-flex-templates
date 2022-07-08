package com.google.cloud.teleport.v2.neo4j.actions.transforms;

import com.google.cloud.teleport.v2.neo4j.common.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.common.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.common.model.job.ActionContext;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
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
        LOG.info("Executing cypher: " + cypher);
        try {
            directConnect.executeCypher(cypher);
        } catch (Exception e) {
            LOG.error("Exception running cypher, " + cypher + ": " + e.getMessage());
        }
        return this.context.emptyReturn;
    }
}
