package com.google.cloud.teleport.v2.neo4j.actions;

import com.google.cloud.teleport.v2.neo4j.actions.transforms.CypherActionTransform;
import com.google.cloud.teleport.v2.neo4j.actions.transforms.HttpGetActionTransform;
import com.google.cloud.teleport.v2.neo4j.actions.transforms.HttpPostActionTransform;
import com.google.cloud.teleport.v2.neo4j.actions.transforms.QueryActionTransform;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.ActionType;
import com.google.cloud.teleport.v2.neo4j.common.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.common.model.job.ActionContext;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Factory providing indirection to action handler.
 */
public class ActionBeamFactory {
    public static PTransform<PCollection<Row>, PCollection<Row>> of(Action action, ActionContext context) {
        ActionType actionType = action.type;
        if (actionType == ActionType.query) {
            return new QueryActionTransform(action, context);
        } else if (actionType == ActionType.cypher) {
            return new CypherActionTransform(action, context);
        } else if (actionType == ActionType.http_post) {
            return new HttpPostActionTransform(action, context);
        } else if (actionType == ActionType.http_get) {
            return new HttpGetActionTransform(action, context);
        } else {
            throw new RuntimeException("Unhandled action type: " + actionType);
        }
    }
}
