package com.google.cloud.teleport.v2.neo4j.actions;

import com.google.cloud.teleport.v2.neo4j.actions.preload.*;
import com.google.cloud.teleport.v2.neo4j.model.enums.ActionType;
import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;

/**
 * Factory providing indirection to action handler.
 */
public class ActionFactory {
    public static PreloadAction of(Action action, ActionContext context) {
        ActionType actionType = action.type;
        if (actionType == ActionType.bigquery) {
            PreloadBigQueryAction impl = new PreloadBigQueryAction();
            impl.configure(action, context);
            return impl;
        } else if (actionType == ActionType.cypher) {
            PreloadCypherAction impl = new PreloadCypherAction();
            impl.configure(action, context);
            return impl;
        } else if (actionType == ActionType.http_post) {
            PreloadHttpPostAction impl = new PreloadHttpPostAction();
            impl.configure(action, context);
            return impl;
        } else if (actionType == ActionType.http_get) {
            PreloadHttpGetAction impl = new PreloadHttpGetAction();
            impl.configure(action, context);
            return impl;
        } else {
            throw new RuntimeException("Unhandled action type: " + actionType);
        }
    }
}
