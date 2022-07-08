package com.google.cloud.teleport.v2.neo4j.actions;

import com.google.cloud.teleport.v2.neo4j.actions.pojos.*;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.ActionType;
import com.google.cloud.teleport.v2.neo4j.common.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.common.model.job.ActionContext;

/**
 * Factory providing indirection to action handler.
 */
public class ActionFactory {
    public static IAction of(Action action, ActionContext context) {
        ActionType actionType = action.type;
        if (actionType == ActionType.query) {
            QueryActionImpl impl = new QueryActionImpl();
            impl.configure(action, context);
            return impl;
        } else if (actionType == ActionType.cypher) {
            CypherActionImpl impl = new CypherActionImpl();
            impl.configure(action, context);
            return impl;
        } else if (actionType == ActionType.http_post) {
            HttpPostActionImpl impl = new HttpPostActionImpl();
            impl.configure(action, context);
            return impl;
        } else if (actionType == ActionType.http_get) {
            HttpGetActionImpl impl = new HttpGetActionImpl();
            impl.configure(action, context);
            return impl;
        } else {
            throw new RuntimeException("Unhandled action type: " + actionType);
        }
    }
}
