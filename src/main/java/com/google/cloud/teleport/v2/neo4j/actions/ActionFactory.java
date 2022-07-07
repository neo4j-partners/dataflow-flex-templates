package com.google.cloud.teleport.v2.neo4j.actions;

import com.google.cloud.teleport.v2.neo4j.common.model.enums.ActionType;
import com.google.cloud.teleport.v2.neo4j.common.model.job.Action;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Factory providing indirection to action handler.
 */
public class ActionFactory {
    public static PTransform<PCollection<Row>, PCollection<Row>> of(Action action, PCollection<Row> contextRow) {
        ActionType actionType=action.type;
        if (actionType == ActionType.query) {
            return new QueryActionImpl(action,contextRow);
        } else if (actionType == ActionType.cypher) {
            return new CypherActionImpl(action,contextRow);
        } else if (actionType == ActionType.http_post) {
            return new HttpPostActionImpl(action,contextRow);
        } else if (actionType == ActionType.http_get) {
            return new HttpGetActionImpl(action,contextRow);
        } else {
            throw new RuntimeException("Unhandled action type: " + actionType);
        }
    }
}
