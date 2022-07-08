package com.google.cloud.teleport.v2.neo4j.common.model.job;

import com.google.cloud.teleport.v2.neo4j.common.model.connection.ConnectionParams;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Context for actions.
 */
public class ActionContext {
    public PCollection<Row> emptyReturn;
    public PCollection<Row> dataContext;
    public ConnectionParams neo4jConnection;
    public JobSpec jobSpec;
}
