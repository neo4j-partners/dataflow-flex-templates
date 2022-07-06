package com.google.cloud.teleport.v2.neo4j.common.model.helpers;

import lombok.Builder;

/**
 * Convenience object for invoking SQL query as well as providing descriptions for read and cast phase of transform.
 */
@Builder
public class SqlQuerySpec {
    public String readDescription;
    public String castDescription;
    public String sql;
}
