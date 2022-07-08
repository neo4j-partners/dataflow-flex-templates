package com.google.cloud.teleport.v2.neo4j.model.enums;

/**
 * Type of action plugin.
 */
public enum ActionType {
    cypher,
    http_get,
    http_post,
    bigquery,
    parquet,
    spanner
}
