package com.google.cloud.teleport.v2.neo4j.model.enums;

/**
 * Save mode.  Does not attempt to replicate Spark connector save modes.
 */
public enum SaveMode {
    merge,
    append
}
