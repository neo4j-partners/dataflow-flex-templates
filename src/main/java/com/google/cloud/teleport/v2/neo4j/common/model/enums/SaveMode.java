package com.google.cloud.teleport.v2.neo4j.common.model.enums;

public enum SaveMode {
    //Overwrite mode performs a MERGE on that node.
    overwrite,
    //ErrorIfExists mode performs a CREATE
    error_if_exists,
    //Append mode performs a CREATE
    append,
    //Match mode performs a MATCH.
    match
}
