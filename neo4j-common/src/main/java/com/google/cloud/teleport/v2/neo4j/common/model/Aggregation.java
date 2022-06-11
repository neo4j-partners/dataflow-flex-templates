package com.google.cloud.teleport.v2.neo4j.common.model;

import java.io.Serializable;

public class Aggregation implements Serializable {
    public String expression;
    public String field;
}
