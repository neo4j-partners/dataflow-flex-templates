package com.google.cloud.teleport.v2.neo4j.model.job;

import java.io.Serializable;

/**
 * Model to capture SQL aggregate expressions.
 */
public class Aggregation implements Serializable {
    public String expression;
    public String field;
}
