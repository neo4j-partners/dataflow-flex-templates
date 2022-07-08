package com.google.cloud.teleport.v2.neo4j.model.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Transform metadata.
 */
public class Transform implements Serializable {
    public String sql = "";
    public List<Aggregation> aggregations = new ArrayList<>();
    public boolean group;
    public String orderBy = "";
    public String where = "";
    public int limit = -1;
}
