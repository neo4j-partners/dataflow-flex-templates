package com.google.cloud.teleport.v2.neo4j.common.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Transform implements Serializable {
    public String sql = "";
    public List<Aggregation> aggregations = new ArrayList<>();
    public boolean group;
    public String orderBy = "";
    public String where = "";
    public int limit = -1;
}
