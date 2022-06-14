package com.google.cloud.teleport.v2.neo4j.common.model;


import com.google.cloud.teleport.v2.neo4j.common.model.enums.SaveMode;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Target implements Serializable, Comparable {

    public String name;
    public boolean active = true;
    public TargetType type;
    public Query query = new Query();
    public List<Mapping> mappings = new ArrayList<>();
    public SaveMode saveMode = SaveMode.append;
    public Map<String,Mapping> mappingByFieldMap =new HashMap();

    public int sequence=0;

    public Target(){}

    public Target(final JSONObject targetObj) {
        this.name = targetObj.getString("name");
        this.active = !targetObj.has("active") || targetObj.getBoolean("active");
        this.type = TargetType.valueOf(targetObj.getString("type"));
        this.saveMode = SaveMode.valueOf(targetObj.getString("save_mode"));

        if (targetObj.has("query")) {
            final JSONObject queryObj = targetObj.getJSONObject("query");
            if (queryObj.has("aggregations")) {
                List<Aggregation> aggregations=new ArrayList<>();
                JSONArray aggregationsArray = queryObj.getJSONArray("aggregations");
                for (int i = 0; i < aggregationsArray.length(); i++) {
                    final JSONObject aggregationObj = aggregationsArray.getJSONObject(i);
                    Aggregation agg=new Aggregation();
                    agg.expression= aggregationObj.getString("expr");
                    agg.field= aggregationObj.getString("field");
                    aggregations.add(agg);
                }
                this.query.aggregations =aggregations;
            }
            this.query.group = queryObj.has("group") && queryObj.getBoolean("group");
            this.query.orderBy = queryObj.has("order_by")?queryObj.getString("order_by"):"";
            this.query.limit = queryObj.has("limit")?queryObj.getInt("limit"):-1;
            this.query.where = queryObj.has("where")?queryObj.getString("where"):"";

        }

        JSONArray mappingsArray = targetObj.getJSONArray("mappings");
        for (int i = 0; i < mappingsArray.length(); i++) {
            final JSONObject mappingObj = mappingsArray.getJSONObject(i);
                final Mapping mapping = new Mapping(this.type,mappingObj);
                this.mappings.add(mapping);
                this.mappingByFieldMap.put(mapping.field, mapping);
        }

    }

    public Mapping getMappingByFieldName(String fieldName){
        return this.mappingByFieldMap.get(fieldName);
    }

    @Override
    public int compareTo(Object o) {
        if (this.type==((Target)o).type) {
            return 0;
        } else if (this.type== TargetType.relationship && ((Target)o).type== TargetType.node) {
            return 1;
        } else {
            return -1;
        }
    }

}
