package com.google.cloud.teleport.v2.neo4j.common.model;


import com.google.cloud.teleport.v2.neo4j.common.model.enums.MappingType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.SaveMode;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Target implements Serializable, Comparable {

    final static Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static final Logger LOG = LoggerFactory.getLogger(Target.class);
    public String source = "";
    public String name = "";
    public boolean active = true;
    public TargetType type;
    public boolean autoMap;
    public Transform transform = new Transform();
    public List<Mapping> mappings = new ArrayList<>();
    public SaveMode saveMode = SaveMode.append;
    public Map<String, Mapping> mappingByFieldMap = new HashMap();
    public List<String> fieldNames = new ArrayList<>();

    public int sequence;

    public Target() {
    }

    public Target(final JSONObject targetObj) {
        if (targetObj.has("node")) {
            this.type = TargetType.node;
            parseMappingsObject(MappingType.node, targetObj.getJSONObject("node"));
        } else if (targetObj.has("edge")) {
            this.type = TargetType.edge;
            parseMappingsObject(MappingType.edge, targetObj.getJSONObject("edge"));
        } else {
            this.type = TargetType.valueOf(targetObj.getString("type"));
            parseMappingsArray(targetObj);
        }
    }

    public void parseMappingsObject(MappingType mappingType, final JSONObject targetObj) {
        parseHeader(targetObj);
        List<Mapping> mappings = MappingTransposed.parseMappings(mappingType, targetObj.getJSONObject("mappings"));
        for (Mapping mapping : mappings) {
            addMapping(mapping);
        }
    }

    public void parseMappingsArray(final JSONObject targetObj) {
        parseHeader(targetObj);
        JSONArray mappingsArray = targetObj.getJSONArray("mappings");
        for (int i = 0; i < mappingsArray.length(); i++) {
            final JSONObject mappingObj = mappingsArray.getJSONObject(i);
            addMapping(new Mapping(mappingObj));
        }
    }

    private void addMapping(Mapping mapping) {
        this.mappings.add(mapping);
        if (mapping.field != null) {
            this.mappingByFieldMap.put(mapping.field, mapping);
            this.fieldNames.add(mapping.field);
        }
    }

    private void parseHeader(final JSONObject targetObj) {
        this.name = targetObj.getString("name");
        this.active = !targetObj.has("active") || targetObj.getBoolean("active");
        this.saveMode = SaveMode.valueOf(targetObj.getString("mode"));
        this.source = targetObj.has("source") ? targetObj.getString("source") : "";
        this.autoMap = !targetObj.has("automap") || targetObj.getBoolean("automap");
        if (targetObj.has("transform")) {
            final JSONObject queryObj = targetObj.getJSONObject("transform");
            if (queryObj.has("aggregations")) {
                List<Aggregation> aggregations = new ArrayList<>();
                JSONArray aggregationsArray = queryObj.getJSONArray("aggregations");
                for (int i = 0; i < aggregationsArray.length(); i++) {
                    final JSONObject aggregationObj = aggregationsArray.getJSONObject(i);
                    Aggregation agg = new Aggregation();
                    agg.expression = aggregationObj.getString("expr");
                    agg.field = aggregationObj.getString("field");
                    aggregations.add(agg);
                }
                this.transform.aggregations = aggregations;
            }
            this.transform.group = queryObj.has("group") && queryObj.getBoolean("group");
            this.transform.orderBy = queryObj.has("order_by") ? queryObj.getString("order_by") : "";
            this.transform.limit = queryObj.has("limit") ? queryObj.getInt("limit") : -1;
            this.transform.where = queryObj.has("where") ? queryObj.getString("where") : "";
        }
    }

    public Mapping getMappingByFieldName(String fieldName) {
        return this.mappingByFieldMap.get(fieldName);
    }

    @Override
    public int compareTo(Object o) {
        if (this.type == ((Target) o).type) {
            return 0;
        } else if (this.type == TargetType.edge && ((Target) o).type == TargetType.node) {
            return 1;
        } else {
            return -1;
        }
    }

}
