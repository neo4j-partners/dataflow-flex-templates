package com.google.cloud.teleport.v2.neo4j.common.model.helpers;

import com.google.cloud.teleport.v2.neo4j.common.model.enums.ActionExecuteAfter;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.SaveMode;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.common.model.job.Aggregation;
import com.google.cloud.teleport.v2.neo4j.common.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.common.model.job.Target;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;

public class TargetMapper {

    public static Target fromJson(final JSONObject targetObj) {
        Target target=new Target();
        if (targetObj.has("node")) {
            target.type=TargetType.node;
            parseMappingsObject(target, targetObj.getJSONObject("node"));
        } else if (targetObj.has("edge")) {
            target.type=TargetType.edge;
            parseMappingsObject(target, targetObj.getJSONObject("edge"));
        } else {
            target.type=TargetType.valueOf(targetObj.getString("type"));
            parseMappingsArray(target, targetObj);
        }
        return target;
    }

    private static void parseMappingsObject(Target target, final JSONObject targetObj) {
        parseHeader(target,targetObj);
        List<Mapping> mappings = TransposedMappingMapper.parseMappings(target, targetObj.getJSONObject("mappings"));
        for (Mapping mapping : mappings) {
            addMapping(target, mapping);
        }
    }

    public static void parseMappingsArray(Target target, final JSONObject targetObj) {

        parseHeader(target,targetObj);
        JSONArray mappingsArray = targetObj.getJSONArray("mappings");
        for (int i = 0; i < mappingsArray.length(); i++) {
            final JSONObject mappingObj = mappingsArray.getJSONObject(i);
            addMapping(target, VerboseMappingMapper.fromJsonObject(mappingObj));
        }
    }

    private static void addMapping(Target target, Mapping mapping) {
        target.mappings.add(mapping);
        if (mapping.field != null) {
            target.mappingByFieldMap.put(mapping.field, mapping);
            target.fieldNames.add(mapping.field);
        }
    }

    private static void parseHeader(Target target, final JSONObject targetObj) {
        target.name = targetObj.getString("name");
        target.active = !targetObj.has("active") || targetObj.getBoolean("active");
        target.saveMode = SaveMode.valueOf(targetObj.getString("mode"));
        target.source = targetObj.has("source") ? targetObj.getString("source") : "";
        target.autoMap = !targetObj.has("automap") || targetObj.getBoolean("automap");
        if (targetObj.has("execute_after")) {
            target.executeAfter = ActionExecuteAfter.valueOf(targetObj.getString("execute_after"));
        } else {
            if (target.type==TargetType.node){
                //this will not wait for anything...
                target.executeAfter= ActionExecuteAfter.sources;
            } else if (target.type==TargetType.edge){
                target.executeAfter= ActionExecuteAfter.nodes;
            }
        }
        target.executeAfterName = targetObj.has("execute_after_name")?targetObj.getString("execute_after_name"):"";

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
                target.transform.aggregations = aggregations;
            }
            target.transform.group = queryObj.has("group") && queryObj.getBoolean("group");
            target.transform.orderBy = queryObj.has("order_by") ? queryObj.getString("order_by") : "";
            target.transform.limit = queryObj.has("limit") ? queryObj.getInt("limit") : -1;
            target.transform.where = queryObj.has("where") ? queryObj.getString("where") : "";
        }
    }
}
