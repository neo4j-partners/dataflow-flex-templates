package com.google.cloud.teleport.v2.neo4j.common.model;


import com.google.cloud.teleport.v2.neo4j.common.model.enums.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Target implements Serializable, Comparable {

    public String source="";
    public String name="";
    public boolean active = true;
    public TargetType type;
    public boolean autoMap=false;
    public Transform transform = new Transform();
    public List<Mapping> mappings = new ArrayList<>();
    public SaveMode saveMode = SaveMode.append;
    public Map<String,Mapping> mappingByFieldMap =new HashMap();
    public List<String> fieldNames=new ArrayList<>();

    public int sequence=0;

    public Target(){}

    public static Target createSimpleNode(String source, String label, String[] propertyNames, PropertyType propertyType, boolean indexed){
        Target target=new Target();
        target.type=TargetType.node;
        target.active=true;
        target.saveMode= SaveMode.append;
        target.source=source;
        Mapping mapping=new Mapping(TargetType.node);

        mapping.constant=label;
        mapping.fragmentType= FragmentType.node;
        mapping.type = PropertyType.String;
        mapping.role= RoleType.label;
        for (String fieldname: propertyNames){
            mapping.field=fieldname;
            mapping.fragmentType=FragmentType.node;
            mapping.type= propertyType;
            mapping.indexed=indexed;
            mapping.unique=false;
            mapping.role=RoleType.property;
        }
        target.mappings.add(mapping);
        return target;
    }
    public Target(final JSONObject targetObj) {
        this.name = targetObj.getString("name");
        this.active = !targetObj.has("active") || targetObj.getBoolean("active");
        this.type = TargetType.valueOf(targetObj.getString("type"));
        this.saveMode = SaveMode.valueOf(targetObj.getString("mode"));
        this.source =  targetObj.has("source")?targetObj.getString("source"):"";
        this.autoMap = !targetObj.has("automap") || targetObj.getBoolean("automap");
        if (targetObj.has("transform")) {
            final JSONObject queryObj = targetObj.getJSONObject("transform");
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
                this.transform.aggregations =aggregations;
            }
            this.transform.group = queryObj.has("group") && queryObj.getBoolean("group");
            this.transform.orderBy = queryObj.has("order_by")?queryObj.getString("order_by"):"";
            this.transform.limit = queryObj.has("limit")?queryObj.getInt("limit"):-1;
            this.transform.where = queryObj.has("where")?queryObj.getString("where"):"";

        }

        JSONArray mappingsArray = targetObj.getJSONArray("mappings");
        for (int i = 0; i < mappingsArray.length(); i++) {
            final JSONObject mappingObj = mappingsArray.getJSONObject(i);
                final Mapping mapping = new Mapping(this.type,mappingObj);
                this.mappings.add(mapping);
                this.mappingByFieldMap.put(mapping.field, mapping);
           this.fieldNames.add(mapping.field);
        }

    }

    public Mapping getMappingByFieldName(String fieldName){
        return this.mappingByFieldMap.get(fieldName);
    }

    @Override
    public int compareTo(Object o) {
        if (this.type==((Target)o).type) {
            return 0;
        } else if (this.type== TargetType.edge && ((Target)o).type== TargetType.node) {
            return 1;
        } else {
            return -1;
        }
    }

}
