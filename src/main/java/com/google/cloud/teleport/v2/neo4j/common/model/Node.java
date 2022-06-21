package com.google.cloud.teleport.v2.neo4j.common.model;

import com.google.cloud.teleport.v2.neo4j.common.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.PropertyType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class Node implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(Node.class);
    public TargetType targetType=TargetType.node;
    public String constant;
    public RoleType role=RoleType.property;
    public String name;
    public String label;
    public String field;
    public String description;
    public PropertyType type;
    public String defaultValue = "";
    public boolean mandatory=false;
    public boolean unique=false;
    public boolean indexed=true;
    public FragmentType fragmentType = FragmentType.node;

    public Node(TargetType type){
        this.targetType=type;
    }

    public Node(TargetType type, final JSONObject mappingObj)  {
        this.targetType=type;
        this.label = mappingObj.has("label")?mappingObj.getString("label"):"";
        this.constant = mappingObj.has("constant")?mappingObj.getString("constant"):"";
        this.role = mappingObj.has("role")?RoleType.valueOf(mappingObj.getString("role")):role;
        this.fragmentType = mappingObj.has("fragment")?FragmentType.valueOf(mappingObj.getString("fragment")):fragmentType;

        this.field = mappingObj.has("field")?mappingObj.getString("field"):"";
        this.name = mappingObj.has("name")?mappingObj.getString("name"):"";
        if (StringUtils.isNotEmpty(this.field) && StringUtils.isEmpty(this.name)){
            throw new RuntimeException("Invalid target.  Every field must include a 'name' attribute.");
        }
        // source value is required.
        this.description = mappingObj.has("description")?mappingObj.getString("description"):"";
        this.unique = mappingObj.has("unique") && mappingObj.getBoolean("unique");
        this.indexed = mappingObj.has("indexed") && mappingObj.getBoolean("indexed");
        if (this.role==RoleType.key){
            this.unique=true;
            this.indexed=true;
        }
        if (mappingObj.has("type")){
            this.type = PropertyType.valueOf(mappingObj.getString("type"));
        } else {
            // check to see if data type is defined in fields...
           this.type= PropertyType.String;
        }
        this.mandatory = mappingObj.has("mandatory") && mappingObj.getBoolean("mandatory");
        this.defaultValue =  mappingObj.has("default")?mappingObj.get("default")+"":"";
    }
}
