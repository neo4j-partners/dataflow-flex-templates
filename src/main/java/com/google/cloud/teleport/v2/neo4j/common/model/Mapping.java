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

public class Mapping implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(Mapping.class);

    public String constant;
    public RoleType role;
    public String name;
    public String field;
    public String description;
    public PropertyType type;
    public String defaultValue = "";
    public boolean mandatory=false;
    public boolean unique=false;
    public boolean indexed=true;
    public FragmentType fragmentType = FragmentType.node;

    public Mapping(TargetType type, final JSONObject mappingObj)  {

        this.constant = mappingObj.has("constant")?mappingObj.getString("constant"):"";
        this.role = mappingObj.has("role")?RoleType.valueOf(mappingObj.getString("role")):RoleType.property;
        this.fragmentType = mappingObj.has("fragment")?FragmentType.valueOf(mappingObj.getString("fragment")):FragmentType.node;

        if (type == TargetType.relationship) {
            if (this.fragmentType == fragmentType.node){
                throw new RuntimeException("Invalid fragment type for relationship target: "+this.fragmentType);
            }
        }
        this.field = mappingObj.has("field")?mappingObj.getString("field"):"";
        this.name = mappingObj.has("name")?mappingObj.getString("name"):"";
        if (StringUtils.isNotEmpty(this.field) && StringUtils.isEmpty(this.name)){
            throw new RuntimeException("Invalid target.  Every field must include a 'name' attribute.");
        }
        // source value is required.
        this.description = mappingObj.has("description")?mappingObj.getString("description"):"";
        this.unique = mappingObj.has("unique")?mappingObj.getBoolean("unique"):false;
        this.indexed = mappingObj.has("indexed")?mappingObj.getBoolean("indexed"):false;
        if (mappingObj.has("type")){
            this.type = PropertyType.valueOf(mappingObj.getString("type"));
        } else {
            // check to see if data type is defined in fields...
           this.type= PropertyType.String;
        }
        this.mandatory = mappingObj.has("mandatory")?mappingObj.getBoolean("mandatory"):false;
        this.defaultValue =  mappingObj.has("default")?mappingObj.get("default")+"":"";
    }
}
