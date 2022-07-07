package com.google.cloud.teleport.v2.neo4j.common.model.helpers;

import com.google.cloud.teleport.v2.neo4j.common.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.PropertyType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.common.model.job.Mapping;
import java.util.Arrays;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

public class VerboseMappingMapper {
    public static Mapping fromJsonObject(final JSONObject mappingObj){
        Mapping mapping=new Mapping();
        mapping.labels = Arrays.asList(mappingObj.has("label") ? mappingObj.getString("label") : "");
        mapping.constant = mappingObj.has("constant") ? mappingObj.getString("constant") : "";
        mapping.role = mappingObj.has("role") ? RoleType.valueOf(mappingObj.getString("role")) : mapping.role;
        mapping.fragmentType = mappingObj.has("fragment") ? FragmentType.valueOf(mappingObj.getString("fragment")) : mapping.fragmentType;

        mapping.field = mappingObj.has("field") ? mappingObj.getString("field") : "";
        mapping.name = mappingObj.has("name") ? mappingObj.getString("name") : "";
        if (StringUtils.isNotEmpty(mapping.field) && StringUtils.isEmpty(mapping.name)) {
            throw new RuntimeException("Invalid target.  Every field must include a 'name' attribute.");
        }
        // source value is required.
        mapping.description = mappingObj.has("description") ? mappingObj.getString("description") : "";
        mapping.unique = mappingObj.has("unique") && mappingObj.getBoolean("unique");
        mapping.indexed = mappingObj.has("indexed") && mappingObj.getBoolean("indexed");
        if (mapping.role == RoleType.key) {
            mapping.unique = true;
            mapping.indexed = true;
        }
        if (mappingObj.has("type")) {
            mapping.type = PropertyType.valueOf(mappingObj.getString("type"));
        } else {
            // check to see if data type is defined in fields...
            mapping.type = PropertyType.String;
        }
        mapping.mandatory = mappingObj.has("mandatory") && mappingObj.getBoolean("mandatory");
        mapping.defaultValue = mappingObj.has("default") ? mappingObj.get("default") + "" : "";
        return mapping;
    }
}
