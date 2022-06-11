package com.google.cloud.teleport.v2.neo4j.common;

import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.Mapping;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JobSpecOptimizer {

    final static Pattern ORDER_BY_PATTERN=Pattern.compile(".*ORDER\\sBY.*");

    private static final Logger LOG = LoggerFactory.getLogger(JobSpecOptimizer.class);

    final static Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public static Validations validateAndOptimize(JobSpecRequest jobSpec){

        List<String> validationMessages=new ArrayList<>();

        //VALIDATION
        for (Target target:jobSpec.targets) {
            // Check that all targets have names
            if (StringUtils.isBlank(target.name)){
                validationMessages.add("Targets must include a 'name' attribute for debugging.");
            }
            // Check that SQL does not have order by...
            if (target.query!=null && StringUtils.isNotBlank(target.query.sql)){
                if (target.query.sql.toUpperCase().matches("")){
                    Matcher m = ORDER_BY_PATTERN.matcher(target.query.sql);
                    if (m.find()){
                        validationMessages.add("SQL contains ORDER BY which is not supported");
                    }
                }
            }
            if (target.type == TargetType.relationship) {
                for (Mapping mapping:target.mappings){
                    if (mapping.fragmentType == FragmentType.node){
                        validationMessages.add("Invalid fragment type "+mapping.fragmentType+" for node mapping: "+mapping.name);
                    }
                }
                //relationship validation checks..
                if (StringUtils.isBlank(ModelUtils.getFirstFieldOrConstant(target, FragmentType.source, List.of(RoleType.key)))){
                    validationMessages.add("Could not find target key field for relationship: "+target.name);
                }
                if (StringUtils.isBlank(ModelUtils.getFirstFieldOrConstant(target, FragmentType.target, List.of(RoleType.key)))){
                    validationMessages.add("Could not find target key field for relationship: "+target.name);
                }
                if (StringUtils.isBlank(ModelUtils.getRelationshipKeyField( target, FragmentType.rel))){
                    validationMessages.add("Could not find relation name: "+target.name);
                }
            } else if (target.type == TargetType.node) {
                for (Mapping mapping:target.mappings){
                    if (mapping.fragmentType != FragmentType.node){
                        validationMessages.add("Invalid fragment type "+mapping.fragmentType+" for node mapping: "+mapping.name);
                    }
                }
                if (StringUtils.isBlank(ModelUtils.getFirstFieldOrConstant(target, FragmentType.node, List.of(RoleType.label)))){
                    LOG.info("Invalid target: "+gson.toJson(target));
                    validationMessages.add("Missing label in node: "+target.name);
                }
                if (StringUtils.isBlank(ModelUtils.getFirstFieldOrConstant(target, FragmentType.node, List.of(RoleType.key)))){
                    validationMessages.add("Missing key field in node: "+target.name);
                }
            }
        }

        //NODES first then relationships
        Collections.sort(jobSpec.targets);
        //sort so relationships are executed after nodes
        Validations validationResponse=new Validations();
        validationResponse.errors=validationMessages.size()>0;
        validationResponse.validationMessages=validationMessages;
        return validationResponse;
    }


}
