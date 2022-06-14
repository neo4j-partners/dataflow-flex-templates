package com.google.cloud.teleport.v2.neo4j.common;

import com.google.cloud.teleport.v2.neo4j.common.model.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.Mapping;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.common.options.Neo4jFlexTemplateOptions;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InputValidator {

    final static Pattern ORDER_BY_PATTERN=Pattern.compile(".*ORDER\\sBY.*");

    private static final Logger LOG = LoggerFactory.getLogger(InputValidator.class);

    final static Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public static List<String> validateNeo4jPipelineOptions(Neo4jFlexTemplateOptions pipelineOptions)  {

        List<String> validationMessages =new ArrayList();

        if (StringUtils.isEmpty(pipelineOptions.getNeo4jConnectionUri())) {
            validationMessages.add("Neo4j connection URI not provided.");
        }

        if (StringUtils.isEmpty(pipelineOptions.getJobSpecUri())) {
            validationMessages.add("Job spec URI not provided.");
        }

        ConnectionParams neoConnection = new ConnectionParams( pipelineOptions.getNeo4jConnectionUri());
        validationMessages.addAll(InputValidator.validateNeo4jConnection(neoConnection));

        JobSpecRequest jobSpec = new JobSpecRequest(pipelineOptions.getJobSpecUri());

        validationMessages.addAll(InputValidator.validateJobSpec(jobSpec));
        return validationMessages;
    }


    private static List<String> validateNeo4jConnection(ConnectionParams connectionParams) {
        List<String> validationMessages=new ArrayList<>();
        if (StringUtils.isEmpty(connectionParams.serverUrl)){
            validationMessages.add("Missing connection server URL");
        }
        if (StringUtils.isEmpty(connectionParams.username)){
            validationMessages.add("Missing connection username");
        }
        if (StringUtils.isEmpty(connectionParams.password)){
            validationMessages.add("Missing connection password");
        }
        return validationMessages;
    }

    private static List<String> validateJobSpec(JobSpecRequest jobSpec){

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

        return validationMessages;
    }


    public static void refactorJobSpec(JobSpecRequest jobSpec){

        //NODES first then relationships
        Collections.sort(jobSpec.targets);

        //number and name targets
        int targetNum=0;
        for (Target target:jobSpec.targets){
            targetNum++;
            target.sequence=targetNum;
            if (StringUtils.isEmpty(target.name)){
                target.name = "Target "+targetNum;
            }
        }

    }


}
