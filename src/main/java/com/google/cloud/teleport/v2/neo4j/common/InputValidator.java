package com.google.cloud.teleport.v2.neo4j.common;

import com.google.cloud.teleport.v2.neo4j.common.model.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.Mapping;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.*;
import com.google.cloud.teleport.v2.neo4j.common.options.Neo4jFlexTemplateOptions;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.util.mapping.Mappings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InputValidator {

    final static Set<String> validOptions=Set.of(
            "relationship",
            "relationship.save.strategy",
            "relationship.source.labels",
            "relationship.source.save.mode",
            "relationship.source.node.keys",
            "relationship.target.labels",
            "relationship.target.node.keys",
            "relationship.target.node.properties",
            "relationship.target.save.mode");

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

        if (jobSpec.source==null){
            validationMessages.add("Source is not defined");
        } else {
            if (StringUtils.isBlank(jobSpec.source.name)){
                validationMessages.add("Source is not named");
            }
        }

        //VALIDATION
        for (Target target:jobSpec.targets) {
            // Check that all targets have names
            if (StringUtils.isBlank(target.name)){
                validationMessages.add("Targets must include a 'name' attribute for debugging and synthetic artifact generation.");
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
                    if (mapping.fragmentType == FragmentType.target || mapping.fragmentType == FragmentType.source  ){
                        if (mapping.role != RoleType.key){
                            validationMessages.add("Invalid role "+mapping.role+" on relationship: "+mapping.fragmentType);
                        }
                        if (StringUtils.isEmpty(mapping.label)){
                            validationMessages.add(mapping.fragmentType+" missing label attribute");
                        }
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

        if (jobSpec.options.size()>0) {
            // check valid options
            Iterator<String> optionIt=jobSpec.options.keySet().iterator();
            while (optionIt.hasNext()){
                String option=optionIt.next();
                if (!validOptions.contains(option)){
                    validationMessages.add("Invalid option specified: "+option);
                }
            }
        }

        if (jobSpec.targets.size()==0 && jobSpec.options.size()>0 && jobSpec.source.fieldNames.length==0) {
            validationMessages.add("Unable to compute target node/edges from jobSpec.  Please define targets, options, our source fieldNames.");
        }

        return validationMessages;
    }





}
