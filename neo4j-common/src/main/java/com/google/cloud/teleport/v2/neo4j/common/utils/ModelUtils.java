package com.google.cloud.teleport.v2.neo4j.common.utils;


import com.google.cloud.teleport.v2.neo4j.common.model.*;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.SourceType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ModelUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ModelUtils.class);

    final public static String DEFAULT_STAR_QUERY = "SELECT * FROM PCOLLECTION";
    final static String LEGAL_CHARS_REGEX = "[^a-zA-Z0-9_]";
    final static String LEGAL_CHARS_REGEX_SPACE="[^a-zA-Z0-9_ ]";
    final static String ALPHA_CHARS_REGEX="[^a-zA-Z]";
    final public static String CYPHER_DELETE_ALL = "MATCH (n) DETACH DELETE n";
    final public static long MAX_ROWS = 10000000000000l;


    public static Target generateDefaultTarget(Source source) throws RuntimeException{
        if (source.sourceType== SourceType.text){
            Target target=new Target();

            //TODO: create default target (nodes) for source

            return target;
        } else {
            LOG.info("Unhandled source type.");
            throw new RuntimeException("Unhandled source type: " + source.sourceType);
        }
    }

    public static String getRelationshipKeyField(Target target,FragmentType fragmentType){
        return getFirstField(target,fragmentType,List.of(RoleType.key));
    }

    public static String getFirstField(Target target,FragmentType fragmentType, List<RoleType> roleTypes){
        List<String> fields=getFields( fragmentType,roleTypes, target);
        if (fields.size()>0) return fields.get(0);
        return "";
    }

    public static String getFirstFieldOrConstant(Target target,FragmentType fragmentType, List<RoleType> roleTypes){
        List<String> fieldsOrConstants=getFieldOrConstants(  fragmentType,roleTypes, target);
        if (fieldsOrConstants.size()>0) return fieldsOrConstants.get(0);
        return "";
    }

    public static String getTargetSql(Schema sourceSchema, Target target) {
        StringBuffer sb = new StringBuffer();

        String sortByClause ="";
        if (target.type== TargetType.relationship){
            String sortField=getRelationshipKeyField(target,FragmentType.target);
            if (sb.length() == 0 && StringUtils.isNotBlank(sortField)) {
                sortByClause = " ORDER BY "+sortField+" ASC";
            }
        }
        if (target.query != null) {
            List<String> fieldList = new ArrayList<>();
            /////////////////////////////////
            // Grouping transform
            if (target.query != null) {
                Query query = target.query;
                if (query.group || query.aggregations.size() > 0) {
                    for (int i = 0; i < target.mappings.size(); i++) {
                        Mapping mapping = target.mappings.get(i);
                        if (StringUtils.isNotBlank(mapping.field)) {
                            if (sourceSchema.hasField(mapping.field)){
                               fieldList.add(mapping.field);
                            }
                        }
                    }
                    sb.append("SELECT "+StringUtils.join(fieldList,","));
                    if (query.aggregations.size() > 0){
                        for (Aggregation agg:query.aggregations){
                            sb.append(","+agg.expression+" "+agg.field);
                        }
                    }
                    sb.append(" FROM PCOLLECTION");
                    if (StringUtils.isNotBlank(query.where)) {
                        sb.append(" WHERE "+query.where);
                    }
                    sb.append(" GROUP BY "+StringUtils.join(fieldList,","));

                    if (StringUtils.isNotEmpty(sortByClause)){
                        sb.append(sortByClause);
                        //  ORDER BY without a LIMIT is not supported!
                        if (query.limit>-1){
                            sb.append(" LIMIT "+query.limit);
                        } else {
                            sb.append(" LIMIT "+MAX_ROWS);
                        }
                    } else {
                        if (query.limit>-1){
                            sb.append(" LIMIT "+query.limit);
                        }
                    }

                }
            }
        }

        // If edge/relationship, sort by destination nodeId to reduce locking
        if (sb.length() == 0) {
            return DEFAULT_STAR_QUERY+sortByClause;
        } else {
            return sb.toString();
        }
    }

    public static String makeValidNeo4jIdentifier(String proposedIdString) {
        String finalIdString = proposedIdString.replaceAll(LEGAL_CHARS_REGEX, "_").trim();
        if (finalIdString.substring(0, 1).matches(ALPHA_CHARS_REGEX)) {
            finalIdString = "N" + finalIdString;
        }
        return finalIdString;
    }

    public static String makeValidNeo4jRelationshipIdentifier(String proposedIdString) {
        String finalRelationshipIdString=proposedIdString.replaceAll(LEGAL_CHARS_REGEX, "_").toUpperCase().trim();
        return finalRelationshipIdString;
    }


    public static List<String> getRelationshipTypeFields(Target target) {
        StringBuffer sb = new StringBuffer();
        List<String> relationships = new ArrayList<>();
        for (Mapping m : target.mappings) {
            if (m.fragmentType == FragmentType.rel) {
                if (m.role == RoleType.type) {
                    if (StringUtils.isNotEmpty(m.constant)) {
                        relationships.add(m.constant);
                    } else {
                        //TODO: handle dynamic labels
                        relationships.add(m.field);
                    }
                }
            }
        }
        if (relationships.isEmpty()) {
            // if relationship labels are not defined, use target name
            relationships.add(ModelUtils.makeValidNeo4jRelationshipIdentifier(target.name));
        }
        return relationships;
    }

    public static List<String> getLabels(FragmentType entityType, Target target) {
        List<String> labels = new ArrayList<>();
        for (Mapping m : target.mappings) {
            if (m.fragmentType == entityType) {
                if (m.role == RoleType.label) {
                    if (StringUtils.isNotEmpty(m.constant)) {
                        labels.add(m.constant);
                    } else {
                        //TODO: handle dynamic labels
                        labels.add(m.field);
                    }
                }
            }
        }
        return labels;
    }

    public static List<String> getFields(FragmentType entityType,List<RoleType> roleTypes, Target target) {
        List<String> fieldNames=new ArrayList<>();
        for (Mapping m : target.mappings) {
            if (m.fragmentType == entityType) {
                if (roleTypes.contains(m.role)){
                    fieldNames.add(m.field);
                }
            }
        }
        return fieldNames;
    }

    public static List<String> getFieldOrConstants(FragmentType entityType,List<RoleType> roleTypes, Target target) {
        List<String> fieldOrConstants=new ArrayList<>();
        for (Mapping m : target.mappings) {
            if (m.fragmentType == entityType) {
                if (roleTypes.contains(m.role)){
                    if (StringUtils.isNotBlank(m.constant)) {
                        fieldOrConstants.add(m.constant);
                    } else if (StringUtils.isNotBlank(m.field)) {
                        fieldOrConstants.add(m.field);
                    }
                }
            }
        }
        return fieldOrConstants;
    }

    public static List<String> getIndexedProperties(FragmentType entityType, Target target) {
        List<String> indexedProperties = new ArrayList<>();
        for (Mapping m : target.mappings) {

            if (m.fragmentType == entityType) {
                if (m.role == RoleType.key || m.indexed) {
                    indexedProperties.add(m.field);
                }
            }
        }
        return indexedProperties;
    }

    public static List<String> getUniqueProperties(FragmentType entityType, Target target) {
        List<String> uniqueProperties = new ArrayList<>();
        for (Mapping m : target.mappings) {

            if (m.fragmentType == entityType) {
                if (m.unique || m.role == RoleType.key) {
                    uniqueProperties.add(m.field);
                }
            }
        }
        return uniqueProperties;
    }

    public static List<String> getRequiredProperties(FragmentType entityType, Target target) {
        List<String> mandatoryProperties = new ArrayList<>();
        for (Mapping m : target.mappings) {

            if (m.fragmentType == entityType) {
                if (m.mandatory) {
                    mandatoryProperties.add(m.field);
                }
            }
        }
        return mandatoryProperties;
    }

    public static List<String> getNodeKeyProperties(FragmentType entityType, Target target) {
        List<String> nodeKeyProperties = new ArrayList<>();
        for (Mapping m : target.mappings) {
            if (m.fragmentType == entityType) {
                if (m.role == RoleType.key) {
                    nodeKeyProperties.add(m.field);
                }
            }
        }
        return nodeKeyProperties;
    }

}
