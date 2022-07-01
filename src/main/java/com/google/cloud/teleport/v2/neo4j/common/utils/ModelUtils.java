package com.google.cloud.teleport.v2.neo4j.common.utils;


import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.teleport.v2.neo4j.common.model.*;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.SourceType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ModelUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ModelUtils.class);
    final static Gson gson = new GsonBuilder().setPrettyPrinting().create();
    final public static String DEFAULT_STAR_QUERY = "SELECT * FROM PCOLLECTION";
    final static String LEGAL_CHARS_REGEX = "[^a-zA-Z0-9_]";
    final static String LEGAL_CHARS_REGEX_SPACE = "[^a-zA-Z0-9_ ]";
    final static String ALPHA_CHARS_REGEX = "[^a-zA-Z]";
    final public static String CYPHER_DELETE_ALL = "CREATE OR REPLACE DATABASE `neo4j`";
    final public static long MAX_ROWS = 10000000000000l;
    final static Pattern variablePattern = Pattern.compile("(\\$([a-zA-Z0-9_]+))");


    public static Target generateDefaultTarget(Source source) throws RuntimeException {
        if (source.sourceType == SourceType.text) {
            Target target = new Target();

            //TODO: create default target (nodes) for source

            return target;
        } else {
            LOG.info("Unhandled source type.");
            throw new RuntimeException("Unhandled source type: " + source.sourceType);
        }
    }

    public static String getRelationshipKeyField(Target target, FragmentType fragmentType) {
        return getFirstField(target, fragmentType, List.of(RoleType.key));
    }

    public static String getFirstField(Target target, FragmentType fragmentType, List<RoleType> roleTypes) {
        List<String> fields = getFields(fragmentType, roleTypes, target);
        if (fields.size() > 0) return fields.get(0);
        return "";
    }

    public static String getFirstFieldOrConstant(Target target, FragmentType fragmentType, List<RoleType> roleTypes) {
        List<String> fieldsOrConstants = getFieldOrConstants(fragmentType, roleTypes, target);
        if (fieldsOrConstants.size() > 0) return fieldsOrConstants.get(0);
        return "";
    }

    public static boolean singleSourceSpec(JobSpecRequest jobSpec) {
        boolean singleSourceQuery = true;
        for (Target target : jobSpec.targets) {
            if (target.active) {
                boolean targetRequiresRequery = ModelUtils.targetHasTransforms(target);
                if (targetRequiresRequery) {
                    singleSourceQuery = false;
                }
            }
        }
        return singleSourceQuery;
    }

    public static boolean targetsHaveTransforms(JobSpecRequest jobSpec, Source source) {
        for (Target target : jobSpec.targets) {
            if (target.active) {
                if (target.source.equals(source.name)) {
                    boolean targetRequiresRequery = ModelUtils.targetHasTransforms(target);
                    if (targetRequiresRequery) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static boolean nodesOnly(JobSpecRequest jobSpec) {
        for (Target target : jobSpec.targets) {
            if (target.active) {
                if (target.type == TargetType.edge) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean relationshipsOnly(JobSpecRequest jobSpec) {
        for (Target target : jobSpec.targets) {
            if (target.active) {
                if (target.type == TargetType.node) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean targetHasTransforms(Target target) {
        boolean requiresRequery = false;
        if (target.transform != null) {
            if (target.transform.group || target.transform.aggregations.size() > 0 || StringUtils.isNotEmpty(target.transform.orderBy) || StringUtils.isNotEmpty(target.transform.where)) {
                return true;
            }
        }
        return requiresRequery;
    }

    public static Set<String> getBqFieldSet(com.google.cloud.bigquery.Schema schema) {
        Set<String> fieldNameMap = new HashSet<>();
        FieldList fieldList = schema.getFields();
        for (Field field : fieldList) {
            fieldNameMap.add(field.getName());
        }
        return fieldNameMap;
    }

    public static Set<String> getBeamFieldSet(Schema schema) {
        return new HashSet<>(schema.getFieldNames());
    }

    public static String getTargetSql(Set<String> fieldNameMap, Target target, boolean generateSqlSort) {
        return getTargetSql(fieldNameMap, target, generateSqlSort, null);
    }

    public static String getTargetSql(Set<String> fieldNameMap, Target target, boolean generateSqlSort, String baseSql) {
        StringBuffer sb = new StringBuffer();

        String orderByClause = "";
        if (target.type == TargetType.edge) {
            String sortField = getRelationshipKeyField(target, FragmentType.target);
            if (StringUtils.isNotBlank(sortField)) {
                orderByClause = " ORDER BY " + sortField + " ASC";
            } else if (StringUtils.isNotBlank(target.transform.orderBy)) {
                orderByClause = " ORDER BY " + target.transform.orderBy;
            }
        } else {
            if (StringUtils.isNotBlank(target.transform.orderBy)) {
                orderByClause = " ORDER BY " + target.transform.orderBy;
            }
        }

        if (target.transform != null) {
            List<String> fieldList = new ArrayList<>();
            /////////////////////////////////
            // Grouping transform
            Transform query = target.transform;
            if (query.group || query.aggregations.size() > 0) {
                for (int i = 0; i < target.mappings.size(); i++) {
                    Mapping mapping = target.mappings.get(i);
                    if (StringUtils.isNotBlank(mapping.field)) {
                        if (fieldNameMap.contains(mapping.field)) {
                            fieldList.add(mapping.field);
                        }
                    }
                }
                sb.append("SELECT " + StringUtils.join(fieldList, ","));
                if (query.aggregations.size() > 0) {
                    for (Aggregation agg : query.aggregations) {
                        sb.append("," + agg.expression + " " + agg.field);
                    }
                }
                sb.append(" FROM PCOLLECTION");
                if (StringUtils.isNotBlank(query.where)) {
                    sb.append(" WHERE " + query.where);
                }
                sb.append(" GROUP BY " + StringUtils.join(fieldList, ","));


                if (StringUtils.isNotEmpty(orderByClause) && generateSqlSort) {
                    LOG.info("Order by clause: " + orderByClause);
                    sb.append(orderByClause);
                    //  ORDER BY without a LIMIT is not supported!
                    if (query.limit > -1) {
                        sb.append(" LIMIT " + query.limit);
                    }
                } else {
                    if (query.limit > -1) {
                        sb.append(" LIMIT " + query.limit);
                    }
                }
            }
        }

        // If edge/relationship, sort by destination nodeId to reduce locking
        String innerSql = null;
        if (sb.length() == 0 && generateSqlSort) {
            innerSql = DEFAULT_STAR_QUERY + orderByClause;
        } else if (sb.length() == 0) {
            innerSql = DEFAULT_STAR_QUERY;
        } else {
            innerSql = sb.toString();
        }
        if (StringUtils.isNotEmpty(baseSql)) {
            return innerSql.replace(" PCOLLECTION", " (" + baseSql + ")");
        } else {
            return innerSql;
        }
    }

    public static boolean messagesContains(List<String> messages, String text){
        for (String msg:messages){
            if (msg.toUpperCase().contains(text.toUpperCase())){
                return true;
            }
        }
        return false;
    }
    public static String makeValidNeo4jIdentifier(String proposedIdString) {
        String finalIdString = proposedIdString.replaceAll(LEGAL_CHARS_REGEX, "_").trim();
        if (finalIdString.substring(0, 1).matches(ALPHA_CHARS_REGEX)) {
            finalIdString = "N" + finalIdString;
        }
        return finalIdString;
    }

    public static String makeValidNeo4jRelationshipIdentifier(String proposedIdString) {
        String finalRelationshipIdString = proposedIdString.replaceAll(LEGAL_CHARS_REGEX, "_").toUpperCase().trim();
        return finalRelationshipIdString;
    }


    public static List<String> getStaticOrDynamicRelationshipType(String dynamicRowPrefix, Target target) {
        StringBuffer sb = new StringBuffer();
        List<String> relationships = new ArrayList<>();
        for (Mapping m : target.mappings) {
            if (m.fragmentType == FragmentType.rel) {
                if (m.role == RoleType.type) {
                    if (StringUtils.isNotEmpty(m.constant)) {
                        relationships.add(m.constant);
                    } else {
                        //TODO: handle dynamic labels
                        relationships.add(dynamicRowPrefix + "." + m.field);
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

    public static List<String> getStaticLabels(FragmentType entityType, Target target) {
        List<String> labels = new ArrayList<>();
        for (Mapping m : target.mappings) {
            if (m.fragmentType == entityType) {
                if (m.labels.size()>0) {
                    labels.addAll(m.labels);
                } else if (m.role == RoleType.label) {
                    if (StringUtils.isNotEmpty(m.constant)) {
                        labels.add(m.constant);
                    } else {
                        // dynamic labels not handled here
                        // labels.add(prefix+"."+m.field);
                    }
                }
            }
        }
        return labels;
    }


    public static List<String> getStaticOrDynamicLabels(String dynamicRowPrefix, FragmentType entityType, Target target) {
        List<String> labels = new ArrayList<>();
        for (Mapping m : target.mappings) {
            if (m.fragmentType == entityType) {
                if (m.labels.size()>0) {
                    labels.addAll(m.labels);
                } else if (m.role == RoleType.label) {
                    if (StringUtils.isNotEmpty(m.constant)) {
                        labels.add(m.constant);
                    } else {
                        labels.add(dynamicRowPrefix + "." + m.field);
                    }
                }
            }
        }
        return labels;
    }

    public static List<String> getFields(FragmentType entityType, List<RoleType> roleTypes, Target target) {
        List<String> fieldNames = new ArrayList<>();
        for (Mapping m : target.mappings) {
            if (m.fragmentType == entityType) {
                if (roleTypes.contains(m.role)) {
                    fieldNames.add(m.field);
                }
            }
        }
        return fieldNames;
    }

    public static List<String> getFieldOrConstants(FragmentType entityType, List<RoleType> roleTypes, Target target) {
        List<String> fieldOrConstants = new ArrayList<>();
        for (Mapping m : target.mappings) {
            if (m.fragmentType == entityType) {
                if (roleTypes.contains(m.role)) {
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

    public static String replaceVariableTokens(String text, HashMap<String, String> replacements) {
        Matcher matcher = variablePattern.matcher(text);
        //populate the replacements map ...
        StringBuilder builder = new StringBuilder();
        int i = 0;
        LOG.info("Replacing variable tokens: "+text);
        while (matcher.find()) {
            String replacement = replacements.get(matcher.group(1));
            builder.append(text.substring(i, matcher.start()));
            if (replacement == null)
                builder.append(matcher.group(0));
            else
                builder.append(replacement);
            i = matcher.end();
        }
        builder.append(text.substring(i, text.length()));
        String repacedText= builder.toString();
        LOG.info("Before: "+text+", after: "+repacedText);
        return repacedText;
    }

    public static List<String> getIndexedProperties(Config config, FragmentType entityType, Target target) {
        List<String> indexedProperties = new ArrayList<>();
        for (Mapping m : target.mappings) {

            if (m.fragmentType == entityType) {
                if (m.role == RoleType.key || m.indexed || config.indexAllProperties) {
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
