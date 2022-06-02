package com.google.cloud.teleport.v2.neo4j.common;

import com.google.cloud.teleport.v2.neo4j.common.model.Mapping;
import com.google.cloud.teleport.v2.neo4j.common.model.Targets;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.*;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CypherGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(CypherGenerator.class);
    final static String CONST_ROW_VARIABLE_NAME = "rows";

    public static String getUnwindCreateCypher(Targets target) {
        StringBuffer sb = new StringBuffer();
        // Model node creation statement
        //  "UNWIND $rows AS row CREATE(c:Customer { id : row.id, name: row.name, firstName: row.firstName })
        String nodeAlias = "N_" + RandomStringUtils.randomAlphanumeric(8);

        /// RELATIONSHIP TYPE
        if (target.type == TargetType.relationship) {

            /*
            SaveMode.ErrorIfExists builds a CREATE query.
            SaveMode.Append builds a MERGE query.
            If you are using Spark 3, the default save mode ErrorIfExists does not work.
                For SaveMode.Append mode, you need to have unique constraints on the keys.
            */
            SaveMode effectiveSaveMode = target.saveMode;
            if (target.saveMode == SaveMode.append && (getUniqueProperties(FragmentType.source, target).size() == 0 || getUniqueProperties(FragmentType.target, target).size() == 0)) {
                effectiveSaveMode = SaveMode.error_if_exists;
                //For SaveMode.Overwrite mode, you need to have unique constraints on the keys.
            } else if (target.saveMode == SaveMode.overwrite && getUniqueProperties(FragmentType.source, target).size() == 0 || getUniqueProperties(FragmentType.target, target).size() == 0) {
                effectiveSaveMode = SaveMode.error_if_exists;
            }

            // Verb
            if (effectiveSaveMode == SaveMode.append || effectiveSaveMode == SaveMode.overwrite) { // merge
                sb.append("UNWIND $" + CONST_ROW_VARIABLE_NAME + " AS row ");
                //MERGE (variable1:Label1 {nodeProperties1})-[:REL_TYPE]->
                //(variable2:Label2 {nodeProperties2})
                // MATCH before MERGE
                sb.append(" MATCH (" + getLabelsPropertiesList("source", true, FragmentType.source, Arrays.asList(RoleType.key,RoleType.property), target) + ")");
                sb.append(" MATCH (" + getLabelsPropertiesList("target", true, FragmentType.target, Arrays.asList(RoleType.key,RoleType.property), target) + ")");
                sb.append("MERGE (from)");
                sb.append(" -[" + getRelationshipTypePropertiesList("rel", false, target) + "]-> ");
                sb.append("(to)");
                // SET properties...
            } else if (effectiveSaveMode == SaveMode.error_if_exists) { // Fast, blind create
                sb.append("UNWIND $" + CONST_ROW_VARIABLE_NAME + " AS row CREATE ");
                sb.append("(" + getLabelsPropertiesList("source", false, FragmentType.source, Arrays.asList(RoleType.key,RoleType.property), target) + ")");
                sb.append(" -[" + getRelationshipTypePropertiesList("rel", false, target) + "]-> ");
                sb.append("(" + getLabelsPropertiesList("target", false, FragmentType.target, Arrays.asList(RoleType.key,RoleType.property), target) + ")");
                // SET properties
            } else  if (effectiveSaveMode == SaveMode.match) { // Update
                sb.append("UNWIND $" + CONST_ROW_VARIABLE_NAME + " AS row CREATE ");
                sb.append(" MATCH (" + getLabelsPropertiesList("source", true, FragmentType.source, Arrays.asList(RoleType.key,RoleType.property), target) + ")");
                sb.append(" , (" + getLabelsPropertiesList("target", true, FragmentType.target, Arrays.asList(RoleType.key,RoleType.property), target) + ")");
                sb.append(" MERGE (from)");
                sb.append(" -[" + getRelationshipTypePropertiesList("rel", false, target) + "]-> ");
                sb.append(" (to)");
            } else {
                LOG.error("Unhandled saveMode: " + target.saveMode);
            }


            // NODE TYPE
        } else if (target.type == TargetType.node) {

            /*
            SaveMode.ErrorIfExists builds a CREATE query.
            SaveMode.Overwrite builds a MERGE query.
            For SaveMode.Overwrite mode, you need to have unique constraints on the keys.
             */

            SaveMode effectiveSaveMode = target.saveMode;
            if (target.saveMode == SaveMode.append && getUniqueProperties(FragmentType.node, target).size() == 0) {
                effectiveSaveMode = SaveMode.error_if_exists;
                //For SaveMode.Overwrite mode, you need to have unique constraints on the keys.
            } else if (target.saveMode == SaveMode.overwrite && getUniqueProperties(FragmentType.node, target).size() == 0) {
                effectiveSaveMode = SaveMode.error_if_exists;
            }

            // Verb
            if (effectiveSaveMode == SaveMode.append || effectiveSaveMode == SaveMode.overwrite) { // merge
                sb.append("UNWIND $" + CONST_ROW_VARIABLE_NAME + " AS row ");
                // MERGE clause represents matching properties
                // MERGE (charlie {name: 'Charlie Sheen', age: 10})  A new node with the name 'Charlie Sheen' will be created since not all properties matched the existing 'Charlie Sheen' node.
                sb.append("MERGE (" + getLabelsPropertiesList("n", true, FragmentType.node, Arrays.asList(RoleType.key), target) + ")");
                String nodePropertyMapStr = getPropertiesList(FragmentType.node, true, Arrays.asList(RoleType.property), target);
                if (nodePropertyMapStr.length() > 0) {
                    sb.append("SET n+=" + nodePropertyMapStr);
                }
            } else if (effectiveSaveMode == SaveMode.error_if_exists) { // fast create
                sb.append("UNWIND $" + CONST_ROW_VARIABLE_NAME + " AS row ");
                sb.append("CREATE (" + getLabelsPropertiesList("n", false, FragmentType.node, Arrays.asList(RoleType.key, RoleType.property), target) + ")");
            } else  if (effectiveSaveMode == SaveMode.match) { // update only (this is not implemented in Spark connector)
                sb.append("UNWIND $" + CONST_ROW_VARIABLE_NAME + " AS row ");
                // MERGE clause represents matching properties
                // MERGE (charlie {name: 'Charlie Sheen', age: 10})  A new node with the name 'Charlie Sheen' will be created since not all properties matched the existing 'Charlie Sheen' node.
                sb.append("MERGE (" + getLabelsPropertiesList("n", true, FragmentType.node, Arrays.asList(RoleType.key), target) + ")");
                String nodePropertyMapStr = getPropertiesList(FragmentType.node, true, Arrays.asList(RoleType.property), target);
                if (nodePropertyMapStr.length() > 0) {
                    sb.append(" ON MATCH SET =" + nodePropertyMapStr);
                }
            } else {
                LOG.error("Unhandled saveMode: " + target.saveMode);
            }
        } else {
            throw new RuntimeException("Unhandled target type: " + target.type);
        }
        return sb.toString();
    }

    public static String getLabelsPropertiesList(String nodePrefix, boolean onlyIndexedProperties, FragmentType entityType, List<RoleType> roleTypes, Targets target) {
        StringBuffer sb = new StringBuffer();
        List<String> labels = getLabels(entityType, target);
        String propertiesKeyListStr = getPropertiesList(entityType, onlyIndexedProperties, roleTypes, target);
        // Labels
        if (labels.size() > 0) {
            sb.append(nodePrefix);
            for (String label : labels) {
                sb.append(":" + Neo4jUtils.makeValidNeo4jIdentifier(label));
            }
        } else if (StringUtils.isNotEmpty(target.name)) {
            sb.append(nodePrefix);
            sb.append(":" + Neo4jUtils.makeValidNeo4jIdentifier(target.name));
        } else {
            sb.append(nodePrefix);
        }
        if (StringUtils.isNotEmpty(propertiesKeyListStr)) {
            sb.append(" " + propertiesKeyListStr);
        }
        return sb.toString();
    }

    public static String getPropertiesList(FragmentType entityType, boolean onlyIndexedProperties, List<RoleType> roleTypes, Targets target) {
        StringBuffer sb = new StringBuffer();
        int targetColCount = 0;
        for (Mapping m : target.mappings) {
            if (m.fragmentType == entityType) {
                if (roleTypes.contains(m.role) && (!onlyIndexedProperties || m.indexed)) {
                    if (targetColCount > 0) sb.append(",");
                    if (StringUtils.isNotEmpty(m.constant)) {
                        sb.append(Neo4jUtils.makeValidNeo4jIdentifier(m.name) + ": '" + m.constant + "'");
                    } else {
                        sb.append(Neo4jUtils.makeValidNeo4jIdentifier(m.name) + ": row." + m.field);
                    }
                    targetColCount++;
                }
            }
        }
        if (sb.length() > 0) {
            return "{" + sb.toString() + "}";
        }
        return "";
    }


    public static Map<String, Object> getUnwindRowDataMapper(Row row, Targets target) {

        Map<String, Object> map = new HashMap();
        for (Mapping m : target.mappings) {
            String fieldName = m.field;
            PropertyType dataColPropertyType = m.type;
            //lookup data type
            if (StringUtils.isNotEmpty(m.constant)) {
                if (StringUtils.isNotEmpty(m.name)) {
                    map.put(m.name, m.constant);
                } else {
                    map.put(m.constant, m.constant);
                }
            } else {
                if (dataColPropertyType == PropertyType.Integer) {
                    map.put(m.field, row.getInt32(fieldName));
                } else if (dataColPropertyType == PropertyType.Float) {
                    map.put(m.field, row.getFloat(fieldName));
                } else if (dataColPropertyType == PropertyType.Long) {
                    map.put(m.field, row.getInt64(fieldName));
                } else if (dataColPropertyType == PropertyType.Boolean) {
                    map.put(m.field, row.getBoolean(fieldName));
                } else if (dataColPropertyType == PropertyType.ByteArray) {
                    map.put(m.field, row.getBytes(fieldName));
                } else if (dataColPropertyType == PropertyType.Point) {
                    map.put(m.field, row.getString(fieldName));
                } else if (dataColPropertyType == PropertyType.Duration) {
                    map.put(m.field, row.getDecimal(fieldName));
                } else if (dataColPropertyType == PropertyType.Date) {
                    map.put(m.field, row.getDateTime(fieldName));
                } else if (dataColPropertyType == PropertyType.LocalDateTime) {
                    map.put(m.field, row.getDateTime(fieldName));
                        } else if (dataColPropertyType == PropertyType.DateTime) {
                    map.put(m.field, row.getDateTime(fieldName));
                            //TODO: how to model time?
                        } else if (dataColPropertyType == PropertyType.LocalTime) {
                    map.put(m.field, row.getFloat(fieldName));
                        } else if (dataColPropertyType == PropertyType.Time) {
                    map.put(m.field, row.getFloat(fieldName));
                    } else {
                    map.put(m.field, row.getString(fieldName));
                    }
                }
        }

        return map;
    }

    public static List<String> getNodeIndexAndConstraintsCypherStatements(Targets target) {

        List<String> cyphers = new ArrayList<>();
        // Model node creation statement
        //  "UNWIND $rows AS row CREATE(c:Customer { id : row.id, name: row.name, firstName: row.firstName })
        //derive labels
        List<String> labels = getLabels(FragmentType.node, target);
        List<String> indexedProperties = getIndexedProperties(FragmentType.node, target);
        List<String> uniqueProperties = getUniqueProperties(FragmentType.node, target);
        List<String> mandatoryProperties = getRequiredProperties(FragmentType.node, target);
        List<String> nodeKeyProperties = getNodeKeyProperties(FragmentType.node, target);


        for (String indexedProperty : indexedProperties) {
            cyphers.add("CREATE INDEX IF NOT EXISTS FOR (t:" + StringUtils.join(labels, ":") + ") ON (t." + indexedProperty + ")");
        }
        for (String uniqueProperty : uniqueProperties) {
            cyphers.add("CREATE CONSTRAINT IF NOT EXISTS FOR (n:" + StringUtils.join(labels, ":") + ") REQUIRE n." + uniqueProperty + " IS UNIQUE");
        }
        for (String mandatoryProperty : mandatoryProperties) {
            cyphers.add("CREATE CONSTRAINT IF NOT EXISTS FOR (n:" + StringUtils.join(labels, ":") + ") REQUIRE n." + mandatoryProperty + " IS NOT NULL");
        }
        for (String nodeKeyProperty : nodeKeyProperties) {
            cyphers.add("CREATE CONSTRAINT IF NOT EXISTS FOR (n:" + StringUtils.join(labels, ":") + ") REQUIRE n." + nodeKeyProperty + " IS NODE KEY");
        }

        return cyphers;
    }


    private static String getRelationshipTypePropertiesList(String prefix, boolean onlyIndexedProperties, Targets target) {
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
            relationships.add(Neo4jUtils.makeValidNeo4jRelationshipIdentifier(target.name));
        }
        sb.append(prefix + ":" + StringUtils.join(relationships, ":"));
        sb.append(" " + getPropertiesList(FragmentType.rel, onlyIndexedProperties, Arrays.asList(RoleType.key, RoleType.property), target));
        return sb.toString();
    }

    private static List<String> getLabels(FragmentType entityType, Targets target) {
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


    private static List<String> getIndexedProperties(FragmentType entityType, Targets target) {
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

    private static List<String> getUniqueProperties(FragmentType entityType, Targets target) {
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

    private static List<String> getRequiredProperties(FragmentType entityType, Targets target) {
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

    private static List<String> getNodeKeyProperties(FragmentType entityType, Targets target) {
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
