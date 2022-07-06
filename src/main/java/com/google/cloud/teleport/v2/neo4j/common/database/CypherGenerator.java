package com.google.cloud.teleport.v2.neo4j.common.database;

import com.google.cloud.teleport.v2.neo4j.common.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.SaveMode;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.common.model.job.Config;
import com.google.cloud.teleport.v2.neo4j.common.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.common.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Generates cypher based on model metadata.
 * TODO: Needs to be refactored to use DSL
 */
public class CypherGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(CypherGenerator.class);
    private static final String CONST_ROW_VARIABLE_NAME = "rows";

    public static String getUnwindCreateCypher(Target target) {
        StringBuffer sb = new StringBuffer();
        // Model node creation statement
        //  "UNWIND $rows AS row CREATE(c:Customer { id : row.id, name: row.name, firstName: row.firstName })
        String nodeAlias = "N_" + RandomStringUtils.randomAlphanumeric(8);

        /// RELATIONSHIP TYPE
        if (target.type == TargetType.edge) {

            // Verb
            if (target.saveMode == SaveMode.merge) { // merge
                sb.append("UNWIND $" + CONST_ROW_VARIABLE_NAME + " AS row ");
                //MERGE (variable1:Label1 {nodeProperties1})-[:REL_TYPE]->
                //(variable2:Label2 {nodeProperties2})
                // MATCH before MERGE
                sb.append(" MATCH (" + getLabelsPropertiesListCypherFragment("source", true, FragmentType.source, Arrays.asList(RoleType.key), target) + ")");
                sb.append(" MATCH (" + getLabelsPropertiesListCypherFragment("target", true, FragmentType.target, Arrays.asList(RoleType.key), target) + ")");
                sb.append(" MERGE (source)");
                sb.append(" -[" + getRelationshipTypePropertiesListFragment("rel", false, target) + "]-> ");
                sb.append("(target)");
                // SET properties...
            } else if (target.saveMode == SaveMode.append) { // Fast, blind create
                sb.append("UNWIND $" + CONST_ROW_VARIABLE_NAME + " AS row CREATE ");
                sb.append("(" + getLabelsPropertiesListCypherFragment("source", false, FragmentType.source, Arrays.asList(RoleType.key, RoleType.property), target) + ")");
                sb.append(" -[" + getRelationshipTypePropertiesListFragment("rel", false, target) + "]-> ");
                sb.append("(" + getLabelsPropertiesListCypherFragment("target", false, FragmentType.target, Arrays.asList(RoleType.key, RoleType.property), target) + ")");
            } else {
                LOG.error("Unhandled saveMode: " + target.saveMode);
            }


            // NODE TYPE
        } else if (target.type == TargetType.node) {

            // Verb
            if (target.saveMode == SaveMode.merge) { // merge
                sb.append("UNWIND $" + CONST_ROW_VARIABLE_NAME + " AS row ");
                // MERGE clause represents matching properties
                // MERGE (charlie {name: 'Charlie Sheen', age: 10})  A new node with the name 'Charlie Sheen' will be created since not all properties matched the existing 'Charlie Sheen' node.
                sb.append("MERGE (" + getLabelsPropertiesListCypherFragment("n", false, FragmentType.node, Arrays.asList(RoleType.key), target) + ")");
                String nodePropertyMapStr = getPropertiesListCypherFragment(FragmentType.node, false, Arrays.asList(RoleType.property), target);
                if (nodePropertyMapStr.length() > 0) {
                    sb.append(" SET n+=" + nodePropertyMapStr);
                }
            } else if (target.saveMode == SaveMode.append) { // fast create
                sb.append("UNWIND $" + CONST_ROW_VARIABLE_NAME + " AS row ");
                sb.append("CREATE (" + getLabelsPropertiesListCypherFragment("n", false, FragmentType.node, Arrays.asList(RoleType.key, RoleType.property), target) + ")");
            } else {
                LOG.error("Unhandled saveMode: " + target.saveMode);
            }
        } else {
            throw new RuntimeException("Unhandled target type: " + target.type);
        }
        return sb.toString();
    }

    public static String getLabelsPropertiesListCypherFragment(String alias, boolean onlyIndexedProperties, FragmentType entityType, List<RoleType> roleTypes, Target target) {
        StringBuffer sb = new StringBuffer();
        List<String> labels = ModelUtils.getStaticOrDynamicLabels(CONST_ROW_VARIABLE_NAME, entityType, target);
        String propertiesKeyListStr = getPropertiesListCypherFragment(entityType, onlyIndexedProperties, roleTypes, target);
        // Labels
        if (labels.size() > 0) {
            sb.append(alias);
            for (String label : labels) {
                sb.append(":" + ModelUtils.makeValidNeo4jIdentifier(label));
            }
        } else if (StringUtils.isNotEmpty(target.name)) {
            sb.append(alias);
            sb.append(":" + ModelUtils.makeValidNeo4jIdentifier(target.name));
        } else {
            sb.append(alias);
        }
        if (StringUtils.isNotEmpty(propertiesKeyListStr)) {
            sb.append(" " + propertiesKeyListStr);
        }
        return sb.toString();
    }

    public static String getPropertiesListCypherFragment(FragmentType entityType, boolean onlyIndexedProperties, List<RoleType> roleTypes, Target target) {
        StringBuffer sb = new StringBuffer();
        int targetColCount = 0;
        for (Mapping m : target.mappings) {
            if (m.fragmentType == entityType) {
                if (roleTypes.contains(m.role) && (!onlyIndexedProperties || m.indexed)) {
                    if (targetColCount > 0) {
                        sb.append(",");
                    }
                    if (StringUtils.isNotEmpty(m.constant)) {
                        sb.append(ModelUtils.makeValidNeo4jIdentifier(m.name) + ": \"" + m.constant + "\"");
                    } else {
                        sb.append(ModelUtils.makeValidNeo4jIdentifier(m.name) + ": row." + m.field);
                    }
                    targetColCount++;
                }
            }
        }
        if (sb.length() > 0) {
            return "{" + sb + "}";
        }
        return "";
    }


    public static List<String> getNodeIndexAndConstraintsCypherStatements(Config config, Target target) {

        List<String> cyphers = new ArrayList<>();
        // Model node creation statement
        //  "UNWIND $rows AS row CREATE(c:Customer { id : row.id, name: row.name, firstName: row.firstName })
        //derive labels
        List<String> labels = ModelUtils.getStaticLabels(FragmentType.node, target);
        List<String> indexedProperties = ModelUtils.getIndexedProperties(config, FragmentType.node, target);
        List<String> uniqueProperties = ModelUtils.getUniqueProperties(FragmentType.node, target);
        List<String> mandatoryProperties = ModelUtils.getRequiredProperties(FragmentType.node, target);
        List<String> nodeKeyProperties = ModelUtils.getNodeKeyProperties(FragmentType.node, target);


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

    public static String getRelationshipTypePropertiesListFragment(String prefix, boolean onlyIndexedProperties, Target target) {
        StringBuffer sb = new StringBuffer();
        List<String> relType = ModelUtils.getStaticOrDynamicRelationshipType(CONST_ROW_VARIABLE_NAME, target);
        sb.append(prefix + ":" + StringUtils.join(relType, ":"));
        sb.append(" " + getPropertiesListCypherFragment(FragmentType.rel, onlyIndexedProperties, Arrays.asList(RoleType.key, RoleType.property), target));
        return sb.toString();
    }

}
