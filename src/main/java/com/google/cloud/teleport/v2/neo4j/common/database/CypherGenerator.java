package com.google.cloud.teleport.v2.neo4j.common.database;

import com.google.cloud.teleport.v2.neo4j.common.model.Mapping;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.SaveMode;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils.getRelationshipTypeFields;

public class CypherGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(CypherGenerator.class);
    final static String CONST_ROW_VARIABLE_NAME = "rows";

    public static String getUnwindCreateCypher(Target target) {
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
            if (target.saveMode == SaveMode.append && (ModelUtils.getUniqueProperties(FragmentType.source, target).size() == 0 || ModelUtils.getUniqueProperties(FragmentType.target, target).size() == 0)) {
                effectiveSaveMode = SaveMode.error_if_exists;
                //For SaveMode.Overwrite mode, you need to have unique constraints on the keys.
            } else if (target.saveMode == SaveMode.overwrite && ModelUtils.getUniqueProperties(FragmentType.source, target).size() == 0 || ModelUtils.getUniqueProperties(FragmentType.target, target).size() == 0) {
                effectiveSaveMode = SaveMode.error_if_exists;
            }

            // Verb
            if (effectiveSaveMode == SaveMode.append || effectiveSaveMode == SaveMode.overwrite) { // merge
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
            } else if (effectiveSaveMode == SaveMode.error_if_exists) { // Fast, blind create
                sb.append("UNWIND $" + CONST_ROW_VARIABLE_NAME + " AS row CREATE ");
                sb.append("(" + getLabelsPropertiesListCypherFragment("source", false, FragmentType.source, Arrays.asList(RoleType.key,RoleType.property), target) + ")");
                sb.append(" -[" + getRelationshipTypePropertiesListFragment("rel", false, target) + "]-> ");
                sb.append("(" + getLabelsPropertiesListCypherFragment("target", false, FragmentType.target, Arrays.asList(RoleType.key,RoleType.property), target) + ")");
                // SET properties
            } else  if (effectiveSaveMode == SaveMode.match) { // Update
                sb.append("UNWIND $" + CONST_ROW_VARIABLE_NAME + " AS row CREATE ");
                sb.append(" MATCH (" + getLabelsPropertiesListCypherFragment("source", true, FragmentType.source, Arrays.asList(RoleType.key,RoleType.property), target) + ")");
                sb.append(" , (" + getLabelsPropertiesListCypherFragment("target", true, FragmentType.target, Arrays.asList(RoleType.key,RoleType.property), target) + ")");
                sb.append(" MERGE (source)");
                sb.append(" -[" + getRelationshipTypePropertiesListFragment("rel", false, target) + "]-> ");
                sb.append(" (target)");
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
            if (target.saveMode == SaveMode.append && ModelUtils.getUniqueProperties(FragmentType.node, target).size() == 0) {
                effectiveSaveMode = SaveMode.error_if_exists;
                //For SaveMode.Overwrite mode, you need to have unique constraints on the keys.
            } else if (target.saveMode == SaveMode.overwrite && ModelUtils.getUniqueProperties(FragmentType.node, target).size() == 0) {
                effectiveSaveMode = SaveMode.error_if_exists;
            }

            // Verb
            if (effectiveSaveMode == SaveMode.append || effectiveSaveMode == SaveMode.overwrite || effectiveSaveMode == SaveMode.match) { // merge
                sb.append("UNWIND $" + CONST_ROW_VARIABLE_NAME + " AS row ");
                // MERGE clause represents matching properties
                // MERGE (charlie {name: 'Charlie Sheen', age: 10})  A new node with the name 'Charlie Sheen' will be created since not all properties matched the existing 'Charlie Sheen' node.
                sb.append("MERGE (" + getLabelsPropertiesListCypherFragment("n", false, FragmentType.node, Arrays.asList(RoleType.key), target) + ")");
                String nodePropertyMapStr = getPropertiesListCypherFragment(FragmentType.node, false, Arrays.asList(RoleType.property), target);
                if (nodePropertyMapStr.length() > 0) {
                    sb.append(" SET n+=" + nodePropertyMapStr);
                }
            } else if (effectiveSaveMode == SaveMode.error_if_exists) { // fast create
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
        List<String> labels = ModelUtils.getStaticOrDynamicLabels("row",entityType, target);
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
                    if (targetColCount > 0) sb.append(",");
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


    public static List<String> getNodeIndexAndConstraintsCypherStatements(Target target) {

        List<String> cyphers = new ArrayList<>();
        // Model node creation statement
        //  "UNWIND $rows AS row CREATE(c:Customer { id : row.id, name: row.name, firstName: row.firstName })
        //derive labels
        List<String> labels = ModelUtils.getStaticLabels(FragmentType.node, target);
        List<String> indexedProperties = ModelUtils.getIndexedProperties(FragmentType.node, target);
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
        List<String> relationships = getRelationshipTypeFields(target);
        sb.append(prefix + ":" + StringUtils.join(relationships, ":"));
        sb.append(" " + getPropertiesListCypherFragment(FragmentType.rel, onlyIndexedProperties, Arrays.asList(RoleType.key, RoleType.property), target));
        return sb.toString();
    }

}
