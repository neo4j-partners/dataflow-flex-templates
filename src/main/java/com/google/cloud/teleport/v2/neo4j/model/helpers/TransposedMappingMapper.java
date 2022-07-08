package com.google.cloud.teleport.v2.neo4j.model.helpers;

import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.PropertyType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.FieldNameTuple;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper object for parsing transposed field mappings (ie. strings, indexed, longs, etc.).
 */
public class TransposedMappingMapper {

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static final Logger LOG = LoggerFactory.getLogger(TransposedMappingMapper.class);

    public static List<Mapping> parseMappings(Target target, final JSONObject mappingsObject) {
        if (target.type == TargetType.node.node) {
            return parseNode(mappingsObject);
        } else if (target.type == TargetType.edge.edge) {
            return parseEdge(mappingsObject);
        } else {
            return new ArrayList();
        }
    }

    public static List<Mapping> parseNode(final JSONObject nodeMappingsObject) {
        List<Mapping> mappings = new ArrayList<>();
        // label nodes
        // "constant": "Customer",
        // "name": "Customer",
        // "role": "label"
        if (nodeMappingsObject.has("label")) {
            FieldNameTuple labelTuple = createFieldNameTuple(nodeMappingsObject.getString("label"));
            Mapping mapping = new Mapping(FragmentType.node, RoleType.label, labelTuple);
            addMapping(mappings, mapping);
        }
        if (nodeMappingsObject.has("labels")) {
            List<FieldNameTuple> labels = getFieldAndNameTuples(nodeMappingsObject.get("labels"));
            for (FieldNameTuple f : labels) {
                Mapping mapping = new Mapping(FragmentType.node, RoleType.label, f);
                mapping.indexed = true;
                addMapping(mappings, mapping);
            }
        }
        if (nodeMappingsObject.has("key")) {
            FieldNameTuple labelTuple = createFieldNameTuple(nodeMappingsObject.getString("key"));
            Mapping mapping = new Mapping(FragmentType.node, RoleType.key, labelTuple);
            addMapping(mappings, mapping);
        }
        // keys
        // "field": "customer_id",
        // "name": "CustomerId",
        // "role": "key",
        // "description": "Customer identifier",
        // "type": "String",
        // "unique": true,
        // "indexed": true
        if (nodeMappingsObject.has("keys")) {
            List<FieldNameTuple> keys = getFieldAndNameTuples(nodeMappingsObject.get("keys"));
            for (FieldNameTuple f : keys) {
                Mapping mapping = new Mapping(FragmentType.node, RoleType.key, f);
                mapping.indexed = true;
                addMapping(mappings, mapping);
            }
        }

        parseProperties(nodeMappingsObject.getJSONObject("properties"), mappings, FragmentType.node);
        return mappings;
    }

    public static List<Mapping> parseEdge(final JSONObject edgeMappingsObject) {
        List<Mapping> mappings = new ArrayList<>();
        // type
        if (edgeMappingsObject.has("type")) {
            FieldNameTuple typeTuple = createFieldNameTuple(edgeMappingsObject.getString("type"), edgeMappingsObject.getString("type"));
            Mapping mapping = new Mapping(FragmentType.rel, RoleType.type, typeTuple);
            addMapping(mappings, mapping);
        }
        // source
        // "label": "\"Customer\"",
        // "key": "customer_id"
        if (edgeMappingsObject.has("source")) {
            JSONObject sourceObj = edgeMappingsObject.getJSONObject("source");
            List<String> labels = getLabels(sourceObj.getString("label"));
            FieldNameTuple keyTuple = createFieldNameTuple(sourceObj.getString("key"));
            Mapping keyMapping = new Mapping(FragmentType.source, RoleType.key, keyTuple);
            keyMapping.labels = labels;
            mappings.add(keyMapping);
        }
        // target
        // "label": "\"Product\"",
        // "key": "product_id"

        if (edgeMappingsObject.has("target")) {
            JSONObject sourceObj = edgeMappingsObject.getJSONObject("target");
            List<String> labels = getLabels(sourceObj.getString("label"));
            FieldNameTuple keyTuple = createFieldNameTuple(sourceObj.getString("key"));
            Mapping keyMapping = new Mapping(FragmentType.target, RoleType.key, keyTuple);
            keyMapping.labels = labels;
            mappings.add(keyMapping);
        }
        // properties
        parseProperties(edgeMappingsObject.getJSONObject("properties"), mappings, FragmentType.rel);
        return mappings;
    }

    private static void parseProperties(JSONObject propertyMappingsObject, List<Mapping> mappings, FragmentType fragmentType) {
        if (propertyMappingsObject == null) {
            return;
        }
        // properties
        //  "field": "contact_name",
        // "name": "ContactName",
        // "role": "property",
        // "description": "Customer contact",
        // "type": "String",
        // "unique": false,
        // "indexed": true
        List<FieldNameTuple> uniques = new ArrayList();
        List<FieldNameTuple> indexed = new ArrayList<>();

        //LOG.info("Parsing mapping: "+gson.toJson(propertyMappingsObject));

        if (propertyMappingsObject.has("unique")) {
            uniques = getFieldAndNameTuples(propertyMappingsObject.get("unique"));
        }
        if (propertyMappingsObject.has("indexed")) {
            indexed = getFieldAndNameTuples(propertyMappingsObject.get("indexed"));
        }

        for (FieldNameTuple f : uniques) {
            Mapping mapping = new Mapping(fragmentType, RoleType.property, f);
            addMapping(mappings, mapping);
            mapping.indexed = indexed.contains(f);
        }
        for (FieldNameTuple f : indexed) {
            Mapping mapping = new Mapping(fragmentType, RoleType.property, f);
            addMapping(mappings, mapping);
            mapping.unique = uniques.contains(f);
        }
        if (propertyMappingsObject.has("dates")) {
            List<FieldNameTuple> dates = getFieldAndNameTuples(propertyMappingsObject.get("dates"));
            for (FieldNameTuple f : dates) {
                Mapping mapping = new Mapping(fragmentType, RoleType.property, f);
                mapping.type = PropertyType.Date;
                mapping.indexed = indexed.contains(f);
                mapping.unique = uniques.contains(f);
                addMapping(mappings, mapping);

            }
        }

        if (propertyMappingsObject.has("doubles")) {
            //LOG.info("Parsing doubles...");
            List<FieldNameTuple> numbers = getFieldAndNameTuples(propertyMappingsObject.get("doubles"));
            for (FieldNameTuple f : numbers) {
                Mapping mapping = new Mapping(fragmentType, RoleType.property, f);
                mapping.type = PropertyType.BigDecimal;
                mapping.indexed = indexed.contains(f);
                mapping.unique = uniques.contains(f);
                //LOG.info("double mappings: "+gson.toJson(mapping));
                addMapping(mappings, mapping);
            }
        }
        if (propertyMappingsObject.has("longs")) {
            //LOG.info("Parsing longs...");
            List<FieldNameTuple> longs = getFieldAndNameTuples(propertyMappingsObject.get("longs"));
            for (FieldNameTuple f : longs) {
                Mapping mapping = new Mapping(fragmentType, RoleType.property, f);
                mapping.type = PropertyType.Long;
                mapping.indexed = indexed.contains(f);
                mapping.unique = uniques.contains(f);
                //LOG.info("longs mappings: "+gson.toJson(mapping));
                addMapping(mappings, mapping);
            }
        }
        if (propertyMappingsObject.has("strings")) {
            List<FieldNameTuple> strings = getFieldAndNameTuples(propertyMappingsObject.get("strings"));
            for (FieldNameTuple f : strings) {
                Mapping mapping = new Mapping(fragmentType, RoleType.property, f);
                mapping.type = PropertyType.String;
                mapping.indexed = indexed.contains(f);
                mapping.unique = uniques.contains(f);
                addMapping(mappings, mapping);
            }
        }

    }

    private static List<String> getLabels(Object tuplesObj) {
        List<String> labels = new ArrayList<>();
        if (tuplesObj instanceof JSONArray) {
            JSONArray tuplesArray = (JSONArray) tuplesObj;
            for (int i = 0; i < tuplesArray.length(); i++) {
                if (tuplesArray.get(i) instanceof JSONObject) {
                    //{field:name} or {field1:name,field2:name} tuples
                    Iterator<String> it = tuplesArray.getJSONObject(i).keys();
                    while (it.hasNext()) {
                        String key = it.next();
                        labels.add(key);
                    }
                } else {
                    labels.add(tuplesArray.getString(i));
                }
            }
        } else if (tuplesObj instanceof JSONObject) {
            JSONObject jsonObject = (JSONObject) tuplesObj;
            //{field:name} or {field1:name,field2:name} tuples
            Iterator<String> it = jsonObject.keys();
            while (it.hasNext()) {
                labels.add(it.next());
            }
        } else {
            labels.add(tuplesObj + "");
        }
        return labels;
    }

    private static List<FieldNameTuple> getFieldAndNameTuples(Object tuplesObj) {
        List<FieldNameTuple> tuples = new ArrayList<>();
        if (tuplesObj instanceof JSONArray) {
            JSONArray tuplesArray = (JSONArray) tuplesObj;
            for (int i = 0; i < tuplesArray.length(); i++) {
                if (tuplesArray.get(i) instanceof JSONObject) {
                    //{field:name} or {field1:name,field2:name} tuples
                    Iterator<String> it = tuplesArray.getJSONObject(i).keys();
                    while (it.hasNext()) {
                        String key = it.next();
                        tuples.add(createFieldNameTuple(key, tuplesArray.getJSONObject(i).getString(key)));
                    }
                } else {
                    tuples.add(createFieldNameTuple(tuplesArray.getString(i), tuplesArray.getString(i)));
                }
            }
        } else if (tuplesObj instanceof JSONObject) {
            JSONObject jsonObject = (JSONObject) tuplesObj;
            //{field:name} or {field1:name,field2:name} tuples
            Iterator<String> it = jsonObject.keys();
            while (it.hasNext()) {
                String key = it.next();
                tuples.add(createFieldNameTuple(key, jsonObject.getString(key)));
            }
        } else {
            tuples.add(createFieldNameTuple(tuplesObj + "", tuplesObj + ""));
        }
        return tuples;
    }

    private static FieldNameTuple createFieldNameTuple(String field) {
        return createFieldNameTuple(field, null);
    }

    private static FieldNameTuple createFieldNameTuple(String field, String name) {
        FieldNameTuple fieldSet = new FieldNameTuple();
        fieldSet.name = name;
        field = field.trim();
        //handle double quoted constants
        if (field.charAt(0) == '\"' && field.charAt(field.length() - 1) == '\"') {
            fieldSet.constant = StringUtils.replace(field, "\"", "");
            if (StringUtils.isEmpty(name)) {
                fieldSet.name = fieldSet.constant;
            } else {
                fieldSet.name = StringUtils.replace(name, "\"", "");
            }
            //field is ""
        } else {
            if (StringUtils.isEmpty(name)) {
                fieldSet.name = ModelUtils.makeValidNeo4jIdentifier(field);
            } else {
                fieldSet.name = ModelUtils.makeValidNeo4jIdentifier(name);
            }
            fieldSet.field = field;
        }
        return fieldSet;
    }

    private static void addMapping(List<Mapping> mappings, Mapping mapping) {
        if (!StringUtils.isEmpty(mapping.field)) {
            for (Mapping existingMapping : mappings) {
                if (existingMapping.field != null && existingMapping.field.equals(mapping.field)) {
                    throw new RuntimeException("Duplicate mapping: " + gson.toJson(mapping));
                }
            }
        }
        mappings.add(mapping);
    }


}
