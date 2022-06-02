package com.google.cloud.teleport.v2.neo4j.common;

import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.teleport.v2.neo4j.common.model.Mapping;
import com.google.cloud.teleport.v2.neo4j.common.model.Targets;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.PropertyType;
import org.apache.avro.LogicalTypes;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;

public class BeamSchemaUtils {
    private static final Logger LOG = LoggerFactory.getLogger(BeamSchemaUtils.class);

    public static String getSchemaFieldNameCsv(Schema schema) {
        return StringUtils.join(schema.getFields(), ",");
    }

    public static Schema fromBqFieldList(FieldList fieldList){
        List<Schema.Field> schemaFieldList = new ArrayList<>();
        for (int i = 0; i < fieldList.size(); i++) {
            com.google.cloud.bigquery.Field field=fieldList.get(i);
            Schema.Field schemaField= Schema.Field.of(field.getName(), getFieldType(field.getType()));
            schemaFieldList.add(schemaField);
        }
        return new Schema(schemaFieldList);
    }
    private static Schema.FieldType getFieldType(LegacySQLTypeName legacySQLTypeName){

        /*
        public static final LegacySQLTypeName BYTES;
        public static final LegacySQLTypeName STRING;
        public static final LegacySQLTypeName INTEGER;
        public static final LegacySQLTypeName FLOAT;
        public static final LegacySQLTypeName NUMERIC;
        public static final LegacySQLTypeName BOOLEAN;
        public static final LegacySQLTypeName TIMESTAMP;
        public static final LegacySQLTypeName DATE;
        public static final LegacySQLTypeName GEOGRAPHY;
        public static final LegacySQLTypeName TIME;
        public static final LegacySQLTypeName DATETIME;
        public static final LegacySQLTypeName RECORD;
        */
        if (LegacySQLTypeName.STRING.equals(legacySQLTypeName)) {
            return Schema.FieldType.STRING;
        } else if (LegacySQLTypeName.TIMESTAMP.equals(legacySQLTypeName)) {
            return Schema.FieldType.DATETIME;
        } else if (LegacySQLTypeName.DATE.equals(legacySQLTypeName)) {
            return Schema.FieldType.DATETIME;
        } else if (LegacySQLTypeName.BYTES.equals(legacySQLTypeName)) {
            return Schema.FieldType.BYTES;
        } else if (LegacySQLTypeName.BOOLEAN.equals(legacySQLTypeName)) {
            return Schema.FieldType.BOOLEAN;
        } else if (LegacySQLTypeName.NUMERIC.equals(legacySQLTypeName)) {
            return Schema.FieldType.DOUBLE;
        } else if (LegacySQLTypeName.FLOAT.equals(legacySQLTypeName)) {
            return Schema.FieldType.FLOAT;
        } else if (LegacySQLTypeName.INTEGER.equals(legacySQLTypeName)) {
            return Schema.FieldType.INT64;
        }
        throw new UnsupportedOperationException("LegacySQL type " + legacySQLTypeName.getStandardType() + " not supported.");
    }
    public static Schema toNeo4jTargetSchema
            (Targets target) {

        // map source column names to order
        List<Schema.Field> fields = new ArrayList<>();
        LOG.info("Fields explicitly defined");
        // Map these fields to a schema in a row
        for (int i = 0; i < target.mappings.size(); i++) {
            Mapping mapping = target.mappings.get(i);
            String fieldName = "";
            if (StringUtils.isNotBlank(mapping.field)) {
                fieldName = mapping.field;
            } else if (StringUtils.isNotBlank(mapping.name)) {
                fieldName = mapping.name;
            } else if (StringUtils.isNotBlank(mapping.constant)) {
                fieldName = mapping.constant;
            }

            if (StringUtils.isEmpty(fieldName)) {
                throw new RuntimeException("Could not find field name or constant in target: "+target.name);
            }
            Schema.Field schemaField;
            if (mapping.type == PropertyType.Integer) {
                schemaField = Schema.Field.of(fieldName, Schema.FieldType.INT32);
            } else if (mapping.type == PropertyType.Float) {
                schemaField = Schema.Field.of(fieldName, Schema.FieldType.FLOAT);
            } else if (mapping.type == PropertyType.Long) {
                schemaField = Schema.Field.of(fieldName, Schema.FieldType.INT64);
            } else if (mapping.type == PropertyType.Boolean) {
                schemaField = Schema.Field.of(fieldName, Schema.FieldType.BOOLEAN);
            } else if (mapping.type == PropertyType.ByteArray) {
                schemaField = Schema.Field.of(fieldName, Schema.FieldType.BYTES);
            } else if (mapping.type == PropertyType.Point) {
                schemaField = Schema.Field.of(fieldName, Schema.FieldType.STRING);
            } else if (mapping.type == PropertyType.Duration) {
                schemaField = Schema.Field.of(fieldName, Schema.FieldType.DECIMAL);
            } else if (mapping.type == PropertyType.Date) {
                schemaField = Schema.Field.of(fieldName, Schema.FieldType.DATETIME);
            } else if (mapping.type == PropertyType.LocalDateTime) {
                schemaField = Schema.Field.of(fieldName, Schema.FieldType.DATETIME);
            } else if (mapping.type == PropertyType.DateTime) {
                schemaField = Schema.Field.of(fieldName, Schema.FieldType.DATETIME);
                //TODO: how to model time?
            } else if (mapping.type == PropertyType.LocalTime) {
                schemaField = Schema.Field.of(fieldName, Schema.FieldType.FLOAT);
            } else if (mapping.type == PropertyType.Time) {
                schemaField = Schema.Field.of(fieldName, Schema.FieldType.FLOAT);
            } else {
                schemaField = Schema.Field.of(fieldName, Schema.FieldType.STRING);
            }
            fields.add(schemaField);
        }
        return new Schema(fields);
    }
}
