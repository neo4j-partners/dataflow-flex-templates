package com.google.cloud.teleport.v2.neo4j.common.utils;

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.teleport.v2.neo4j.common.model.Mapping;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.PropertyType;
import com.google.cloud.teleport.v2.neo4j.common.transforms.DeleteEmptyRowsFn;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BeamUtils {
    private static final Logger LOG = LoggerFactory.getLogger(BeamUtils.class);

    public static String getSchemaFieldNameCsv(Schema schema) {
        return StringUtils.join(schema.getFields(), ",");
    }

    public static Schema toBeamSchema(com.google.cloud.bigquery.Schema bqSchema){
        List<Schema.Field> schemaFieldList = new ArrayList<>();
        for (int i = 0; i < bqSchema.getFields().size(); i++) {
            com.google.cloud.bigquery.Field field=bqSchema.getFields().get(i);
            Schema.Field schemaField= Schema.Field.nullable(field.getName(), bigQueryToBeamFieldType(field));
            schemaFieldList.add(schemaField);
        }
        return new Schema(schemaFieldList);
    }

    public static Schema.FieldType bigQueryToBeamFieldType(com.google.cloud.bigquery.Field field){

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
        LegacySQLTypeName legacySQLTypeName=field.getType();
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

    //This pattern serializes processing until collections in blockingList have completed
    public static PCollection<Row> blockCollection(List<PCollection<Row>> emptyBlockingList, Schema anySchema){
        return PCollectionList.of(emptyBlockingList).apply("Block", Flatten.pCollections()).apply("Remove nullRows",ParDo.of(new DeleteEmptyRowsFn())).setRowSchema(anySchema);
    }

    //This pattern accepts the emptyBlockingList and returns a copy of beamRows after the blocking list collections have processed
    public static PCollection<Row> unblockCollection(PCollection<Row> blockedCollection, PCollection<Row> beamRows, String description){
        List<PCollection<Row>> waitForUnblocked = new ArrayList<>();
        waitForUnblocked.add(beamRows);
        //null row must be added after data row.
        waitForUnblocked.add(blockedCollection);
        // also need to delete null rows...
        return PCollectionList.of(waitForUnblocked).apply(description, Flatten.pCollections());
    }

    public static Schema toBeamSchema(Target target) {

        // map source column names to order
        List<Schema.Field> fields = new ArrayList<>();
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
            if (mapping.type == PropertyType.Integer || mapping.type == PropertyType.Long) {
                //avoid truncation by making it long
                schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.INT64);
            } else if (mapping.type == PropertyType.Float) {
                schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.FLOAT);
            } else if (mapping.type == PropertyType.Boolean) {
                schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.BOOLEAN);
            } else if (mapping.type == PropertyType.ByteArray) {
                schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.BYTES);
            } else if (mapping.type == PropertyType.Point) {
                schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.STRING);
            } else if (mapping.type == PropertyType.Duration) {
                schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.DECIMAL);
            } else if (mapping.type == PropertyType.Date) {
                schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.DATETIME);
            } else if (mapping.type == PropertyType.LocalDateTime) {
                schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.DATETIME);
            } else if (mapping.type == PropertyType.DateTime) {
                schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.DATETIME);
            } else if (mapping.type == PropertyType.LocalTime) {
                schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.DATETIME);
            } else if (mapping.type == PropertyType.Time) {
                schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.STRING);
            } else {
                schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.STRING);
            }
            fields.add(schemaField);
        }
        return new Schema(fields);
    }

    public static Schema textToBeamSchema(String[] fieldNames){
            // map source column names to order
            List<Schema.Field> fields = new ArrayList<>();
            // Map these fields to a schema in a row
            for (int i = 0; i < fieldNames.length; i++) {
                String fieldName = fieldNames[i];
                Schema.Field schemaField= Schema.Field.of(fieldName, Schema.FieldType.STRING);
                fields.add(schemaField);
            }
            return new Schema(fields);
    }
}