package com.google.cloud.teleport.v2.neo4j.common.model;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.commons.csv.CSVFormat;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TextFile implements Serializable {


    public String uri = "";
    public String delimiter = "";
    public CSVFormat csvFormat = CSVFormat.DEFAULT;
    public String[] fieldNames = new String[0];
    public Map<String, Integer> fieldPosByName = new HashMap();

    public TextFile() {
    }

    public TextFile(final JSONObject jsonObject) {
        String csvFormatStr = jsonObject.has("csv_format") ? jsonObject.getString("csv_format").toUpperCase() : "DEFAULT";
        if (csvFormatStr.equals("EXCEL")) {
            csvFormat = CSVFormat.EXCEL;
        } else if (csvFormatStr.equals("MONGO")) {
            csvFormat = CSVFormat.MONGODB_CSV;
        } else if (csvFormatStr.equals("INFORMIX")) {
            csvFormat = CSVFormat.INFORMIX_UNLOAD_CSV;
        } else if (csvFormatStr.equals("POSTGRES")) {
            csvFormat = CSVFormat.POSTGRESQL_CSV;
        } else if (csvFormatStr.equals("MYSQL")) {
            csvFormat = CSVFormat.MYSQL;
        } else if (csvFormatStr.equals("ORACLE")) {
            csvFormat = CSVFormat.ORACLE;
        } else if (csvFormatStr.equals("MONGO_TSV")) {
            csvFormat = CSVFormat.MONGODB_TSV;
        } else if (csvFormatStr.equals("RFC4180")) {
            csvFormat = CSVFormat.RFC4180;
        } else if (csvFormatStr.equals("POSTGRESQL_CSV")) {
            csvFormat = CSVFormat.POSTGRESQL_CSV;
        }
        uri = jsonObject.getString("uri");
        delimiter = jsonObject.has("delimiter") ? jsonObject.getString("delimiter") : "";
        final String colNamesStr = jsonObject.has("ordered_field_names") ? jsonObject.getString("ordered_field_names") : "";
        if (StringUtils.isNotEmpty(colNamesStr)) {
            fieldNames = StringUtils.split(colNamesStr, ",");
            for (int i = 0; i < fieldNames.length; i++) {
                fieldPosByName.put(fieldNames[i], (i + 1));
            }
        } else {
            throw new RuntimeException("Unable to determine column names from input file.  Please populate 'ordered_field_names'");
        }
        if (StringUtils.isNotEmpty(delimiter)) {
            csvFormat.withDelimiter(delimiter.charAt(0));
        }
    }

    public Schema getTextFileSchemaData(){
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