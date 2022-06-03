package com.google.cloud.teleport.v2.neo4j.common.utils;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.teleport.v2.neo4j.common.model.Mapping;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.PropertyType;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataCastingUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DataCastingUtils.class);

    private static DateTimeFormatter jsDateTimeFormatter = DateTimeFormat.forPattern("YYYY-MM-DD HH:MM:SSZ");
    private static DateTimeFormatter jsDateFormatter = DateTimeFormat.forPattern("YYYY-MM-DD");
    private static SimpleDateFormat jsTimeFormatter = new SimpleDateFormat("HH:MM:SS");

    public static Row txtRowToTargetRow(final Row strRow, List<Mapping> targetMappings, Schema targetSchema) {
        Schema sourceSchema = strRow.getSchema();
        Object[] castVals = new Object[targetMappings.size()];

        for (int i = 0; i < targetMappings.size(); i++) {
            Mapping mapping = targetMappings.get(i);
            String strEl="";
            if (StringUtils.isNotEmpty(mapping.constant)) {
                strEl=mapping.constant;
                // LOG.info("Found constant, name: "+mapping.name+", value: "+strEl);
            } else {
                String fieldName = mapping.field;
                Schema.Field sourceField = sourceSchema.getField(fieldName);
                if (sourceField==null){
                    LOG.error("Could not map target field to source:"+fieldName);
                    strEl = null;
                } else {
                    strEl = StringUtils.trim(strRow.getValue(fieldName)+"");
                }
            }

            if (StringUtils.isEmpty(strEl) && StringUtils.isNotEmpty(mapping.defaultValue)){
                LOG.info("Setting default value for field: "+mapping.field+", name: "+mapping.name+", default: "+mapping.defaultValue);
                strEl = mapping.defaultValue;
            }
            if (mapping.type == PropertyType.Integer) {
                castVals[i] = Integer.parseInt(strEl);
            } else if (mapping.type == PropertyType.Float) {
                castVals[i] = Float.parseFloat(strEl);
            } else if (mapping.type == PropertyType.BigDecimal) {
                castVals[i] = new BigDecimal(strEl);
            } else if (mapping.type == PropertyType.Long) {
                castVals[i] = Long.parseLong(strEl);
            } else if (mapping.type == PropertyType.Boolean) {
                castVals[i] = Boolean.parseBoolean(strEl);
            } else if (mapping.type == PropertyType.ByteArray) {
                castVals[i]=strEl.getBytes(StandardCharsets.UTF_8);
            } else if (mapping.type == PropertyType.Point) {
                //point is string
                castVals[i]=strEl;
            } else if (mapping.type == PropertyType.Duration) {
                //TODO: how do we cast this?
                castVals[i]=strEl;
            } else if (mapping.type == PropertyType.Date) {
                castVals[i]= DateTime.parse(strEl,jsDateFormatter);
            } else if (mapping.type == PropertyType.LocalDateTime) {
                castVals[i] = LocalDateTime.parse(strEl);
            } else if (mapping.type == PropertyType.DateTime) {
                castVals[i] =  DateTime.parse(strEl,jsDateTimeFormatter);
            } else if (mapping.type == PropertyType.LocalTime) {
                castVals[i] = LocalDate.parse(strEl);
            } else if (mapping.type == PropertyType.Time) {
                try {
                    Date dt = jsTimeFormatter.parse(strEl);
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(dt);
                } catch (ParseException e){
                    castVals[i]=null;
                }
            } else {
                castVals[i]=strEl;
            }
        }

        Row targetRow = Row.withSchema(targetSchema).addValues(castVals).build();
        return targetRow;
    }

    public static List<Object> bigQueryToBeamValues(com.google.cloud.bigquery.Schema bqSchema,Collection<Object> bqValues){

        List<Object> beamValues=new ArrayList<>();
        Object[] objValues=bqValues.toArray();
        FieldList fieldList = bqSchema.getFields();
        for (int i=0;i<fieldList.size();i++){
            Field bqField=fieldList.get(i);
            LegacySQLTypeName legacySQLTypeName=bqField.getType();
            Object val=objValues[i];
            if (LegacySQLTypeName.STRING.equals(legacySQLTypeName)) {
                //return Schema.FieldType.STRING;
                beamValues.add(asString(val));
            } else if (LegacySQLTypeName.TIMESTAMP.equals(legacySQLTypeName)) {
                //return Schema.FieldType.DATETIME;
                beamValues.add(asDateTime(val));
            } else if (LegacySQLTypeName.DATE.equals(legacySQLTypeName)) {
                //return Schema.FieldType.DATETIME;
                beamValues.add(asDateTime(val));
            } else if (LegacySQLTypeName.BYTES.equals(legacySQLTypeName)) {
                //return Schema.FieldType.BYTES;
                beamValues.add(val);
            } else if (LegacySQLTypeName.BOOLEAN.equals(legacySQLTypeName)) {
                //return Schema.FieldType.BOOLEAN;
                beamValues.add(asBoolean(val));
            } else if (LegacySQLTypeName.NUMERIC.equals(legacySQLTypeName)) {
                //return Schema.FieldType.DOUBLE;
                beamValues.add(asDouble(val));
            } else if (LegacySQLTypeName.FLOAT.equals(legacySQLTypeName)) {
                //return Schema.FieldType.FLOAT;
                beamValues.add(asFloat(val));
            } else if (LegacySQLTypeName.INTEGER.equals(legacySQLTypeName)) {
                //return Schema.FieldType.INT64;
                beamValues.add(asInteger(val));
            }
        }
        return beamValues;
    }

    public static Map<String, Object> rowToNeo4jDataMap(Row row, Target target) {

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

    public static byte[] asBytes(Object obj) throws IOException
    {
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bytesOut);
        oos.writeObject(obj);
        oos.flush();
        byte[] bytes = bytesOut.toByteArray();
        bytesOut.close();
        oos.close();
        return bytes;
    }

    private static DateTime asDateTime(Object o) {
        if (o==null) return null;
        DateTime val = null;
        if (o instanceof DateTime) {
            val = ((DateTime) o).toDateTime();
        }
        return val;
    }

    private static Double asDouble(Object o) {
        if (o==null) return null;
        Double val = null;
        if (o instanceof Number) {
            val = ((Number) o).doubleValue();
        }
        return val;
    }

    private static Float asFloat(Object o) {
        if (o==null) return null;
        Float val = null;
        if (o instanceof Number) {
            val = ((Number) o).floatValue();
        }
        return val;
    }

    private static Integer asInteger(Object o) {
        if (o==null) return null;
        Integer val = null;
        if (o instanceof Number) {
            val = ((Number) o).intValue();
        }
        return val;
    }

    private static Boolean asBoolean(Object o) {
        if (o==null) return null;
        Boolean val = null;
        if (o instanceof Boolean) {
            val = ((Boolean) o).booleanValue();
        }
        return val;
    }

    private static String asString(Object o) {
        if (o==null) return null;
        String val = null;
        if (o instanceof String) {
            val = ((String) o).toString();
        }
        return val;
    }

}
