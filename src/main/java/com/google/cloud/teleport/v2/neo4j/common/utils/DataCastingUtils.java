package com.google.cloud.teleport.v2.neo4j.common.utils;

import com.google.cloud.bigquery.Field;
import com.google.cloud.teleport.v2.neo4j.common.model.Mapping;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.PropertyType;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.ReadableDateTime;
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
import java.time.*;
import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.stream.Collectors;

public class DataCastingUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DataCastingUtils.class);

    private static final DateTimeFormatter jsDateTimeFormatter = DateTimeFormat.forPattern("YYYY-MM-DD HH:MM:SSZ");
    private static final DateTimeFormatter jsDateFormatter = DateTimeFormat.forPattern("YYYY-MM-DD");
    private static final SimpleDateFormat jsTimeFormatter = new SimpleDateFormat("HH:MM:SS");

    public static Row txtRowToTargetRow(final Row strRow, List<Mapping> targetMappings, Schema targetSchema) {
        Schema sourceSchema = strRow.getSchema();
        Object[] castVals = new Object[targetMappings.size()];
        //LOG.info("FIELDNAMES: "+StringUtils.join(targetSchema.getFieldNames(),","));
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
            //LOG.info("DEBUG: "+mapping.constant+""+mapping.field+": "+strEl);
            if (StringUtils.isEmpty(strEl) && StringUtils.isNotEmpty(mapping.defaultValue)){
                LOG.info("Setting default value for field: "+mapping.field+", name: "+mapping.name+", default: "+mapping.defaultValue);
                strEl = mapping.defaultValue;
            }
            if (mapping.type == PropertyType.Integer) {
                castVals[i] = Long.parseLong(strEl);
            } else if (mapping.type == PropertyType.Float) {
                castVals[i] = Double.parseDouble(strEl);
            } else if (mapping.type == PropertyType.BigDecimal) {
                castVals[i] = Double.parseDouble(strEl);
            } else if (mapping.type == PropertyType.Long) {
                castVals[i] = Long.parseLong(strEl);
            } else if (mapping.type == PropertyType.Boolean) {
                castVals[i] = Boolean.parseBoolean(strEl);
            } else if (mapping.type == PropertyType.ByteArray) {
                castVals[i]=strEl.getBytes(StandardCharsets.UTF_8);
            } else if (mapping.type == PropertyType.Point) {
                //TODO: cast to point
                //org.neo4j.graphdb.spatial.Point
                castVals[i]=strEl;
            } else if (mapping.type == PropertyType.Duration) {
                java.time.temporal.TemporalAmount duration=TimeParser.parseTemporalAmount(strEl);
                castVals[i]= duration;
                //TODO: check on time parsing
            } else if (mapping.type == PropertyType.Date || mapping.type == PropertyType.DateTime) {
                DateTime dt = DateTime.parse(strEl,jsDateFormatter);
                LocalDate ldt=LocalDate.of(dt.getYear(),dt.getMonthOfYear(),dt.getDayOfMonth());
                ldt.atTime(dt.getHourOfDay(),dt.getMinuteOfHour(),dt.getSecondOfMinute());
                castVals[i]= ldt;
            } else if (mapping.type == PropertyType.LocalDateTime) {
                castVals[i] = LocalDateTime.parse(strEl);
            } else if (mapping.type == PropertyType.LocalTime) {
                castVals[i] = LocalTime.parse(strEl);
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

    public static Map<String, Object> rowToNeo4jDataMap(Row row, Target target) {

        Map<String, Object> map = new HashMap();

        Schema dataSchema=row.getSchema();
        for (Schema.Field field:dataSchema.getFields()){
            String fieldName=field.getName();
            Schema.FieldType type=field.getType();
            // BYTE, INT16, INT32, INT64, DECIMAL, FLOAT, DOUBLE, STRING, DATETIME, BOOLEAN, BYTES, ARRAY, ITERABLE, MAP, ROW, LOGICAL_TYPE;
            // NUMERIC_TYPES; STRING_TYPES; DATE_TYPES; COLLECTION_TYPES; MAP_TYPES; COMPOSITE_TYPES;
            if (row.getValue(fieldName)==null){
                map.put(fieldName,null);
                continue;
            }
            if (type.getTypeName().isNumericType()){
                if (type.getTypeName()== Schema.TypeName.DECIMAL){
                    map.put(fieldName, row.getDecimal(fieldName).doubleValue());
                } else if (type.getTypeName()== Schema.TypeName.FLOAT){
                    map.put(fieldName, row.getFloat(fieldName).doubleValue());
                } else if (type.getTypeName()== Schema.TypeName.DOUBLE){
                    map.put(fieldName, row.getDouble(fieldName));
                } else {
                    map.put(fieldName, Long.parseLong(row.getValue(fieldName)+""));
                }
            } else if (type.getTypeName().isLogicalType()){
                map.put(fieldName, Boolean.parseBoolean(row.getBoolean(fieldName)+""));
            } else if (type.getTypeName().isDateType()){
                    ReadableDateTime dt=row.getDateTime(fieldName);
                    ZonedDateTime zdt = ZonedDateTime.ofLocal(
                            LocalDateTime.of(
                                    dt.getYear(),
                                    dt.getMonthOfYear(),
                                    dt.getDayOfMonth(),
                                    dt.getHourOfDay(),
                                    dt.getMinuteOfHour(),
                                    dt.getSecondOfMinute(),
                                    dt.getMillisOfSecond() * 1_000_000),
                            ZoneId.of(dt.getZone().getID(), ZoneId.SHORT_IDS),
                            ZoneOffset.ofTotalSeconds(dt.getZone().getOffset(dt) / 1000));
                    map.put(fieldName,zdt);
            } else {
                map.put(fieldName, row.getValue(fieldName)+"");
            }
        }
        for (Mapping m : target.mappings) {
            //if row is empty continue
            if (listFullOfNulls(row.getValues())) {
                continue;
            }
            String fieldName = m.field;
            PropertyType targetMappingType = m.type;
            //lookup data type
            if (StringUtils.isNotEmpty(m.constant)) {
                if (StringUtils.isNotEmpty(m.name)) {
                    map.put(m.name, m.constant);
                } else {
                    map.put(m.constant, m.constant);
                }
            }
        }

        //LOG.info("Casted map: "+mapToString(map));
        return map;
    }

    private static boolean listFullOfNulls(List<Object> entries){
            for (Object key: entries){
                if (key!=null) return false;
        }
        return true;
    }
    public static String mapToString(Map<String, ?> map) {
        String mapAsString = map.keySet().stream()
                .map(key -> key + "=" + map.get(key))
                .collect(Collectors.joining(", ", "{", "}"));
        return mapAsString;
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
            val = ((String) o);
        }
        return val;
    }

}
