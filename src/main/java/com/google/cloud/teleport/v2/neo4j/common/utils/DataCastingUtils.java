package com.google.cloud.teleport.v2.neo4j.common.utils;

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
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

public class DataCastingUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DataCastingUtils.class);

    private static final DateTimeFormatter jsDateTimeFormatter = DateTimeFormat.forPattern("YYYY-MM-DD HH:MM:SSZ");
    private static final DateTimeFormatter jsDateFormatter = DateTimeFormat.forPattern("YYYY-MM-DD");
    private static final SimpleDateFormat jsTimeFormatter = new SimpleDateFormat("HH:MM:SS");

    public static List<Object> sourceTextToTargetObjects(final Row rowOfStrings, Target target) {
        Schema targetSchema= BeamUtils.toBeamSchema(target);
        List<Mapping> targetMappings=target.mappings;
        List<Object> castVals = new ArrayList<>();
        Iterator<Schema.Field> it = targetSchema.getFields().iterator();
        int indx = -1;
        while (it.hasNext()) {
            Schema.Field field = it.next();
            String fieldName = field.getName();
            Schema.FieldType type = field.getType();
            Object objVal=null;

            try {
                objVal=rowOfStrings.getValue(fieldName);
            } catch (Exception e){
                //LOG.warn("Error getting value: "+fieldName);
            }
            if (objVal == null) {
                String constant= findConstantValue(targetMappings,  fieldName);
                if (constant!=null) {
                    castVals.add(constant);
                    continue;
                } else {
                    LOG.error("Value for "+fieldName+" not found.");
                    castVals.add(null);
                    continue;
                }
            }

            try {
                String strEl = objVal+"";
                //LOG.info(fieldName+":"+objVal);
                if (type.getTypeName().isNumericType()) {
                    if (type.getTypeName() == Schema.TypeName.DECIMAL) {
                        castVals.add(Double.parseDouble(strEl));
                    } else if (type.getTypeName() == Schema.TypeName.FLOAT) {
                        castVals.add(Float.parseFloat(strEl));
                    } else if (type.getTypeName() == Schema.TypeName.DOUBLE) {
                        castVals.add(Double.parseDouble(strEl));
                    } else {
                        castVals.add(Long.parseLong(strEl));
                    }
                } else if (type.getTypeName().isLogicalType()) {
                    castVals.add(Boolean.parseBoolean(strEl));
                } else if (type.getTypeName().isDateType()) {
                    if (strEl.indexOf(":") > 0) {
                        DateTime dt = DateTime.parse(strEl, jsDateTimeFormatter);
                        LocalDate ldt = LocalDate.of(dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth());
                        ldt.atTime(dt.getHourOfDay(), dt.getMinuteOfHour(), dt.getSecondOfMinute());
                        castVals.add(ldt);
                    } else {
                        DateTime dt = DateTime.parse(strEl, jsDateFormatter);
                        LocalDate ldt = LocalDate.of(dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth());
                        ldt.atTime(dt.getHourOfDay(), dt.getMinuteOfHour(), dt.getSecondOfMinute());
                        castVals.add(ldt);
                    }
                } else {
                    castVals.add(objVal);
                }
            }catch (Exception e){
                castVals.add(null);
                LOG.warn("Exception casting "+fieldName+": "+objVal);
            }
        }
        //LOG.info("Constructing target row: {}, values: {}",targetSchema,StringUtils.join(castVals,","));
        return castVals;
    }

    private static String findConstantValue(List<Mapping> targetMappings, String fieldName){
        for (Mapping m : targetMappings) {
            //lookup data type
            if (StringUtils.isNotEmpty(m.constant)) {
                if (m.name.equals(fieldName) || m.constant.equals(fieldName)) {
                    return m.constant;
                }
            }
        }
        return null;
    }

    public static Map<String, Object> rowToNeo4jDataMap(Row row, Target target) {

        Map<String, Object> map = new HashMap();

        Schema dataSchema = row.getSchema();
        for (Schema.Field field : dataSchema.getFields()) {
            String fieldName = field.getName();
            Schema.FieldType type = field.getType();

            try {
                // BYTE, INT16, INT32, INT64, DECIMAL, FLOAT, DOUBLE, STRING, DATETIME, BOOLEAN, BYTES, ARRAY, ITERABLE, MAP, ROW, LOGICAL_TYPE;
                // NUMERIC_TYPES; STRING_TYPES; DATE_TYPES; COLLECTION_TYPES; MAP_TYPES; COMPOSITE_TYPES;
                if (row.getValue(fieldName) == null) {
                    map.put(fieldName, null);
                    continue;
                }
                if (type.getTypeName().isNumericType()) {
                    if (type.getTypeName() == Schema.TypeName.DECIMAL) {
                        map.put(fieldName, row.getDecimal(fieldName).doubleValue());
                    } else if (type.getTypeName() == Schema.TypeName.FLOAT) {
                        map.put(fieldName, row.getFloat(fieldName).doubleValue());
                    } else if (type.getTypeName() == Schema.TypeName.DOUBLE) {
                        map.put(fieldName, row.getDouble(fieldName));
                    } else {
                        map.put(fieldName, Long.parseLong(row.getValue(fieldName) + ""));
                    }
                // TODO: this is an upstream error.  Dates are coming across as LOGICAL_TYPE.  Logical type identifier does include ":date:"
                } else if ((type.getLogicalType()==null?"":type.getLogicalType().getIdentifier()).contains(":date:")){
                    //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                    java.util.Date parsedDate= sdf.parse(row.getValue(fieldName));
                    ZonedDateTime zdt=ZonedDateTime.from(parsedDate.toInstant());
                    map.put(fieldName, zdt);
                } else if (type.getTypeName().isDateType()) {
                    ReadableDateTime dt = row.getDateTime(fieldName);
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
                    map.put(fieldName, zdt);
                } else if (type.typesEqual(Schema.FieldType.BOOLEAN)) {
                    map.put(fieldName, Boolean.parseBoolean(row.getBoolean(fieldName) + ""));
                } else {
                    map.put(fieldName, row.getValue(fieldName) + "");
                }
            } catch (Exception e){
                LOG.error("Error casting "+type.getTypeName().name()+", "+ type.getLogicalType()+", "+fieldName+": "+row.getValue(fieldName) + " ["+e.getMessage()+"]");
                map.put(fieldName, row.getValue(fieldName) + "");
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

    private static boolean listFullOfNulls(List<Object> entries) {
        for (Object key : entries) {
            if (key != null) return false;
        }
        return true;
    }

    private static String mapToString(Map<String, ?> map) {
        String mapAsString = map.keySet().stream()
                .map(key -> key + "=" + map.get(key))
                .collect(Collectors.joining(", ", "{", "}"));
        return mapAsString;
    }

    private static byte[] asBytes(Object obj) throws IOException {
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
        if (o == null) return null;
        DateTime val = null;
        if (o instanceof DateTime) {
            val = ((DateTime) o).toDateTime();
        }
        return val;
    }

    private static Double asDouble(Object o) {
        if (o == null) return null;
        Double val = null;
        if (o instanceof Number) {
            val = ((Number) o).doubleValue();
        }
        return val;
    }

    private static Float asFloat(Object o) {
        if (o == null) return null;
        Float val = null;
        if (o instanceof Number) {
            val = ((Number) o).floatValue();
        }
        return val;
    }

    private static Integer asInteger(Object o) {
        if (o == null) return null;
        Integer val = null;
        if (o instanceof Number) {
            val = ((Number) o).intValue();
        }
        return val;
    }

    private static Boolean asBoolean(Object o) {
        if (o == null) return null;
        Boolean val = null;
        if (o instanceof Boolean) {
            val = ((Boolean) o).booleanValue();
        }
        return val;
    }

    private static String asString(Object o) {
        if (o == null) return null;
        String val = null;
        if (o instanceof String) {
            val = ((String) o);
        }
        return val;
    }

}
