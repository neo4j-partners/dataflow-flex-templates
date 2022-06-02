package com.google.cloud.teleport.v2.neo4j.common.utils;

import com.google.cloud.teleport.v2.neo4j.common.model.Mapping;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.PropertyType;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TextParserUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TextParserUtils.class);
    private static DateTimeFormatter jsDateTimeFormatter = DateTimeFormat.forPattern("YYYY-MM-DD HH:MM:SSZ");
    private static DateTimeFormatter jsDateFormatter = DateTimeFormat.forPattern("YYYY-MM-DD");
    private static SimpleDateFormat jsTimeFormatter = new SimpleDateFormat("HH:MM:SS");


    public static List<Object> parseDelimitedLine(CSVFormat csvFormat, String line) {
        List<Object> textCols = new ArrayList<>();
        if (StringUtils.isEmpty(line)) {
            return null;
        } else {
            try {
                //LOG.info("Csv format: "+csvFormat.toString());
                CSVParser csvParser = CSVParser.parse(line, csvFormat);
                // this is always going to be 1 row
                CSVRecord csvRecord = csvParser.getRecords().get(0);
                Iterator<String> it = csvRecord.iterator();
                while (it.hasNext()) {
                    // cast to text
                    textCols.add( it.next()+"");
                }
            } catch (IOException ioException) {
                LOG.error("Error parsing line: " + line + ", exception: " + ioException.getMessage());
                return textCols;
            }
        }
        return textCols;
    }

    public static Row castRow(final Row strRow, List<Mapping> targetMappings, Schema targetSchema) {
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

}
