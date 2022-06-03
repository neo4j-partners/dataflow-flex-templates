package com.google.cloud.teleport.v2.neo4j.common.utils;

import com.google.cloud.teleport.v2.neo4j.common.model.Mapping;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.PropertyType;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.io.FileSystems;
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

import java.io.*;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
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

}
