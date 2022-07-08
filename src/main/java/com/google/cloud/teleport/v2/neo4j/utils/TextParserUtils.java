package com.google.cloud.teleport.v2.neo4j.utils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utils for parsing text files.
 */
public class TextParserUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TextParserUtils.class);
    private static final DateTimeFormatter jsDateTimeFormatter = DateTimeFormat.forPattern("YYYY-MM-DD HH:MM:SSZ");
    private static final DateTimeFormatter jsDateFormatter = DateTimeFormat.forPattern("YYYY-MM-DD");
    private static final SimpleDateFormat jsTimeFormatter = new SimpleDateFormat("HH:MM:SS");


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
                    textCols.add(it.next());
                }
            } catch (IOException ioException) {
                LOG.error("Error parsing line: " + line + ", exception: " + ioException.getMessage());
                return textCols;
            }
        }
        return textCols;
    }

    //Function useful for small in-memory datasets
    public static List<List<Object>> parseDelimitedLines(CSVFormat csvFormat, String[] lines) {
        List rows = new ArrayList<>();
        for (String line : lines) {
            rows.add(parseDelimitedLine(csvFormat, line));
        }
        return rows;
    }

}
