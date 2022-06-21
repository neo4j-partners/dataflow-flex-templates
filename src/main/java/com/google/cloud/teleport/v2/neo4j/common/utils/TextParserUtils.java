package com.google.cloud.teleport.v2.neo4j.common.utils;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
                    textCols.add( it.next());
                }
            } catch (IOException ioException) {
                LOG.error("Error parsing line: " + line + ", exception: " + ioException.getMessage());
                return textCols;
            }
        }
        return textCols;
    }

    //Function useful for small in-memory datasets
    public static List<List<String>> parseDelimitedLines(CSVFormat csvFormat, String csv) {
        List rows = new ArrayList<>();
        if (StringUtils.isEmpty(csv)) {
            return null;
        } else {
            try {
                CSVParser csvParser = CSVParser.parse(csv, csvFormat);
                Iterator<CSVRecord> recordsIterator = csvParser.iterator();
                while (recordsIterator.hasNext()) {
                    CSVRecord csvRecord = recordsIterator.next();
                    List<Object> row = new ArrayList<>();
                    Iterator<String> it = csvRecord.iterator();
                    while (it.hasNext()) {
                        // cast to text
                        row.add( it.next()+"");
                    }
                    rows.add(row);
                }
                LOG.info("Csv format: "+csvFormat.toString()+", csv: "+StringUtils.truncate(csv,100)+", records: "+rows.size());

            } catch (IOException ioException) {
                LOG.error("Error parsing text file, exception: " + ioException.getMessage());
                return rows;
            }
        }
        return rows;
    }

}
