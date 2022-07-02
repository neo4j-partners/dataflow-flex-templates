package com.google.cloud.teleport.v2.neo4j.common.model;

import com.google.cloud.teleport.v2.neo4j.common.model.enums.SourceType;
import com.google.cloud.teleport.v2.neo4j.common.utils.BeamUtils;
import com.google.cloud.teleport.v2.neo4j.common.utils.TextParserUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.commons.csv.CSVFormat;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class Source implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(Source.class);


    static Pattern NEWLINE_PATTERN = Pattern.compile("\\R");

    public SourceType sourceType = SourceType.text;
    public String name = "";
    public String uri = "";
    public String delimiter = ",";
    //row separator
    public String separator;

    public String query = "";
    public CSVFormat csvFormat = CSVFormat.DEFAULT;
    public String[] fieldNames = new String[0];
    public Map<String, Integer> fieldPosByName = new HashMap();
    public List<List<Object>> inline = new ArrayList<>();

    public Source(final JSONObject sourceObj) {
        this.name = sourceObj.getString("name");
        //TODO: avro, parquet, etc.
        this.sourceType = sourceObj.has("type") ? SourceType.valueOf(sourceObj.getString("type")) : SourceType.text;

        boolean isJson = false;
        String formatStr = sourceObj.has("format") ? sourceObj.getString("format").toUpperCase() : "DEFAULT";
        if ("EXCEL".equals(formatStr)) {
            csvFormat = CSVFormat.EXCEL;
        } else if ("MONGO".equals(formatStr)) {
            csvFormat = CSVFormat.MONGODB_CSV;
        } else if ("INFORMIX".equals(formatStr)) {
            csvFormat = CSVFormat.INFORMIX_UNLOAD_CSV;
        } else if ("POSTGRES".equals(formatStr)) {
            csvFormat = CSVFormat.POSTGRESQL_CSV;
        } else if ("MYSQL".equals(formatStr)) {
            csvFormat = CSVFormat.MYSQL;
        } else if ("ORACLE".equals(formatStr)) {
            csvFormat = CSVFormat.ORACLE;
        } else if ("MONGO_TSV".equals(formatStr)) {
            csvFormat = CSVFormat.MONGODB_TSV;
        } else if ("RFC4180".equals(formatStr)) {
            csvFormat = CSVFormat.RFC4180;
        } else if ("POSTGRESQL_CSV".equals(formatStr)) {
            csvFormat = CSVFormat.POSTGRESQL_CSV;
        } else {
            csvFormat = CSVFormat.DEFAULT;
        }

        delimiter = sourceObj.has("delimiter") ? sourceObj.getString("delimiter") : delimiter;
        separator = sourceObj.has("separator") ? sourceObj.getString("separator") : separator;
        //handle inline data
        if (sourceObj.has("data")) {
            if (sourceObj.get("data") instanceof JSONArray) {

                if (csvFormat == CSVFormat.DEFAULT) {
                    this.inline = jsonToListOfListsArray(sourceObj.getJSONArray("data"));
                } else {
                    String[] rows = jsonToListOfStringArray(sourceObj.getJSONArray("data"), delimiter);
                    this.inline = TextParserUtils.parseDelimitedLines(csvFormat, rows);
                }

            } else {
                String csv = sourceObj.getString("data");
                String[] rows;
                if (separator != null && csv.contains(separator)) {
                    rows = StringUtils.split(csv, separator);
                    // we may have more luck with varieties of newline
                } else {
                    rows = NEWLINE_PATTERN.split(csv);
                }
                if (rows.length < 2) {
                    String errMsg = "Cold not parse inline data.  Check separator: " + separator;
                    LOG.error(errMsg);
                    throw new RuntimeException(errMsg);
                }
                this.inline = TextParserUtils.parseDelimitedLines(csvFormat, rows);
            }
        }
        query = sourceObj.has("query") ? sourceObj.getString("query") : "";
        uri = sourceObj.has("uri") ? sourceObj.getString("uri") : "";
        final String colNamesStr = sourceObj.has("ordered_field_names") ? sourceObj.getString("ordered_field_names") : "";
        if (StringUtils.isNotEmpty(colNamesStr)) {
            fieldNames = StringUtils.split(colNamesStr, ",");
            for (int i = 0; i < fieldNames.length; i++) {
                fieldPosByName.put(fieldNames[i], (i + 1));
            }
        }
        if (StringUtils.isNotEmpty(delimiter)) {
            csvFormat.withDelimiter(delimiter.charAt(0));
        }
    }

    public static List<List<Object>> jsonToListOfListsArray(JSONArray lines) {
        if (lines == null) {
            return new ArrayList<>();
        }

        List<List<Object>> rows = new ArrayList<>();
        for (int i = 0; i < lines.length(); i++) {
            JSONArray rowArr = lines.getJSONArray(i);
            List<Object> tuples = new ArrayList<>();
            for (int j = 0; j < rowArr.length(); j++) {
                tuples.add(rowArr.optString(j));
            }
            rows.add(tuples);
        }
        return rows;
    }

    public static String[] jsonToListOfStringArray(JSONArray lines, String delimiter) {
        if (lines == null) {
            return new String[0];
        }

        String[] rows = new String[lines.length()];
        for (int i = 0; i < lines.length(); i++) {
            JSONArray rowArr = lines.getJSONArray(i);
            StringBuffer sb = new StringBuffer();
            for (int j = 0; j < rowArr.length(); j++) {
                if (j > 0) {
                    sb.append(delimiter);
                }
                sb.append(rowArr.optString(j));
            }
            rows[i] = sb.toString();
        }
        return rows;
    }

    public Schema getTextFileSchema() {
        return BeamUtils.textToBeamSchema(fieldNames);
    }

}
