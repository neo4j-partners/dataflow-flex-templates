package com.google.cloud.teleport.v2.neo4j.common.model;

import com.google.cloud.teleport.v2.neo4j.common.model.enums.SourceType;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.TextFormat;
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

public class Source implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(Source.class);
    public SourceType sourceType=SourceType.text;
    public String name="";
    public String uri = "";
    public String delimiter = "";
    //row separator
    public String separator = "";

    public String query="";
    public TextFormat textFormat= TextFormat.CSV;
    public CSVFormat csvFormat = CSVFormat.DEFAULT;
    public String[] fieldNames = new String[0];
    public Map<String, Integer> fieldPosByName = new HashMap();
    public List<List<String>> inline =null;

    public Source(final JSONObject sourceObj) {
        this.name = sourceObj.getString("name");
        //TODO: avro, parquet, etc.
        this.sourceType=sourceObj.has("type")?SourceType.valueOf(sourceObj.getString("type")):SourceType.text;

        String csvFormatStr = sourceObj.has("format") ? sourceObj.getString("format").toUpperCase() : "DEFAULT";
        if (csvFormatStr.equals("JSON")){
            textFormat = TextFormat.JSON;
        } else if (csvFormatStr.equals("EXCEL")) {
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
        } else {
            csvFormat = CSVFormat.DEFAULT;
        }

        delimiter = sourceObj.has("delimiter") ? sourceObj.getString("delimiter") : "";
        separator = sourceObj.has("separator") ? sourceObj.getString("separator") : "";
        //handle inline data
        if (sourceObj.has("data")){
            if (textFormat==TextFormat.JSON) {
                this.inline = jsonToListArray(sourceObj.getJSONArray("data"));
            } else {
                String csv=sourceObj.getString("data");
                if (StringUtils.isNotEmpty(separator)){
                    csv=StringUtils.join(StringUtils.split(csv,separator),System.lineSeparator());
                }
                this.inline = TextParserUtils.parseDelimitedLines(csvFormat,csv);
            }
        }
        query = sourceObj.has("query") ? sourceObj.getString("query"):"";
        uri = sourceObj.has("uri") ? sourceObj.getString("uri"):"";
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

    public Integer lookupColIndexByFieldName(String fieldName){
        if (fieldPosByName.containsKey(fieldName)) {
            return fieldPosByName.get(fieldName);
        } else {
            LOG.warn("Unable to find field named in input data structure: "+fieldName);
            return -1;
        }
    }
    public Schema getTextFileSchema(){
        return BeamUtils.textToBeamSchema(fieldNames);
    }

    public static List<List<String>> jsonToListArray(JSONArray lines) {
        if(lines==null)
            return null;

        List<List<String>> rows=new ArrayList<>();
        for(int i=0; i<lines.length(); i++) {
            JSONArray rowArr=lines.getJSONArray(i);
            List<String> tuples=new ArrayList<>();
            for(int j=0; j<rowArr.length(); j++) {
                tuples.add(rowArr.optString(j));
            }
            rows.add(tuples);
        }
        return rows;
    }

}