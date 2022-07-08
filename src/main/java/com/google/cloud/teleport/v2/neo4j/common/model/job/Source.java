package com.google.cloud.teleport.v2.neo4j.common.model.job;

import com.google.cloud.teleport.v2.neo4j.common.model.enums.ActionExecuteAfter;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.SourceType;
import com.google.cloud.teleport.v2.neo4j.common.utils.BeamUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.commons.csv.CSVFormat;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Source query metdata.
 */
public class Source implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Source.class);
    public SourceType sourceType = SourceType.text;
    public String name = "";
    public String uri = "";
    public String delimiter = ",";
    //row separator
    public String separator = null;

    public String query = "";
    public CSVFormat csvFormat = CSVFormat.DEFAULT;
    public String[] fieldNames = new String[0];
    public Map<String, Integer> fieldPosByName = new HashMap();
    public List<List<Object>> inline = new ArrayList<>();

    public ActionExecuteAfter executeAfter = ActionExecuteAfter.preloads;
    public String executeAfterName = "";

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