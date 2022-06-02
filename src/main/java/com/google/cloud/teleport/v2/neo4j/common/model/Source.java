package com.google.cloud.teleport.v2.neo4j.common.model;

import com.google.cloud.teleport.v2.neo4j.common.model.enums.SourceType;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class Source implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(Source.class);

    public SourceType sourceType=SourceType.text;
    public String name="";
    public TextFile textFile =new TextFile();

    public Source(final JSONObject sourceObj) {
        this.name = sourceObj.getString("name");

        if (sourceObj.has("file")) {
            this.textFile = new TextFile(sourceObj.getJSONObject("file"));
        }
        this.sourceType=sourceObj.has("type")?SourceType.valueOf(sourceObj.getString("type")):SourceType.text;

    }

    public Integer lookupColIndexByFieldName(String fieldName){
        if (textFile.fieldPosByName.containsKey(fieldName)) {
            return textFile.fieldPosByName.get(fieldName);
        } else {
            LOG.warn("Unable to find field named in input data structure: "+fieldName);
            return -1;
        }
    }


}