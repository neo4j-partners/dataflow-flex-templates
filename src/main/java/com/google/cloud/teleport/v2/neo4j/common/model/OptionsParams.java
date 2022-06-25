package com.google.cloud.teleport.v2.neo4j.common.model;

import com.google.cloud.teleport.v2.neo4j.common.options.Neo4jFlexTemplateOptions;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;

public class OptionsParams implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(OptionsParams.class);

    public String readQuery="";
    public String inputFilePattern="";
    public HashMap<String,String> tokenMap=new HashMap<>();

    public OptionsParams(Neo4jFlexTemplateOptions pipelineOptions) {
        String optionsJsonStr=pipelineOptions.getOptionsJson();
        if (StringUtils.isEmpty(optionsJsonStr)){
            return;
        }
        try {
            LOG.info("Pipeline options: "+optionsJsonStr);
            final JSONObject optionsJson = new JSONObject(optionsJsonStr);
            if (StringUtils.isNotEmpty(pipelineOptions.getReadQuery())){
                readQuery = pipelineOptions.getReadQuery();
            }
            if (StringUtils.isNotEmpty(pipelineOptions.getInputFilePattern())){
                inputFilePattern = pipelineOptions.getInputFilePattern();
            }
            Iterator<String> optionsKeys=optionsJson.keys();
            while (optionsKeys.hasNext()){
                String optionsKey=optionsKeys.next();
                tokenMap.put(optionsKey,optionsJson.opt(optionsKey)+"");
                if (optionsKey.equals("readQuery")) {
                    readQuery = optionsJson.getString("readQuery");
                } else if (optionsKey.equals("inputFilePattern")) {
                    inputFilePattern = optionsJson.getString("inputFilePattern");
                }
                LOG.info(optionsKey + ": "+optionsJson.opt(optionsKey));
            }

        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

    }
}

