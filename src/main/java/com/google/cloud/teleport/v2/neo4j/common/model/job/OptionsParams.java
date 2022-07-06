package com.google.cloud.teleport.v2.neo4j.common.model.job;

import com.google.cloud.teleport.v2.neo4j.Neo4jFlexTemplateOptions;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.OptionKey;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runtime options object that coalesces well-known (readQuery, inputFilePattern) and arbitrary options.
 */

public class OptionsParams implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(OptionsParams.class);

    public String readQuery = "";
    public String inputFilePattern = "";
    public HashMap<String, String> tokenMap = new HashMap<>();

    //for testing
    public OptionsParams() {
    }

    public OptionsParams(Neo4jFlexTemplateOptions pipelineOptions) {

        try {

            if (StringUtils.isNotEmpty(pipelineOptions.getReadQuery())) {
                readQuery = pipelineOptions.getReadQuery();
            }
            if (StringUtils.isNotEmpty(pipelineOptions.getInputFilePattern())) {
                inputFilePattern = pipelineOptions.getInputFilePattern();
            }
            overlayTokens(pipelineOptions.getOptionsJson());

        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

    }


    public void overlayTokens(String optionsJsonStr) {
        if (!StringUtils.isEmpty(optionsJsonStr)) {
            LOG.info("Pipeline options: " + optionsJsonStr);
            final JSONObject optionsJson = new JSONObject(optionsJsonStr);
            Iterator<String> optionsKeys = optionsJson.keys();
            while (optionsKeys.hasNext()) {
                String optionsKey = optionsKeys.next();
                tokenMap.put(optionsKey, optionsJson.opt(optionsKey) + "");
                if (optionsKey.equals("readQuery")) {
                    readQuery = optionsJson.getString("readQuery");
                } else if (optionsKey.equals("inputFilePattern")) {
                    inputFilePattern = optionsJson.getString("inputFilePattern");
                }
                LOG.info(optionsKey + ": " + optionsJson.opt(optionsKey));
            }
        }
    }
    /**
     * Inner class for modelling structured options.
     */
    protected class Option {
        OptionKey optionKey;
        String optionValue;
    }

}

