package com.google.cloud.teleport.v2.neo4j.model.job;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.annotate.JsonIgnore;
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

    public OptionsParams() {
    }

    @JsonIgnore
    public void overlayTokens(String optionsJsonStr) {
        if (!StringUtils.isEmpty(optionsJsonStr)) {
            LOG.info("Pipeline options: " + optionsJsonStr);
            final JSONObject optionsJson = new JSONObject(optionsJsonStr);
            Iterator<String> optionsKeys = optionsJson.keys();
            while (optionsKeys.hasNext()) {
                String optionsKey = optionsKeys.next();
                this.tokenMap.put(optionsKey, optionsJson.opt(optionsKey) + "");
                if (optionsKey.equals("readQuery")) {
                    this.readQuery = optionsJson.getString("readQuery");
                } else if (optionsKey.equals("inputFilePattern")) {
                    this.inputFilePattern = optionsJson.getString("inputFilePattern");
                }
                LOG.info(optionsKey + ": " + optionsJson.opt(optionsKey));
            }
        }
    }
}

