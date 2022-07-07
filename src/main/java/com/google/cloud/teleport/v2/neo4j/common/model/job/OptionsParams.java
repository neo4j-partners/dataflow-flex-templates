package com.google.cloud.teleport.v2.neo4j.common.model.job;

import java.io.Serializable;
import java.util.HashMap;
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

}

