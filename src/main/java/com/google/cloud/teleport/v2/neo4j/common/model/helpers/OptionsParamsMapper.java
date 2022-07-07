package com.google.cloud.teleport.v2.neo4j.common.model.helpers;

import com.google.cloud.teleport.v2.neo4j.Neo4jFlexTemplateOptions;
import com.google.cloud.teleport.v2.neo4j.common.model.job.OptionsParams;
import java.util.Iterator;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptionsParamsMapper {

    private static final Logger LOG = LoggerFactory.getLogger(OptionsParamsMapper.class);

    public static OptionsParams fromPipelineOptions(Neo4jFlexTemplateOptions pipelineOptions) {
        OptionsParams optionsParams=new OptionsParams();
        try {

            if (StringUtils.isNotEmpty(pipelineOptions.getReadQuery())) {
                optionsParams.readQuery = pipelineOptions.getReadQuery();
            }
            if (StringUtils.isNotEmpty(pipelineOptions.getInputFilePattern())) {
                optionsParams.inputFilePattern = pipelineOptions.getInputFilePattern();
            }
            overlayTokens(optionsParams,pipelineOptions.getOptionsJson());

        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        return optionsParams;
    }


    public static void overlayTokens(OptionsParams optionsParams, String optionsJsonStr) {
        if (!StringUtils.isEmpty(optionsJsonStr)) {
            LOG.info("Pipeline options: " + optionsJsonStr);
            final JSONObject optionsJson = new JSONObject(optionsJsonStr);
            Iterator<String> optionsKeys = optionsJson.keys();
            while (optionsKeys.hasNext()) {
                String optionsKey = optionsKeys.next();
                optionsParams.tokenMap.put(optionsKey, optionsJson.opt(optionsKey) + "");
                if (optionsKey.equals("readQuery")) {
                    optionsParams.readQuery = optionsJson.getString("readQuery");
                } else if (optionsKey.equals("inputFilePattern")) {
                    optionsParams.inputFilePattern = optionsJson.getString("inputFilePattern");
                }
                LOG.info(optionsKey + ": " + optionsJson.opt(optionsKey));
            }
        }
    }
}
