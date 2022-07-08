package com.google.cloud.teleport.v2.neo4j.common.model.helpers;

import com.google.cloud.teleport.v2.neo4j.Neo4jFlexTemplateOptions;
import com.google.cloud.teleport.v2.neo4j.common.model.job.OptionsParams;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
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
            optionsParams.overlayTokens(pipelineOptions.getOptionsJson());

        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        return optionsParams;
    }



}
