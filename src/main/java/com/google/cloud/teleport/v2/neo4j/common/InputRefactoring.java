package com.google.cloud.teleport.v2.neo4j.common;

import com.google.cloud.teleport.v2.neo4j.common.model.JobSpecRequest;
import com.google.cloud.teleport.v2.neo4j.common.model.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.common.model.Source;
import com.google.cloud.teleport.v2.neo4j.common.model.Target;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class InputRefactoring {

    final static Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static final Logger LOG = LoggerFactory.getLogger(InputRefactoring.class);
    OptionsParams optionsParams;

    private InputRefactoring() {
    }

    public InputRefactoring(OptionsParams optionsParams) {
        this.optionsParams = optionsParams;
    }

    public void refactorJobSpec(JobSpecRequest jobSpec) {

        //Create or enrich targets from options
        if (jobSpec.targets.size() == 0) {
            if (jobSpec.options.size() > 0) {
                LOG.info("Targets not found, synthesizing from options");
                throw new RuntimeException("Not currently synthesizing targets from options.");
            } else if (jobSpec.getAllFieldNames().size() == 0) {
                LOG.info("Targets not found, synthesizing from source.  All properties will be indexed.");
                throw new RuntimeException("Not currently auto-generating targets.");
            }
        }

        //replace URI and SQL with run-time options
        for (Source source : jobSpec.getSourceList()) {
            rewriteSource(source);
        }

        //number and name targets
        int targetNum = 0;
        for (Target target : jobSpec.targets) {
            targetNum++;
            target.sequence = targetNum;
            if (StringUtils.isEmpty(target.name)) {
                target.name = "Target " + targetNum;
            }
        }

    }

    public void optimizeJobSpec(JobSpecRequest jobSpec) {

        // NODES first then relationships
        // This does not actually change execution order, just numbering
        Collections.sort(jobSpec.targets);

    }

    private void rewriteSource(Source source) {

        LOG.info("Options params: " + gson.toJson(optionsParams));

        //rewrite file URI
        String dataFileUri = source.uri;
        if (StringUtils.isNotEmpty(optionsParams.inputFilePattern)) {
            LOG.info("Overriding source uri with run-time option");
            dataFileUri = optionsParams.inputFilePattern;
        }
        source.uri = ModelUtils.replaceVariableTokens(dataFileUri, optionsParams.tokenMap);

        //rewrite SQL
        String sql = source.query;
        if (StringUtils.isNotEmpty(optionsParams.readQuery)) {
            LOG.info("Overriding sql with run-time option");
            sql = optionsParams.readQuery;
        }
        source.query = ModelUtils.replaceVariableTokens(sql, optionsParams.tokenMap);

    }

}
