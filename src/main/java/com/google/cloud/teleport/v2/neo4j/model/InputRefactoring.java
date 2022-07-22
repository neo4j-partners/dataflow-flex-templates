package com.google.cloud.teleport.v2.neo4j.model;

import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.Collections;
import java.util.Iterator;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Synthesizes and optimizes missing elements to model from inputs.
 */
public class InputRefactoring {

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static final Logger LOG = LoggerFactory.getLogger(InputRefactoring.class);
    OptionsParams optionsParams;

    private InputRefactoring() {
    }

    /**
     * Constructor uses template input options.
     *
     * @param optionsParams
     */
    public InputRefactoring(OptionsParams optionsParams) {
        this.optionsParams = optionsParams;
    }

    public void refactorJobSpec(JobSpec jobSpec) {

        //Create or enrich targets from options
        if (jobSpec.targets.size() == 0) {
            if (jobSpec.options.size() > 0) {
                LOG.info("Targets not found, synthesizing from options");
                throw new RuntimeException("Not currently synthesizing targets from options.");
            }
            // targets defined but no field names defined.
        } else if (jobSpec.getAllFieldNames().size() == 0) {
            LOG.info("Targets not found, synthesizing from source.  All properties will be indexed.");
            throw new RuntimeException("Not currently auto-generating targets.");
        }

        LOG.info("Options params: " + gson.toJson(optionsParams));

        //replace URI and SQL with run-time options
        for (Source source : jobSpec.getSourceList()) {
            rewriteSource(source);
        }

        for (Action action : jobSpec.actions) {
            rewriteAction(action);
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

    public void optimizeJobSpec(JobSpec jobSpec) {

        // NODES first then relationships
        // This does not actually change execution order, just numbering
        Collections.sort(jobSpec.targets);

    }

    private void rewriteSource(Source source) {

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

    private void rewriteAction(Action action) {
        Iterator<String> optionIt = action.options.keySet().iterator();
        while (optionIt.hasNext()) {
            String key = optionIt.next();
            String value=action.options.get(key);
            action.options.put( key,ModelUtils.replaceVariableTokens(value, optionsParams.tokenMap));
        }
    }

}
