package com.google.cloud.teleport.v2.neo4j.common.model.helpers;

import com.google.cloud.teleport.v2.neo4j.common.model.job.*;
import com.google.cloud.teleport.v2.neo4j.common.utils.FileSystemUtils;
import java.util.Iterator;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for parsing JobSpec json files, accepts file URI as entry point.
 */
public class JobSpecMapper {

    private static final Logger LOG = LoggerFactory.getLogger(JobSpecMapper.class);
    private static final String DEFAULT_SOURCE_NAME = "";

    public static JobSpec fromUri(final String jobSpecUri) {

        JobSpec jobSpecRequest=new JobSpec();

        String jobSpecJsonStr = "{}";
        try {
            jobSpecJsonStr = FileSystemUtils.getPathContents(jobSpecUri);
        } catch (final Exception e) {
            LOG.error(
                    "Unable to read {} neo4j job specification: ", jobSpecUri, e);
            throw new RuntimeException(e);
        }

        try {
            final JSONObject jobSpecObj = new JSONObject(jobSpecJsonStr);

            if (jobSpecObj.has("config")) {
                jobSpecRequest.config = new Config(jobSpecObj.getJSONObject("config"));
            }

            if (jobSpecObj.has("source")) {
                Source source = SourceMapper.fromJson(jobSpecObj.getJSONObject("source"));
                if (StringUtils.isNotEmpty(source.name)) {
                    jobSpecRequest.sources.put(source.name, source);
                } else {
                    jobSpecRequest.sources.put(DEFAULT_SOURCE_NAME, source);
                }
            } else if (jobSpecObj.has("sources")) {

                JSONArray sourceArray = jobSpecObj.getJSONArray("sources");
                for (int i = 0; i < sourceArray.length(); i++) {
                    final Source source = SourceMapper.fromJson(sourceArray.getJSONObject(i));
                    if (StringUtils.isNotEmpty(source.name)) {
                        jobSpecRequest.sources.put(source.name, source);
                    } else {
                        jobSpecRequest.sources.put(DEFAULT_SOURCE_NAME, source);
                    }
                }
            } else {
                // there is no source defined this could be used in a big query job...
                // this would lead to a validation error (elsewhere)
            }

            if (jobSpecObj.has("targets")) {
                final JSONArray targetObjArray = jobSpecObj.getJSONArray("targets");
                for (int i = 0; i < targetObjArray.length(); i++) {
                    final Target target = TargetMapper.fromJson(targetObjArray.getJSONObject(i));
                    jobSpecRequest.targets.add(target);
                }
            }

            //Note: this options array was created to allow mimicing Spark syntax.
            //It is currently unused.
            if (jobSpecObj.has("options")) {
                final JSONArray optionsArray = jobSpecObj.getJSONArray("options");
                for (int i = 0; i < optionsArray.length(); i++) {
                    JSONObject jsonObject = optionsArray.getJSONObject(i);
                    Iterator<String> keys = jsonObject.keys();
                    while (keys.hasNext()) {
                        String key = keys.next();
                        jobSpecRequest.options.put(key, jsonObject.getString(key));
                    }
                }
            }

            if (jobSpecObj.has("actions")) {
                final JSONArray optionsArray = jobSpecObj.getJSONArray("actions");
                for (int i = 0; i < optionsArray.length(); i++) {
                    JSONObject jsonObject = optionsArray.getJSONObject(i);
                    Action action=ActionMapper.fromJson(jsonObject);
                    jobSpecRequest.actions.add(action);
                }
            }

        } catch (final Exception e) {
            LOG.error(
                    "Unable to parse beam configuration from {}: ", jobSpecUri, e);
            throw new RuntimeException(e);
        }

        return jobSpecRequest;
    }
}
