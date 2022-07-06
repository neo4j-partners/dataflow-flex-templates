package com.google.cloud.teleport.v2.neo4j.common.model.job;

import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.common.utils.FileSystemUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Job specification request object.
 */
public class JobSpecRequest implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(JobSpecRequest.class);

    private static final String DEFAULT_SOURCE_NAME = "";
    //initialize defaults;
    public Map<String, Source> sources = new HashMap<>();
    public List<Target> targets = new ArrayList<>();
    public Config config = new Config();
    public Map<String, String> options = new HashMap<>();

    public JobSpecRequest(final String jobSpecUri) {
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
                config = new Config(jobSpecObj.getJSONObject("config"));
            }

            if (jobSpecObj.has("source")) {
                Source source = new Source(jobSpecObj.getJSONObject("source"));
                if (StringUtils.isNotEmpty(source.name)) {
                    sources.put(source.name, source);
                } else {
                    sources.put(DEFAULT_SOURCE_NAME, source);
                }
            } else if (jobSpecObj.has("sources")) {

                JSONArray sourceArray = jobSpecObj.getJSONArray("sources");
                for (int i = 0; i < sourceArray.length(); i++) {
                    final Source source = new Source(sourceArray.getJSONObject(i));
                    if (StringUtils.isNotEmpty(source.name)) {
                        sources.put(source.name, source);
                    } else {
                        sources.put(DEFAULT_SOURCE_NAME, source);
                    }
                }

            } else {
                // there is no source defined this could be used in a big query job...
                // this would be an error
            }

            if (jobSpecObj.has("targets")) {
                final JSONArray targetObjArray = jobSpecObj.getJSONArray("targets");
                for (int i = 0; i < targetObjArray.length(); i++) {
                    final Target targets = new Target(targetObjArray.getJSONObject(i));
                    this.targets.add(targets);
                }
            }

            if (jobSpecObj.has("options")) {
                final JSONArray optionsArray = jobSpecObj.getJSONArray("options");
                for (int i = 0; i < optionsArray.length(); i++) {
                    JSONObject jsonObject = optionsArray.getJSONObject(i);
                    Iterator<String> keys = jsonObject.keys();
                    while (keys.hasNext()) {
                        String key = keys.next();
                        options.put(key, jsonObject.getString(key));
                    }
                }
            }

        } catch (final Exception e) {
            JobSpecRequest.LOG.error(
                    "Unable to parse beam configuration from {}: ", jobSpecUri, e);
            throw new RuntimeException(e);
        }
    }

    public List<Target> getActiveTargetsBySource(String sourceName) {
        List<Target> targets = new ArrayList<>();
        for (Target target : this.targets) {
            if (target.active && target.source.equals(sourceName)) {
                targets.add(target);
            }
        }
        return targets;
    }

    public List<Target> getActiveNodeTargetsBySource(String sourceName) {
        List<Target> targets = new ArrayList<>();
        for (Target target : this.targets) {
            if (target.active && target.type == TargetType.node && target.source.equals(sourceName)) {
                targets.add(target);
            }
        }
        return targets;
    }

    public List<Target> getActiveRelationshipTargetsBySource(String sourceName) {
        List<Target> targets = new ArrayList<>();
        for (Target target : this.targets) {
            if (target.active && target.type == TargetType.edge && target.source.equals(sourceName)) {
                targets.add(target);
            }
        }
        return targets;
    }

    public Source getSourceByName(String name) {
        return
                sources.get(name);
    }

    public Source getDefaultSource() {
        return sources.get(DEFAULT_SOURCE_NAME);
    }

    public List<Source> getSourceList() {
        ArrayList<Source> sourceList = new ArrayList<>();
        Iterator<String> sourceKeySet = sources.keySet().iterator();
        while (sourceKeySet.hasNext()) {
            sourceList.add(sources.get(sourceKeySet.next()));
        }
        return sourceList;
    }

    public List<String> getAllFieldNames() {
        ArrayList<String> fieldNameList = new ArrayList<>();
        for (Target target : targets) {
            fieldNameList.addAll(target.fieldNames);
        }
        return fieldNameList;
    }
}
