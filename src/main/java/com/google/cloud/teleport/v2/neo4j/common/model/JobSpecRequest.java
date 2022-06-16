package com.google.cloud.teleport.v2.neo4j.common.model;

import com.google.cloud.teleport.v2.neo4j.common.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.common.utils.GsUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class JobSpecRequest implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(JobSpecRequest.class);

    //initialize defaults;
    public Source source;
    public List<Target> targets = new ArrayList<>();
    public Config config =new Config();
    public Map<String,String> options= new HashMap<>();

    public JobSpecRequest(final String jobSpecUri) {
        String jobSpecJsonStr = "{}";
        try {
            jobSpecJsonStr = GsUtils.getPathContents(jobSpecUri);
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
                source = new Source(jobSpecObj.getJSONObject("source"));
            } else {
                // there is no source defined this could be used in a big query job...
            }

            if (jobSpecObj.has("targets")) {
                final JSONArray targetObjArray = jobSpecObj.getJSONArray("targets");
                for (int i = 0; i < targetObjArray.length(); i++) {
                    final Target targets = new Target( targetObjArray.getJSONObject(i));
                    this.targets.add(targets);
                }
            }

            if (jobSpecObj.has("options")) {
                final JSONArray optionsArray = jobSpecObj.getJSONArray("options");
                for (int i = 0; i < optionsArray.length(); i++) {
                    JSONObject jsonObject = optionsArray.getJSONObject(i);
                    Iterator<String> keys = jsonObject.keys();
                    while (keys.hasNext()){
                        String key=keys.next();
                        options.put(key,jsonObject.getString(key));
                    }
                }
            }

        } catch (final Exception e) {
            JobSpecRequest.LOG.error(
                    "Unable to parse beam configuration from {}: ", jobSpecUri, e);
            throw new RuntimeException(e);
        }
    }

    public List<Target> getActiveTargets(){
        List<Target> targets=new ArrayList<>();
        for (Target target : this.targets) {
            if (target.active) {
                targets.add(target);
            }
        }
        return targets;
    }

    public List<Target> getActiveNodeTargets(){
        List<Target> targets=new ArrayList<>();
        for (Target target : this.targets) {
            if (target.active && target.type==TargetType.node) {
                targets.add(target);
            }
        }
        return targets;
    }

        public List<Target> getActiveRelationshipTargets(){
            List<Target> targets=new ArrayList<>();
            for (Target target : this.targets) {
                if (target.active && target.type==TargetType.relationship) {
                    targets.add(target);
                }
            }
            return targets;
        }
}
