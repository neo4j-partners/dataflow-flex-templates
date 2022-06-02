package com.google.cloud.teleport.v2.neo4j.common.model;

import com.google.cloud.teleport.v2.neo4j.common.Neo4jUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class JobSpecRequest {

    private static final Logger LOG = LoggerFactory.getLogger(JobSpecRequest.class);

    //initialize defaults;
    public Source source;
    public List<Targets> targets = new ArrayList<>();
    public Config config =new Config();

    public JobSpecRequest(final String jobSpecUri) {
        String jobSpecJsonStr = "{}";
        try {
            jobSpecJsonStr = Neo4jUtils.getGcsPathContents(jobSpecUri);
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
                    final Targets targets = new Targets( targetObjArray.getJSONObject(i));
                    this.targets.add(targets);
                }
            }

        } catch (final Exception e) {
            JobSpecRequest.LOG.error(
                    "Unable to parse beam configuration from {}: ", jobSpecUri, e);
            throw new RuntimeException(e);
        }
        boolean valid=validateSpec();
    }

    private boolean validateSpec(){
        //TODO: validate job sepc
        return true;
    }

}
