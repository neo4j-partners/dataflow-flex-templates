package com.google.cloud.teleport.v2.neo4j.common.model;

import org.json.JSONObject;

import java.io.Serializable;

public class Config implements Serializable {
    public Boolean resetDb = false;
    public Config(){}
    public Config(final JSONObject jsonObject) {
        resetDb = jsonObject.has("reset_db")?jsonObject.getBoolean("reset_db"):false;
    }
}