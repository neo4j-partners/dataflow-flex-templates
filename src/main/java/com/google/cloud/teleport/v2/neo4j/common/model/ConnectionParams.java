package com.google.cloud.teleport.v2.neo4j.common.model;

import com.google.cloud.teleport.v2.neo4j.common.Neo4jUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class ConnectionParams implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionParams.class);

    public String
            serverUrl,
    database,
            authType,
            username,
            password;

    public ConnectionParams(final String neoConnectionUri) {

        String neoConnectionJsonStr = "{}";
        try {
            neoConnectionJsonStr = Neo4jUtils.getGcsPathContents(neoConnectionUri);
        } catch (final Exception e) {
            LOG.error("Unable to read {} neo4j configuration: ", neoConnectionUri, e);
        }

        try {
            final JSONObject neoConnectionJson = new JSONObject(neoConnectionJsonStr);
            serverUrl = neoConnectionJson.getString("server_url");
            if (neoConnectionJson.has("auth_type")) {
                authType = neoConnectionJson.getString("auth_type");
            } else {
                authType = "basic";
            }
            database = neoConnectionJson.has("database")?neoConnectionJson.getString("database"):"neo4j";
            username = neoConnectionJson.getString("username");
            password = neoConnectionJson.getString("pwd");

        } catch (final Exception e) {
            throw new RuntimeException(e);
        }


    }
}

