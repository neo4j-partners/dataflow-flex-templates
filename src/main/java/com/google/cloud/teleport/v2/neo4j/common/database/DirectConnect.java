package com.google.cloud.teleport.v2.neo4j.common.database;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.cloud.teleport.v2.neo4j.common.model.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.common.model.enums.AuthType;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import org.neo4j.driver.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class DirectConnect {

    private static final Logger LOG = LoggerFactory.getLogger(DirectConnect.class);

    public String serverUrl = null;
    public String database = null;
    public AuthType authType = AuthType.BASIC;
    private final String username;
    private final String password;
    Driver driver = null;

    public DirectConnect(ConnectionParams connectionParams) {
        this.username = connectionParams.username;
        this.password = connectionParams.password;
        this.database = connectionParams.database;
        this.serverUrl = connectionParams.serverUrl;
    }

    public DirectConnect(String hostName, int port, String database, String username, String password) {
        this.username = username;
        this.password = password;
        this.database = database;
        this.serverUrl = getUrl(hostName, port);
    }

    public DirectConnect(String serverUrl, String database, String username, String password) {
        this.username = username;
        this.password = password;
        this.database = database;
        this.serverUrl = serverUrl;
    }

    private final String getUrl(String hostName, int port) {
        return "neo4j+s://" + hostName + ":" + port;
    }

    public Driver getDriver() throws URISyntaxException {
        if (this.authType != AuthType.BASIC) {
            LOG.error("Unsupported authType: " + this.authType);
            throw new RuntimeException("Unsupported authentication type: " + this.authType);
        }
        if (this.serverUrl.contains("neo4j+s")) {
            return GraphDatabase.driver(new URI(this.serverUrl), AuthTokens.basic(this.username, this.password),
                    Config.builder().build());
        } else {
            return GraphDatabase.routingDriver(
                    Arrays.asList(new URI(this.serverUrl)),
                    AuthTokens.basic(this.username, this.password),
                    Config.builder().build());
        }
    }

    public Session getSession(boolean withDatabase) throws URISyntaxException {
        if (driver == null) {
            this.driver = getDriver();
        }
        SessionConfig.Builder builder = SessionConfig.builder();
        if (withDatabase) {
            builder = builder.withDatabase(this.database);
        }
        return driver.session(builder.build());
    }


    public void executeOnNeo4j(String cypher, boolean useDatabase) throws Exception {
        try (Session session = getSession(useDatabase)) {
            session.run(cypher);
        }
    }

    public void resetNeo4j() {
        // Direct connect utility...
         LOG.info("Resetting database");
        try {
            LOG.info("Executing cypher: " + ModelUtils.CYPHER_DELETE_ALL);
            executeOnNeo4j(
                    ModelUtils.CYPHER_DELETE_ALL,
                    true);
        } catch (Exception e) {
            LOG.error("Error executing cypher: " + ModelUtils.CYPHER_DELETE_ALL + ", " + e.getMessage());
        }
    }

}