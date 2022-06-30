package com.google.cloud.teleport.v2.neo4j.providers.jdbc;

import lombok.Builder;

@Builder
public class JdbcSpec {
    String readDescription;
    String castDescription;
    String SQL;
}
