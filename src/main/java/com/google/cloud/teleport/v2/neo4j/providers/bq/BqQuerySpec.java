package com.google.cloud.teleport.v2.neo4j.providers.bq;

import lombok.Builder;

@Builder
public class BqQuerySpec {
    String readDescription;
    String castDescription;
    String SQL;
}
