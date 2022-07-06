package com.google.cloud.teleport.v2.neo4j.providers;

import com.google.cloud.teleport.v2.neo4j.common.model.enums.SourceType;
import com.google.cloud.teleport.v2.neo4j.providers.bq.BigQueryImpl;
import com.google.cloud.teleport.v2.neo4j.providers.text.TextImpl;

/**
 * Factory for binding implementation adapters into framework.
 * Currently supports two providers: bigquery and text
 */

public class ProviderFactory {
    public static IProvider of(SourceType sourceType) {
        if (sourceType == SourceType.bigquery) {
            return new BigQueryImpl();
        } else if (sourceType == SourceType.text) {
            return new TextImpl();
        } else {
            //TODO: support spanner sql, postgres, parquet, avro
            throw new RuntimeException("Unhandled source type: " + sourceType);
        }
    }
}
