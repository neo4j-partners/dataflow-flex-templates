package com.google.cloud.teleport.v2.neo4j.common.model;

import com.google.cloud.teleport.v2.neo4j.common.model.enums.AvroType;
import org.json.JSONObject;

import java.io.Serializable;

public class Config implements Serializable {
    public Boolean resetDb = false;
    public Boolean indexAllProperties = false;

    public String auditGsUri;
    public AvroType avroType = AvroType.parquet;

    public Integer nodeParallelism = 5;
    public Integer edgeParallelism = 1;
    public Integer nodeBatchSize = 5000;
    public Integer edgeBatchSize = 1000;

    public Config() {
    }

    public Config(final JSONObject jsonObject) {
        resetDb = jsonObject.has("reset_db") && jsonObject.getBoolean("reset_db");
        auditGsUri = jsonObject.has("audit_gcs_uri") ? jsonObject.getString("audit_gcs_uri") : auditGsUri;
        nodeParallelism = jsonObject.has("node_write_batch_size") ? jsonObject.getInt("node_write_batch_size") : nodeParallelism;
        edgeParallelism = jsonObject.has("edge_write_batch_size") ? jsonObject.getInt("edge_write_batch_size") : edgeParallelism;
        // not currently implemented
        nodeBatchSize = jsonObject.has("node_write_parallelism") ? jsonObject.getInt("node_write_parallelism") : nodeBatchSize;
        edgeBatchSize = jsonObject.has("edge_write_parallelism") ? jsonObject.getInt("edge_write_parallelism") : edgeBatchSize;
        indexAllProperties = jsonObject.has("index_all_properties") ? jsonObject.getBoolean("index_all_properties") : indexAllProperties;

    }
}
