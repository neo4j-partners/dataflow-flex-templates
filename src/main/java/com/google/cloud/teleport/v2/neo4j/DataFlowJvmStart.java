package com.google.cloud.teleport.v2.neo4j;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Security;

/**
 * This class will be picked up and used at the very start of a DataFlow JVM. As such it allows us
 * to disable certain security algorithms so that secure connections automatically pick up the right
 * one. This is an issue for secure Kafka and Neo4j Aura connections (neo4j+s:// protocol).
 */
@AutoService(value = JvmInitializer.class)
public class DataFlowJvmStart implements JvmInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(DataFlowJvmStart.class);

    @Override
    public void onStartup() {
        LOG.info("Initializing JVM.  Disabling certain SSL algorithms.");
        Security.setProperty(
                "jdk.tls.disabledAlgorithms",
                "SSLv3, RC4, DES, MD5withRSA, DH keySize < 1024, EC keySize < 224, 3DES_EDE_CBC, anon, NULL");
    }

    @Override
    public void beforeProcessing(PipelineOptions options) {
    }
}
