package com.google.cloud.teleport.v2.neo4j.utils;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for property attribute decryption.
 */
public class GcpSecretUtils {
    private static final Logger LOG = LoggerFactory.getLogger(GcpSecretUtils.class);

    private static String resolveSecret(String secretName) {
        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            AccessSecretVersionResponse response = client.accessSecretVersion(secretName);

            return response.getPayload().getData().toStringUtf8();
        } catch (IOException e) {
            throw new RuntimeException("Unable to read secret: " + secretName);
        }
    }

}
