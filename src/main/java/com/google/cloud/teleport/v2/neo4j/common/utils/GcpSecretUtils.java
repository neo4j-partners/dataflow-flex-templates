package com.google.cloud.teleport.v2.neo4j.common.utils;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;

import java.io.IOException;

public class GcpSecretUtils {

    private static String resolveSecret(String secretName) {
        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            AccessSecretVersionResponse response = client.accessSecretVersion(secretName);

            return response.getPayload().getData().toStringUtf8();
        } catch (IOException e) {
            throw new RuntimeException("Unable to read secret: "+secretName);
        }
    }

}
