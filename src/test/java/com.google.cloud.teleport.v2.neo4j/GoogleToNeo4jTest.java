/*
 * Copyright (C) 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.neo4j;

import com.google.cloud.teleport.v2.neo4j.common.InputValidator;
import com.google.cloud.teleport.v2.neo4j.common.model.*;
import com.google.cloud.teleport.v2.neo4j.common.utils.ModelUtils;
import com.google.cloud.teleport.v2.neo4j.providers.IProvider;
import com.google.cloud.teleport.v2.neo4j.providers.ProviderFactory;
import com.google.cloud.teleport.v2.neo4j.providers.text.TextImpl;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Unit tests for {@link GcpToNeo4j}.
 */
@RunWith(JUnit4.class)
public class GoogleToNeo4jTest {

    final static Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public transient TestPipeline pipeline = TestPipeline.create();
    private transient PCollection<Row> textRows;
    private static IProvider providerImpl;
    private static ConnectionParams neo4jConnection;
    private static JobSpecRequest jobSpec;
    private static OptionsParams optionsParams;

    private static final Logger LOG = LoggerFactory.getLogger(GoogleToNeo4jTest.class);

    //Exception examples:
    // https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/pubsub-binary-to-bigquery/src/test/java/com/google/cloud/teleport/v2/templates/PubsubProtoToBigQueryTest.java

    @Before
    public void setUp() throws InterruptedException, IOException {
        if (jobSpec == null) {
            LOG.info("Initializing...");
            neo4jConnection = new ConnectionParams("gs://neo4j-dataflow/job-specs/testing/common/auradb-free-connection.json");
            jobSpec = new JobSpecRequest("gs://neo4j-dataflow/job-specs/testing/new/text-northwind-jobspec.json");
            providerImpl = ProviderFactory.of(jobSpec.getSourceList().get(0).sourceType);
            optionsParams = new OptionsParams();
            optionsParams.overlayTokens("{\"limit\":7}");
            providerImpl.configure(optionsParams, jobSpec);
        }
    }

    @Test
    public void testValidateSourceType() {
        Assert.assertTrue(providerImpl.getClass() == TextImpl.class);
    }

    @Test
    public void testValidJobSpec() {
        List<String> sourceValidationMessages = providerImpl.validateJobSpec();
        Assert.assertTrue(sourceValidationMessages.size() == 0);
    }

    @Test
    public void testResolvedVariable() {
        Assert.assertTrue("7".equals(optionsParams.tokenMap.get("limit")));
    }

    @Test
    public void testGetInvalidOrderQuery() {
        Source source = this.jobSpec.getSourceList().get(0);
        source.query = "SELECT * FROM FOO ORDER BY X";
        List<String> messages = InputValidator.validateJobSpec(this.jobSpec);
        Assert.assertTrue(ModelUtils.messagesContains(messages, "SQL contains ORDER BY which is not supported"));
    }

    @Test
    public void testCastDateTransform() {

        Assert.assertTrue(true);
    }


}
