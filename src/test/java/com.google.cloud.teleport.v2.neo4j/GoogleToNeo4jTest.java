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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.common.collect.ImmutableList;
import com.google.re2j.Pattern;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static com.google.re2j.Pattern.CASE_INSENSITIVE;
import static com.google.re2j.Pattern.DOTALL;

/**
 * Unit tests for {@link GcpToNeo4j}.
 */
@RunWith(JUnit4.class)
public class GoogleToNeo4jTest {
    private static final String PROJECT = "test-project1";
    private static final String DATASET = "test-dataset1";
    private static final int MAX_PARALLEL_REQUESTS = 5;

    @Rule
    public final MockitoRule mockito = MockitoJUnit.rule();
    @Rule
    public final TemporaryFolder tmpDir = new TemporaryFolder();
    @Rule
    public final TestPipeline testPipeline = TestPipeline.create();

    // bqMock has to be static, otherwise it won't be serialized properly when passed to
    // DeleteBigQueryDataFn#withTestBqClientFactory.
    @Mock
    private static BigQuery bqMock;
    @Mock
    private BigQueryStorageClient bqsMock;
    @Mock
    private TableResult tableResultMock;
    private BigQueryServices bqFakeServices;
    private FakeDatasetService fakeDatasetService;
    private File outDir;
    private TableRow[] defaultRecords;
    private String[] defaultExpectedRecords;
    private TableSchema bqSchema;
    private Schema avroSchema;


    @Before
    public void setUp() throws InterruptedException, IOException {

        outDir = tmpDir.newFolder("out");

        bqSchema =
                new TableSchema()
                        .setFields(
                                ImmutableList.of(
                                        new TableFieldSchema().setName("ts").setType("TIMESTAMP"),
                                        new TableFieldSchema().setName("s1").setType("STRING"),
                                        new TableFieldSchema().setName("d1").setType("DATE"),
                                        new TableFieldSchema().setName("t1").setType("TIME").setMode("REQUIRED"),
                                        new TableFieldSchema().setName("dt").setType("DATETIME"),
                                        new TableFieldSchema().setName("i1").setType("INTEGER")));

        avroSchema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"__root__\",\"fields\":"
                                        + "[{\"name\":\"ts\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]},"
                                        + "{\"name\":\"s1\",\"type\":[\"null\",\"string\"]},"
                                        + "{\"name\":\"d1\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]},"
                                        + "{\"name\":\"t1\",\"type\":{\"type\":\"long\",\"logicalType\":\"time-micros\"}},"
                                        + "{\"name\":\"dt\",\"type\":[\"null\",{\"type\":\"string\",\"logicalType\":\"datetime\"}]},"
                                        + "{\"name\":\"i1\",\"type\":[\"null\",\"long\"]}]}");

        long modTime = System.currentTimeMillis() * 1000;

        defaultRecords =
                new TableRow[]{
                        new TableRow()
                                .set("ts", 1L)
                                .set("s1", "1001")
                                .set("d1", "1970-01-01")
                                .set("t1", "00:00:00.000001")
                                .set("dt", "2020-01-01T00:42:00.123")
                                .set("i1", 2001L),
                        new TableRow()
                                .set("ts", 2L)
                                .set("s1", "1002")
                                .set("d1", "1970-01-02")
                                .set("t1", "00:00:00.000002")
                                .set("dt", "2020-01-02T00:42:00.123")
                                .set("i1", 2002L),
                        new TableRow()
                                .set("ts", 3L)
                                .set("s1", "1003")
                                .set("d1", "1970-01-03")
                                .set("t1", "00:00:00.000003")
                                .set("dt", "2020-01-03T00:42:00.123")
                                .set("i1", 2003L),
                        new TableRow()
                                .set("ts", 4L)
                                .set("s1", "1004")
                                .set("d1", "1970-01-04")
                                .set("t1", "00:00:00.000004")
                                .set("dt", "2020-01-04T00:42:00.123")
                                .set("i1", null),
                        new TableRow()
                                .set("ts", 5L)
                                .set("s1", "1005")
                                .set("d1", "1970-01-05")
                                .set("t1", "00:00:00.000005")
                                .set("dt", "2020-01-05T00:42:00.123")
                                .set("i1", 2005L)
                };

        defaultExpectedRecords =
                new String[]{
                        "{\"ts\": 1, \"s1\": \"1001\", \"d1\": 0, \"t1\": 1, \"dt\": \"2020-01-01T00:42:00.123\","
                                + " \"i1\": 2001}",
                        "{\"ts\": 2, \"s1\": \"1002\", \"d1\": 1, \"t1\": 2, \"dt\": \"2020-01-02T00:42:00.123\","
                                + " \"i1\": 2002}",
                        "{\"ts\": 3, \"s1\": \"1003\", \"d1\": 2, \"t1\": 3, \"dt\": \"2020-01-03T00:42:00.123\","
                                + " \"i1\": 2003}",
                        "{\"ts\": 4, \"s1\": \"1004\", \"d1\": 3, \"t1\": 4, \"dt\": \"2020-01-04T00:42:00.123\","
                                + " \"i1\": null}",
                        "{\"ts\": 5, \"s1\": \"1005\", \"d1\": 4, \"t1\": 5, \"dt\": \"2020-01-05T00:42:00.123\","
                                + " \"i1\": 2005}"
                };

        FakeDatasetService.setUp();
        fakeDatasetService = new FakeDatasetService();
        fakeDatasetService.createDataset(PROJECT, DATASET, "", "", null);


    }

    private static final Pattern TABLE_QUERY_PATTERN =
            Pattern.compile(
                    "select.*table_id.*last_modified_time.*partitioning_column", CASE_INSENSITIVE | DOTALL);
    private static final Pattern PARTITION_QUERY_PATTERN =
            Pattern.compile("select.*partition_id.*last_modified_time", CASE_INSENSITIVE | DOTALL);


    private String readFirstLine(File outputFile) throws FileNotFoundException {
        Scanner fileReader = new Scanner(outputFile);
        String result = fileReader.nextLine();
        fileReader.close();
        return result;
    }

    private File writeOutputFile(String folderName, String filename, String data) throws IOException {
        File outputDir = tmpDir.newFolder("out", folderName);
        outputDir.mkdirs();
        File outputFile = new File(outputDir.getAbsolutePath() + "/" + filename);
        outputFile.createNewFile();
        FileWriter writer = new FileWriter(outputFile);
        writer.write(data);
        writer.close();
        return outputFile;
    }

    private static TableId tableId(String tableName) {
        return TableId.of(PROJECT, DATASET, tableName);
    }
    private static FieldValueList fields(Object... fieldValues) {
        List<FieldValue> list = new ArrayList<>(fieldValues.length);
        for (Object fieldValue : fieldValues) {
            list.add(FieldValue.of(FieldValue.Attribute.RECORD, fieldValue));
        }
        return FieldValueList.of(list);
    }
}