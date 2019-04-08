/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.processors;

import com.gigaspaces.document.SpaceDocument;
import com.j_spaces.core.client.SQLQuery;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.SpaceProxyConfigurer;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;


public class GSInjestProcessorTest {

    private TestRunner testRunner;
    private TestRunner testRunner2;
    private String jsonconfig = "{ \"name\": \"String\", \"age\": \"Integer\", \"city\": \"String\" }";
    private String scrape = "[{\"lot\": \"Short-Term Parking A, B, C\", \"pct\": 73, \"airport\": \"EWR\", \"asof\": \"2019-03-06 15:59:26.123\"}, {\"lot\": \"Daily Parking P4\", \"pct\": 94, \"airport\": \"EWR\", \"asof\": \"2019-03-06 15:59:26.123\"}, {\"lot\": \"Economy Parking P6\", \"pct\": 70, \"airport\": \"EWR\", \"asof\": \"2019-03-06 15:59:26.123\"}]\n";
    private String config2="{ \"airport\": \"String\", \"pct\": \"Integer\", \"lot\": \"String\", \"asof\": \"DateTimeLong\"}";
    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(GSInjestProcessor.class);
        testRunner2 = TestRunners.newTestRunner(GSInjestProcessor.class);
        GigaSpace gigaSpace = new GigaSpaceConfigurer(new SpaceProxyConfigurer("demo")).gigaSpace();
        try {
            gigaSpace.clear(new SQLQuery<SpaceDocument>("PERSONS", "name <> ?", ""));
        } catch (final Exception e) {
            System.out.println("Not there");
        }
        try {
            gigaSpace.clear(new SQLQuery<SpaceDocument>("parking", "name <> ?", ""));
        } catch (final Exception e) {
            System.out.println("Not there");
        }
    }


    @Test
    public void testInsert() throws InitializationException, ProcessException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(GSInjestProcessor.class);
        final TestRunner runner2 = TestRunners.newTestRunner(GSInjestProcessor.class);
        final TestRunner runnertime = TestRunners.newTestRunner(GSInjestProcessor.class);

        runnertime.setProperty(GSInjestProcessor.SPACE_OBJECT, "parking");
        runnertime.setProperty(GSInjestProcessor.SPACE_NAME, "demo");
        runnertime.setProperty(GSInjestProcessor.STATEMENT_TYPE, "WRITE");
        runnertime.setProperty(GSInjestProcessor.ID_NAME, "id");
        runnertime.setProperty(GSInjestProcessor.ID_AUTO, "true");
        runnertime.setProperty(GSInjestProcessor.JSON_CONFIG, config2);
        runnertime.setProperty(GSInjestProcessor.PART_NAME, "id");
        Map<String, String> attributes= new HashMap<>();
        attributes.put("kafka.offset","29");
        runnertime.enqueue(scrape,attributes);
        runnertime.run();
        runnertime.assertTransferCount(GSInjestProcessor.REL_ORIGINAL, 1);
        runnertime.assertTransferCount(GSInjestProcessor.REL_DONE, 1);
        MockFlowFile orig2 = runnertime.getFlowFilesForRelationship(GSInjestProcessor.REL_ORIGINAL).get(0);
        MockFlowFile done2 = runnertime.getFlowFilesForRelationship(GSInjestProcessor.REL_DONE).get(0);
        orig2.assertContentEquals("[{\"lot\": \"Short-Term Parking A, B, C\", \"pct\": 73, \"airport\": \"EWR\", \"asof\": \"2019-03-06 15:59:26.123\"}, {\"lot\": \"Daily Parking P4\", \"pct\": 94, \"airport\": \"EWR\", \"asof\": \"2019-03-06 15:59:26.123\"}, {\"lot\": \"Economy Parking P6\", \"pct\": 70, \"airport\": \"EWR\", \"asof\": \"2019-03-06 15:59:26.123\"}]\n");
//        done2.assertContentEquals("[{\"typeName\":\"parking\",\"properties\":{\"lot\":\"Short-Term Parking A, B, C\",\"id\":\"A1^1553877215626^22\",\"asof\":1551887966123,\"pct\":73,\"airport\":\"EWR\"},\"transient\":false,\"version\":1}{\"typeName\":\"parking\",\"properties\":{\"lot\":\"Daily Parking P4\",\"id\":\"A2^1553877215665^12\",\"asof\":1551887966123,\"pct\":94,\"airport\":\"EWR\"},\"transient\":false,\"version\":1}{\"typeName\":\"parking\",\"properties\":{\"lot\":\"Economy Parking P6\",\"id\":\"A1^1553877215626^23\",\"asof\":1551887966123,\"pct\":70,\"airport\":\"EWR\"},\"transient\":false,\"version\":1}]");
        runner.setProperty(GSInjestProcessor.SPACE_OBJECT, "PERSONS");
        runner.setProperty(GSInjestProcessor.SPACE_NAME, "demo");
        runner.setProperty(GSInjestProcessor.STATEMENT_TYPE, "WRITE");
        runner.setProperty(GSInjestProcessor.ID_NAME, "name");
        runner.setProperty(GSInjestProcessor.ID_AUTO, "false");
        runner.setProperty(GSInjestProcessor.JSON_CONFIG, jsonconfig);
        runner.setProperty(GSInjestProcessor.PART_NAME, "age");
        runner.enqueue(Paths.get("src/test/resources/person-1.json"));
        runner.run();
        runner.assertTransferCount(GSInjestProcessor.REL_ORIGINAL, 1);
       
        runner.assertTransferCount(GSInjestProcessor.REL_DONE, 1);
        MockFlowFile orig = runner.getFlowFilesForRelationship(GSInjestProcessor.REL_ORIGINAL).get(0);
        System.out.println(orig);
        MockFlowFile done = runner.getFlowFilesForRelationship(GSInjestProcessor.REL_DONE).get(0);
        done.assertContentEquals("{\"typeName\":\"PERSONS\",\"properties\":{\"city\":\"nyc\",\"age\":21,\"name\":\"Wright\"},\"transient\":false,\"version\":1}");
        orig.assertContentEquals("{ \"name\": \"Wright\", \"age\": \"21\", \"city\": \"nyc\" }");

        runner.setProperty(GSInjestProcessor.STATEMENT_TYPE, "UPDATE");
        runner.setProperty(GSInjestProcessor.UPDATE_KEY, "name");
        runner.enqueue(Paths.get("src/test/resources/person-2.json"));
        runner.run();
        runner.assertTransferCount(GSInjestProcessor.REL_ORIGINAL, 2);
       
        runner.assertTransferCount(GSInjestProcessor.REL_DONE, 2);
        orig = runner.getFlowFilesForRelationship(GSInjestProcessor.REL_ORIGINAL).get(1);
        System.out.println(orig);
        done = runner.getFlowFilesForRelationship(GSInjestProcessor.REL_DONE).get(1);
        done.assertContentEquals("{\"typeName\":\"PERSONS\",\"properties\":{\"city\":\"miami\",\"age\":81,\"name\":\"Wright\"},\"transient\":false,\"version\":2}");
        orig.assertContentEquals("{ \"name\": \"Wright\", \"age\": \"81\", \"city\": \"miami\" }");


        runner2.setProperty(GSInjestProcessor.STATEMENT_TYPE, "UPDATE");
        runner2.setProperty(GSInjestProcessor.UPDATE_KEY, "age,city");
        runner2.setProperty(GSInjestProcessor.SPACE_OBJECT, "PERSONS");
        runner2.setProperty(GSInjestProcessor.SPACE_NAME, "demo");
        runner2.setProperty(GSInjestProcessor.ID_NAME, "name");
        runner2.setProperty(GSInjestProcessor.JSON_CONFIG, jsonconfig);
        runner2.enqueue(Paths.get("src/test/resources/person-3.json"));
        runner2.run();
        runner2.assertTransferCount(GSInjestProcessor.REL_ORIGINAL, 0);
        runner2.assertTransferCount(GSInjestProcessor.REL_DONE, 0);
        runner2.assertTransferCount(GSInjestProcessor.REL_FAILURE, 1);
        done = runner2.getFlowFilesForRelationship(GSInjestProcessor.REL_FAILURE).get(0);
        done.assertContentEquals("{ \"name\": \"Smith\", \"age\": \"2\", \"city\": \"NewPort\" }");

        runner2.setProperty(GSInjestProcessor.STATEMENT_TYPE, "WRITE");
        runner2.enqueue(Paths.get("src/test/resources/person-3.json"));
        runner2.run();
        runner2.assertTransferCount(GSInjestProcessor.REL_ORIGINAL, 1);
        runner2.assertTransferCount(GSInjestProcessor.REL_DONE, 1);
        runner2.assertTransferCount(GSInjestProcessor.REL_FAILURE, 1);
        orig = runner2.getFlowFilesForRelationship(GSInjestProcessor.REL_ORIGINAL).get(0);
        System.out.println(orig);
        done = runner2.getFlowFilesForRelationship(GSInjestProcessor.REL_DONE).get(0);
        orig.assertContentEquals("{ \"name\": \"Smith\", \"age\": \"2\", \"city\": \"NewPort\" }");
        done.assertContentEquals("{\"typeName\":\"PERSONS\",\"properties\":{\"city\":\"NewPort\",\"age\":2,\"name\":\"Smith\"},\"transient\":false,\"version\":1}");

        runner2.setProperty(GSInjestProcessor.STATEMENT_TYPE, "UPDATE");
        runner2.setProperty(GSInjestProcessor.UPDATE_KEY, "age,city");
        runner2.enqueue(Paths.get("src/test/resources/person-3.json"));
        runner2.run();
        runner2.assertTransferCount(GSInjestProcessor.REL_ORIGINAL, 2);
        runner2.assertTransferCount(GSInjestProcessor.REL_DONE, 2);
        orig = runner2.getFlowFilesForRelationship(GSInjestProcessor.REL_ORIGINAL).get(1);
        done = runner2.getFlowFilesForRelationship(GSInjestProcessor.REL_DONE).get(1);
        orig.assertContentEquals("{ \"name\": \"Smith\", \"age\": \"2\", \"city\": \"NewPort\" }");
        done.assertContentEquals("{\"typeName\":\"PERSONS\",\"properties\":{\"city\":\"NewPort\",\"age\":2,\"name\":\"Smith\"},\"transient\":false,\"version\":2}");

        runner2.setProperty(GSInjestProcessor.STATEMENT_TYPE, "UPDATE");
        runner2.setProperty(GSInjestProcessor.UPDATE_KEY, "age,city");
        runner2.enqueue(Paths.get("src/test/resources/person-3.json"));
        runner2.run();
        runner2.assertTransferCount(GSInjestProcessor.REL_ORIGINAL, 3);
        runner2.assertTransferCount(GSInjestProcessor.REL_DONE, 3);
        orig = runner2.getFlowFilesForRelationship(GSInjestProcessor.REL_ORIGINAL).get(2);
        done = runner2.getFlowFilesForRelationship(GSInjestProcessor.REL_DONE).get(2);
        orig.assertContentEquals("{ \"name\": \"Smith\", \"age\": \"2\", \"city\": \"NewPort\" }");
        done.assertContentEquals("{\"typeName\":\"PERSONS\",\"properties\":{\"city\":\"NewPort\",\"age\":2,\"name\":\"Smith\"},\"transient\":false,\"version\":3}");



        runner2.setProperty(GSInjestProcessor.STATEMENT_TYPE, "TAKE");
        runner2.setProperty(GSInjestProcessor.UPDATE_KEY, "age,city");
        runner2.enqueue(Paths.get("src/test/resources/person-3.json"));
        runner2.run();
        runner2.assertTransferCount(GSInjestProcessor.REL_ORIGINAL, 4);
        runner2.assertTransferCount(GSInjestProcessor.REL_DONE, 4);
        orig = runner2.getFlowFilesForRelationship(GSInjestProcessor.REL_ORIGINAL).get(3);
        done = runner2.getFlowFilesForRelationship(GSInjestProcessor.REL_DONE).get(3);
        orig.assertContentEquals("{ \"name\": \"Smith\", \"age\": \"2\", \"city\": \"NewPort\" }");
        done.assertContentEquals("{\"typeName\":\"PERSONS\",\"properties\":{\"city\":\"NewPort\",\"age\":2,\"name\":\"Smith\"},\"transient\":false,\"version\":3}");



        runner2.setProperty(GSInjestProcessor.STATEMENT_TYPE, "TAKE");
        runner2.setProperty(GSInjestProcessor.UPDATE_KEY, "age,city");
        runner2.enqueue(Paths.get("src/test/resources/person-3.json"));
        runner2.run();
        runner2.assertTransferCount(GSInjestProcessor.REL_ORIGINAL, 4);
        runner2.assertTransferCount(GSInjestProcessor.REL_DONE, 4);
        runner2.assertTransferCount(GSInjestProcessor.REL_FAILURE, 2);
        done = runner2.getFlowFilesForRelationship(GSInjestProcessor.REL_FAILURE).get(1);
        done.assertContentEquals("{ \"name\": \"Smith\", \"age\": \"2\", \"city\": \"NewPort\" }");



    }

}
