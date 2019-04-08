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

import org.apache.commons.io.IOUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.j_spaces.core.client.SQLQuery;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.GigaSpaceTypeManager;
import org.openspaces.core.space.SpaceProxyConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.copyAttributesToOriginal;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"json", "gigasaces", "space", "insert", "update", "delete"})
@CapabilityDescription("Converts a JSON-formatted FlowFile into an UPDATE, INSERT, or DELETE action. The incoming FlowFile is expected to be "
        + "\"flat\" JSON message, meaning that it consists of a single JSON element and each field maps to a simple type. If a field maps to "
        + "a JSON object, that JSON object will be interpreted as Text. If the input is an array of JSON elements, each element in the array is "
        + "processed as aset of individual such messages.")

public class GSInjestProcessor extends AbstractProcessor {
    private static final String UPDATE_TYPE = "UPDATE";
    private static final String DELETE_TYPE = "TAKE";
    private static final String UPSERT_TYPE = "WRITE";

    private boolean ignoreUnmappedFields;
    private String statementType;
    private String updateKeys;
    private String spaceName;
    private String idName;
    private String partitionName;
    private String spaceObject;
    private String jsonConfig;
    private String dteFormat;
    private boolean includePrimaryKeys;
    private boolean idAuto;

    // Is the unmatched column behaviour fail or warning?
    private boolean failUnmappedColumns;
    private boolean warningUnmappedColumns;

    private SpaceSchema schema;
    private GigaSpace gigaSpace;
    private List keyColoumnNames = new ArrayList();

    private Long endedAt = Long.valueOf("0");
    static final AllowableValue IGNORE_UNMATCHED_FIELD = new AllowableValue("Ignore", "Ignore Unmatched Fields",
            "Any field in the JSON document that cannot be mapped to a property in the space is ignored");
    static final AllowableValue FAIL_UNMATCHED_FIELD = new AllowableValue("Fail", "Fail Unmatched Columns",
            "If the JSON document has any field that cannot be mapped to a property in the space, the FlowFile will be routed to the failure relationship");
    static final AllowableValue IGNORE_UNMATCHED_COLUMN = new AllowableValue("Ignore",
            "Ignore Unmatched Columns",
            "Any property in the space that does not have a field in the JSON document will be assumed to not be required.  No notification will be logged");
    static final AllowableValue FAIL_UNMATCHED_COLUMN = new AllowableValue("Fail",
            "Fail on Unmatched Columns",
            "A flow will fail if any column in the space that does not have a field in the JSON document.  An error will be logged");

    static final PropertyDescriptor STATEMENT_TYPE = new PropertyDescriptor.Builder()
            .name("Statement Type")
            .description("Specifies the type of process to generate")
            .required(true)
            .allowableValues(UPDATE_TYPE, DELETE_TYPE, UPSERT_TYPE)
            .build();
    static final PropertyDescriptor SPACE_NAME = new PropertyDescriptor.Builder()
            .name("Space Name")
            .description("The name of the space to connect to")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor SPACE_OBJECT = new PropertyDescriptor.Builder()
            .name("Space Object Name")
            .description("The name of the space that the statement should update")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor UNMATCHED_FIELD_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("Unmatched Field Behavior")
            .description("If an incoming JSON element has a field that does not map to any of the spaces columns, this property specifies how to handle the situation")
            .allowableValues(IGNORE_UNMATCHED_FIELD, FAIL_UNMATCHED_FIELD)
            .defaultValue(IGNORE_UNMATCHED_FIELD.getValue())
            .build();
    static final PropertyDescriptor UNMATCHED_COLUMN_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("Unmatched Column Behavior")
            .description("If an incoming JSON element does not have a field mapping for all of the spaces columns, this property specifies how to handle the situation")
            .allowableValues(IGNORE_UNMATCHED_COLUMN, FAIL_UNMATCHED_COLUMN)
            .defaultValue(IGNORE_UNMATCHED_COLUMN.getValue())
            .build();
    static final PropertyDescriptor DTE_FORMAT = new PropertyDescriptor.Builder()
            .name("Date Time Format")
            .description("java data format string")
            .defaultValue("yyyy-MM-dd HH:mm:ss.SSS")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor UPDATE_KEY = new PropertyDescriptor.Builder()
            .name("Update Keys")
            .description("A comma-separated list of column names that uniquely identifies a object in the space for UPDATE statements. "
                    + "If the  Type is UPDATE and this property is not set, the spaces's Primary Keys are used. "
                    + "In this case, if no Primary Key exists, the transaction will fail if Unmatched Column Behaviour is set to FAIL. "
                    + "This property is ignored if the Statement Type is INSERT")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();
    static final PropertyDescriptor ID_NAME = new PropertyDescriptor.Builder()
            .name("ID property name")
            .description("Name of the property that is the id of the space object")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();
    static final PropertyDescriptor ID_AUTO = new PropertyDescriptor.Builder()
            .name("ID property auto generate")
            .description("Auto generate ID")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .required(true)
            .build();
    static final PropertyDescriptor PART_NAME = new PropertyDescriptor.Builder()
            .name("Partitioning property name")
            .description("Name of the property that is the space object is partitioned on")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();
    static final PropertyDescriptor JSON_CONFIG = new PropertyDescriptor.Builder()
            .name("Json Config")
            .description("a defintion of the space describe in json e.g. { \"name\": \"String\", \"age\": 3\"Integer\", \"city\": \"String\" }")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("When a FlowFile is processed, the original JSON FlowFile is routed to this relationship")
            .build();

    static final Relationship REL_DONE = new Relationship.Builder()
            .name("done")
            .description("When a FlowFile is processed, the resulting data documents are routed to this relationship")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be processed. Common causes include invalid JSON ")
            .build();


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(STATEMENT_TYPE);
        properties.add(SPACE_NAME);
        properties.add(SPACE_OBJECT);
        properties.add(ID_NAME);
        properties.add(ID_AUTO);
        properties.add(PART_NAME);
        properties.add(JSON_CONFIG);
        properties.add(UNMATCHED_FIELD_BEHAVIOR);
        properties.add(UNMATCHED_COLUMN_BEHAVIOR);
        properties.add(UPDATE_KEY);
        properties.add(DTE_FORMAT);
        return properties;
    }


    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_ORIGINAL);
        rels.add(REL_FAILURE);
        rels.add(REL_DONE);
        return rels;
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        System.out.println("Scehduled Injest");
        ignoreUnmappedFields = IGNORE_UNMATCHED_FIELD.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_FIELD_BEHAVIOR).getValue());
        statementType = context.getProperty(STATEMENT_TYPE).getValue();
        updateKeys = context.getProperty(UPDATE_KEY).getValue();
        spaceName = context.getProperty(SPACE_NAME).getValue();
        idName = context.getProperty(ID_NAME).getValue();
        partitionName = context.getProperty(PART_NAME).getValue();
        spaceObject = context.getProperty(SPACE_OBJECT).getValue();
        jsonConfig = context.getProperty(JSON_CONFIG).getValue();
        includePrimaryKeys = UPDATE_TYPE.equals(statementType) && updateKeys == null;
        idAuto = context.getProperty(ID_AUTO).asBoolean();
        dteFormat = context.getProperty(DTE_FORMAT).getValue();
        // Is the unmatched column behaviour fail or warning?
        failUnmappedColumns = FAIL_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).getValue());

        if (updateKeys != null && updateKeys != "") {
            for (String name : updateKeys.split(",")) {
                keyColoumnNames.add(name);
            }
        }
        if (schema == null) {
            try {
//                final String dev1Url = "jini://localhost:4174/*/demo";
//                gigaSpace = new GigaSpaceConfigurer(new UrlSpaceConfigurer(dev1Url)).gigaSpace();
                gigaSpace = new GigaSpaceConfigurer(new SpaceProxyConfigurer(spaceName)).gigaSpace();
                schema = SpaceSchema.from(gigaSpace, spaceName, spaceObject,
                        idName, idAuto, partitionName, keyColoumnNames, jsonConfig);
                if (schema.getNewSpace())
                     runInit();
            } catch (InterruptedException e) {
                String failmsg = "Failed to init space " + e.toString();
                throw new ProcessException(failmsg);
            } catch (final Exception e) {
                String failmsg = "Failed to tie into a space due to " + e.toString();
                throw new ProcessException(failmsg);
            }
        }
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final Set<FlowFile> created = new HashSet<>();
        StringBuilder inputBuffer= new StringBuilder();

        session.read(flowFile, in -> inputBuffer.append(IOUtils.toString(in,UTF_8)));
        try {
            String attr = flowFile.getAttribute("kafka.offset");
            Long loaded = (attr==null) ? 0 : Long.valueOf(attr);
            if (loaded>endedAt-1) {
                System.out.println("loaded / ended "+loaded+" "+endedAt);
                String doc = triggered(inputBuffer.toString());
                FlowFile doneFlow = session.create();
                created.add(doneFlow);
                doneFlow = session.write(doneFlow, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream out) throws IOException {
                        out.write(doc.getBytes(UTF_8));
                    }
                });
                session.transfer(doneFlow, REL_DONE);
            } else {
                getLogger().info("Failed to process {} part of initial load",
                        new Object[]{inputBuffer.toString()});
            }
            session.transfer(flowFile, REL_ORIGINAL);
        } catch (final Exception pe) {
            getLogger().error("Failed to process {} routing to failure",
                    new Object[]{flowFile}, pe);
//                session.remove(created);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }


    }


    public String triggered(String input) throws Exception {
        // Parse the JSON document
        final ObjectMapper mapper = new ObjectMapper();
        final AtomicReference<JsonNode> rootNodeRef = new AtomicReference<>(null);

        rootNodeRef.set(mapper.readTree(input));

        final JsonNode rootNode = rootNodeRef.get();

        // The node may or may not be a Json Array. If it isn't, we will create an
        // ArrayNode and add just the root node to it. We do this so that we can easily iterate
        // over the array node, rather than duplicating the logic or creating another function that takes many variables
        // in order to implement the logic.
        final ArrayNode arrayNode;
        boolean isArray = false;
        if (rootNode.isArray()) {
            isArray = true;
            arrayNode = (ArrayNode) rootNode;
        } else {
            final JsonNodeFactory nodeFactory = JsonNodeFactory.instance;
            arrayNode = new ArrayNode(nodeFactory);
            arrayNode.add(rootNode);
        }

        final String fragmentIdentifier = UUID.randomUUID().toString();
        StringBuffer doc = new StringBuffer();
        if (isArray)
            doc.append("[");
        final Set<FlowFile> created = new HashSet<>();

        for (int i = 0; i < arrayNode.size(); i++) {
            final JsonNode jsonNode = arrayNode.get(i);

            final Map<String, String> attributes = new HashMap<>();


            if (UPSERT_TYPE.equals(statementType)) {
                doc.append(generateWrite(jsonNode, schema, ignoreUnmappedFields,
                        failUnmappedColumns,
                        statementType, keyColoumnNames));
            } else if (UPDATE_TYPE.equals(statementType)) {
                doc.append(generateUpdate(jsonNode, schema, ignoreUnmappedFields,
                        failUnmappedColumns,
                        statementType, keyColoumnNames));
            } else if (DELETE_TYPE.equals(statementType)) {
                doc.append(generateDelete(jsonNode, schema, ignoreUnmappedFields,
                        failUnmappedColumns,
                        statementType, keyColoumnNames));
            }
        }
        if (isArray)
            doc.append("]");
        return doc.toString();
    }

    private HashMap<String, Object> getMap(JsonNode rootNode,SpaceSchema schema)  {

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
            HashMap<String, Object> jsonProperties = new HashMap<>();
            Iterator<Map.Entry<String, JsonNode>> nodes = rootNode.fields();

            while (nodes.hasNext()) {
                Map.Entry<String, JsonNode> entry = nodes.next();
                if (schema.columns.get(entry.getKey()).dataType.equals("java.lang.Long")) {
                    LocalDateTime fromDateTime = LocalDateTime.parse(entry.getValue().textValue(),
                            formatter);
                    Long millis = fromDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
                    jsonProperties.put(entry.getKey(), millis);
                } else if (schema.columns.get(entry.getKey()).dataType.equals("java.lang.LocalDateTime")) {
                        LocalDateTime fromDateTime = LocalDateTime.parse(entry.getValue().textValue(),
                                formatter);
                        jsonProperties.put(entry.getKey(), fromDateTime);
                } else if (schema.columns.get(entry.getKey()).dataType.equals("java.lang.Integer")) {
                    int i = Integer.parseInt(entry.getValue().asText());
                    jsonProperties.put(entry.getKey(), i);
                } else {
                    jsonProperties.put(entry.getKey(), entry.getValue().asText());
                }

            }

            return jsonProperties;
        }





    private String generateWrite(final JsonNode rootNode,
                                 final SpaceSchema schema,
                                 final boolean ignoreUnmappedFields, final boolean failUnmappedColumns,
                                 final String statementType, final List<String> keyColoumnNames) throws ProcessException {

        try {

            HashMap<String, Object> jsonProperties = getMap(rootNode,schema);

            for (final String requiredColName : schema.getColumns().keySet()) {
                if (!jsonProperties.containsKey(requiredColName)) {
                    String missingColMessage = "JSON does not have a value for the Required column '" + requiredColName + "'";
                    if (failUnmappedColumns) {
                        getLogger().error(missingColMessage);
                        throw new ProcessException(missingColMessage);
                    }
                }
            }
            for (final String requiredColName : jsonProperties.keySet()) {
                if (!schema.getColumns().keySet().contains(requiredColName)) {
                    String missingColMessage = "JSON has a value for the column not is space schema'" + requiredColName + "'";
                    if (!ignoreUnmappedFields) {
                        getLogger().error(missingColMessage);
                        throw new ProcessException(missingColMessage);
                    }
                }
            }


            SpaceDocument dataAsDocument = new SpaceDocument(spaceObject, jsonProperties);

            gigaSpace.write(dataAsDocument);

//            SpaceDocument dataAsDocumentFromGrid = gigaSpace.read(new SQLQuery<SpaceDocument>(spaceObject, idName + " = ?", jsonProperties.get(idName)));


            return new ObjectMapper().writeValueAsString(dataAsDocument);
        } catch (final Exception e) {
            String failmsg = "Failed to write document " + e.toString();
            throw new ProcessException(failmsg);
        }
    }

    private String generateUpdate(final JsonNode rootNode,
                                  final SpaceSchema schema,
                                  final boolean ignoreUnmappedFields, final boolean failUnmappedColumns,
                                  final String statementType, final List<String> keyColoumnNames) throws ProcessException {

        try {
            HashMap<String, Object> jsonProperties = getMap(rootNode,schema);


            for (final String requiredColName : schema.getColumns().keySet()) {
                if (!jsonProperties.containsKey(requiredColName)) {
                    String missingColMessage = "JSON does not have a value for the Required update columns '" + requiredColName + "'";
                    if (failUnmappedColumns) {
                        getLogger().error(missingColMessage);
                        throw new ProcessException(missingColMessage);
                    }
                }
            }
            for (final String requiredColName : jsonProperties.keySet()) {
                if (!schema.getColumns().keySet().contains(requiredColName)) {
                    String missingColMessage = "JSON has a value for the column not is space schema'" + requiredColName + "'";
                    if (!ignoreUnmappedFields) {
                        getLogger().error(missingColMessage);
                        throw new ProcessException(missingColMessage);
                    }
                }
            }
            StringBuffer sqlExpressionBuffer = new StringBuffer();

            int i = 1;
            for (String keyname : keyColoumnNames) {
                if (i == 1)
                    sqlExpressionBuffer.append(keyname + " = ?");
                else
                    sqlExpressionBuffer.append(" and " + keyname + " = ?");
                i++;
            }

            SQLQuery query = new SQLQuery<SpaceDocument>(spaceObject, sqlExpressionBuffer.toString());

            i = 1;
            List<String> updateKeysSet = new ArrayList<>();
            for (String keyname : keyColoumnNames) {
                query.setParameter(i, jsonProperties.get(keyname));
                updateKeysSet.add(keyname);
                i++;
            }

            SpaceDocument dataAsDocumentFromGrid = (SpaceDocument) gigaSpace.readIfExists(query);
            if (dataAsDocumentFromGrid == null) {
                String failmsg = "Failed to find document " + query.toString();
                throw new ProcessException(failmsg);
            }

            for (final String field : jsonProperties.keySet()) {
                if (!updateKeysSet.contains(field))
                    dataAsDocumentFromGrid.setProperty(field, jsonProperties.get(field));
            }

            gigaSpace.write(dataAsDocumentFromGrid);

            Object id = dataAsDocumentFromGrid.getProperty(idName);

//            SpaceDocument dataAsDocumentProcessed = gigaSpace.read(new SQLQuery<SpaceDocument>(spaceObject, " id = ?", id));

            return new ObjectMapper().writeValueAsString(dataAsDocumentFromGrid);
        } catch (final Exception e) {
            String failmsg = "Failed to Jsonify document " + e.toString();
            throw new ProcessException(failmsg);
        }
    }

    private String generateDelete(final JsonNode rootNode,
                                  final SpaceSchema schema,
                                  final boolean ignoreUnmappedFields, final boolean failUnmappedColumns,
                                  final String statementType, final List<String> keyColoumnNames) throws ProcessException {

        try {
            HashMap<String, Object> jsonProperties = getMap(rootNode,schema);


            StringBuffer sqlExpressionBuffer = new StringBuffer();

            int i = 1;
            for (String keyname : updateKeys.split(",")) {
                if (i == 1)
                    sqlExpressionBuffer.append(keyname + " = ?");
                else
                    sqlExpressionBuffer.append(" and " + keyname + " = ?");
                i++;
            }

            SQLQuery query = new SQLQuery<SpaceDocument>(spaceObject, sqlExpressionBuffer.toString());

            i = 1;
            List<String> updateKeysSet = new ArrayList<>();
            for (String keyname : updateKeys.split(",")) {
                query.setParameter(i, jsonProperties.get(keyname));
                updateKeysSet.add(keyname);
                i++;
            }

            SpaceDocument dataAsDocumentFromGrid = (SpaceDocument) gigaSpace.take(query);

            if (dataAsDocumentFromGrid == null) {
                String failmsg = "Failed to find document" + query.toString();
                throw new ProcessException(failmsg);
            }

            return new ObjectMapper().writeValueAsString(dataAsDocumentFromGrid);
        } catch (final Exception e) {
            String failmsg = "Failed to delete document " + e.toString();
            throw new ProcessException(failmsg);
        }
    }


    private static class SpaceSchema {
        private Set<String> primaryKeyColumnNames;
        private Map<String, ColumnDescription> columns;
        private String idName;
        private String partitionName;
        private boolean newSpace;

        private SpaceSchema(final List<ColumnDescription> columnDescriptions,
                            final Set<String> primaryKeyColumnNames, String idName, String partitionName, boolean newSpace) {
            this.columns = new HashMap<>();
            this.primaryKeyColumnNames = primaryKeyColumnNames;
            this.idName = idName;
            this.partitionName = partitionName;
            this.newSpace = newSpace;

            for (final ColumnDescription desc : columnDescriptions) {
                columns.put(desc.getColumnName(), desc);
            }
        }

        public Map<String, ColumnDescription> getColumns() {
            return columns;
        }

        public String getIdName() {
            return idName;
        }

        public boolean getNewSpace() {
            return newSpace;
        }

        public String getPartitionName() {
            return partitionName;
        }

        public Set<String> getPrimaryKeyColumnNames() {
            return primaryKeyColumnNames;
        }

        public static SpaceSchema from(GigaSpace gigaSpace, String spaceName, String spaceObject,
                                       String idName, boolean idAuto, String partitionName, List<String> keyColumnNames, String jsonConfig) throws Exception {

            GigaSpaceTypeManager gigaSpaceTypeManager = gigaSpace.getTypeManager();

            SpaceTypeDescriptor typeManager = gigaSpaceTypeManager.getTypeDescriptor(spaceObject);
            boolean newSpace = false;
            if (typeManager == null) {
                createSpace(gigaSpace, spaceName, spaceObject, idName, idAuto, partitionName,
                        keyColumnNames, jsonConfig);
                typeManager = gigaSpaceTypeManager.getTypeDescriptor(spaceObject);
                newSpace = true;
                System.out.println("NEW SCHEMA");

            }

            String[] typeNames = typeManager.getPropertiesNames();
            final Set<String> primaryKeyColumns = new HashSet<>();

            idName = typeManager.getIdPropertyName();
            partitionName = typeManager.getRoutingPropertyName();

            for (String key : keyColumnNames) {
                primaryKeyColumns.add(key);
            }
            final List<ColumnDescription> cols = new ArrayList<>();
            for (int i = 0; i < typeNames.length; i++) {
                String prop = typeNames[i];
                SpacePropertyDescriptor propdesc = typeManager.getFixedProperty(prop);
                String name = propdesc.getName();
                String type = propdesc.getTypeName();
                System.out.println(name + " " + type);
                cols.add(new ColumnDescription(name, type));
            }

//            admin.close();
            return new SpaceSchema(cols, primaryKeyColumns, idName, partitionName, newSpace);
        }

        private static void createSpace(GigaSpace gigaSpace, String spaceName, String spaceObject,
                                        String idName, boolean idAuto, String partitionName, List<String> keyColumnNames, String jsonConfig)
                throws Exception {
            SpaceTypeDescriptorBuilder typeDescriptorBuilder = new SpaceTypeDescriptorBuilder(spaceObject)
                    .idProperty(idName, idAuto)
                    .routingProperty(partitionName);
            for (String prop : keyColumnNames) {
                typeDescriptorBuilder.addPropertyIndex(prop, SpaceIndexType.EQUAL);
            }

            HashMap<String, Object> jsonProperties =
                    new ObjectMapper().readValue(jsonConfig, HashMap.class);

            for (Map.Entry entry : jsonProperties.entrySet()) {
                switch (entry.getValue().toString()) {
                    case "String":
                        typeDescriptorBuilder.addFixedProperty(entry.getKey().toString(),
                                String.class);
                        break;
                    case "Integer":
                        typeDescriptorBuilder.addFixedProperty(entry.getKey().toString(),
                                Integer.class);
                        break;
                    case "DateTime":
                        typeDescriptorBuilder.addFixedProperty(entry.getKey().toString(),
                                LocalDateTime.class);
                        break;
                    case "DateTimeLong":
                        typeDescriptorBuilder.addFixedProperty(entry.getKey().toString(),
                                Long.class);
                        break;
                }
            }
            // Register type:
            SpaceTypeDescriptor typeDescriptor = typeDescriptorBuilder.create();
            gigaSpace.getTypeManager().registerTypeDescriptor(typeDescriptor);
        }

        private static class ColumnDescription {
            private final String columnName;
            private final String dataType;

            private ColumnDescription(final String columnName, final String dataType) {
                this.columnName = columnName;
                this.dataType = dataType;
            }

            public String getDataType() {
                return dataType;
            }

            public String getColumnName() {
                return columnName;
            }

        }
    }

    public void runInit() throws InterruptedException {


        String topic = spaceObject;
        String BOOTSTRAP_SERVERS =
                "localhost:9092";
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                spaceObject);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG,
                spaceObject);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());


        // Subscribe to the topic.

        ConsumeLoop kloop = new ConsumeLoop(props, topic) {
            @Override
            public void process(ConsumerRecord<Long, String> record, Long maxOffset) {
                System.out.println("Porcessing " + " " + record.offset() + " " + record.value() + " " + maxOffset );
                endedAt = maxOffset;
                try {
                    triggered(record.value());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
//        Thread t = new Thread(kloop);
//        t.start();
//        t.join();
        kloop.runInit();

    }

    public abstract class ConsumeLoop {
        private KafkaConsumer consumer;
        private final CountDownLatch shutdownLatch;
        private final Properties props;
        private final String topic;
        private final Long maxOffset;

        public ConsumeLoop(Properties props, String topic) {
            this.props = props;
            this.topic = topic;
            this.shutdownLatch = new CountDownLatch(1);
            this.consumer = new KafkaConsumer<>(props);
            this.consumer.subscribe(Collections.singletonList(topic));

            Map<String, List<PartitionInfo>> topics = consumer.listTopics();

            final Collection<TopicPartition> topicPartitions = new ArrayList<>();
            for (Object partitionInfo : this.consumer.partitionsFor(topic)) {
                topicPartitions.add(new TopicPartition(((PartitionInfo) partitionInfo).topic(),
                        ((PartitionInfo) partitionInfo).partition()));
            }
            Map<TopicPartition, Long> topicPartitionEnd = this.consumer.endOffsets(topicPartitions);
            this.maxOffset = Collections.max(topicPartitionEnd.values());

        }

        public abstract void process(ConsumerRecord<Long, String> record, Long maxOffset);

        public void runInit() {
            try {
                consumer.poll(0);
                consumer.seekToBeginning(this.consumer.assignment());
                System.out.println("Begining to consume" +maxOffset);
                while (true) {
                    ConsumerRecords<Long, String> records = consumer.poll(100);
                    System.out.println(maxOffset+ " " + records.count());
                    if (maxOffset==records.count())
                        consumer.wakeup();
                    records.forEach(record -> {
                        System.out.println(record.offset() + " " + maxOffset+ " " + record.value());
                        if (maxOffset.equals(record.offset()+1)) {
                            process(record, maxOffset);
                            System.out.println(" WAKE " + maxOffset);
                            consumer.wakeup();
                        }
                        else process(record, maxOffset);
                    });
                }
            } catch (WakeupException e) {
                // ignore, we're closing
                System.out.println("Woke up exception " + e);
            } catch (Exception e) {
                System.out.println("Unexpected error " + e);
            } finally {
                consumer.close();
                shutdownLatch.countDown();
                System.out.println("Closed");
            }
        }
    }
}



