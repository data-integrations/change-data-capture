/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.cdc.source.oracle;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cdc.common.AvroConverter;
import io.cdap.plugin.cdc.common.OperationType;
import io.cdap.plugin.cdc.common.Schemas;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Class responsible for normalizing the StructuredRecords to be sent to the CDC sinks
 */
public class Normalizer {
  private static final Logger LOG = LoggerFactory.getLogger(Normalizer.class);
  private static final Gson GSON = new Gson();

  private static final String INPUT_FIELD = "message";

  /**
   * Normalize the input StructuredRecord containing byte array into the DDL or DML records.
   * One input record can result into multiple output records. For example in case of primary key
   * updates, the output record consist of two StructuredRcords, one represents delete and another represnents
   * insert.
   *
   * @param input record containing message as byte array to be normalized
   * @return {@link List} of normalized records
   */
  public List<StructuredRecord> transform(StructuredRecord input) throws Exception {
    Object message = input.get(INPUT_FIELD);
    if (message == null) {
      throw new IllegalStateException(String.format("Input record does not contain the field '%s'.", INPUT_FIELD));
    }

    if ("GenericWrapperSchema".equals(input.getSchema().getRecordName())) {
      // Do nothing for the generic wrapper schema message
      // Return empty list
      return Collections.emptyList();
    }

    byte[] messageBytes = BinaryMessages.getBytesFromBinaryMessage(message);

    if (input.getSchema().getRecordName().equals(Schemas.DDL_SCHEMA.getRecordName())) {
      String messageBody = new String(messageBytes, StandardCharsets.UTF_8);
      JsonObject schemaObj = GSON.fromJson(messageBody, JsonObject.class);
      String namespaceName = schemaObj.get("namespace").getAsString();
      String tableName = schemaObj.get("name").getAsString();
      StructuredRecord ddlRecord = StructuredRecord.builder(Schemas.DDL_SCHEMA)
        .set(Schemas.TABLE_FIELD, namespaceName + "." + tableName)
        .set(Schemas.SCHEMA_FIELD, getNormalizedDDLSchema(messageBody))
        .build();
      return Collections.singletonList(ddlRecord);
    }

    // Current message is the Wrapped Avro binary message
    // Get the state map
    StructuredRecord stateRecord = input.get("staterecord");
    Map<Long, String> schemaCacheMap = stateRecord.get("data");
    org.apache.avro.Schema avroGenericWrapperSchema = getGenericWrapperMessageSchema();

    GenericRecord genericRecord = getRecord(messageBytes, avroGenericWrapperSchema);
    String tableName = genericRecord.get("table_name").toString();
    long schameHashId = (Long) genericRecord.get("schema_fingerprint");

    byte[] payload = BinaryMessages.getBytesFromBinaryMessage(genericRecord.get("payload"));

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schemaCacheMap.get(schameHashId));
    LOG.debug("Avro schema {} for table {} with fingerprint {}", avroSchema, tableName, schameHashId);

    StructuredRecord structuredRecord = AvroConverter.fromAvroRecord(getRecord(payload, avroSchema),
                                                                     AvroConverter.fromAvroSchema(avroSchema));

    return getNormalizedDMLRecord(structuredRecord);
  }

  private String getNormalizedDDLSchema(String jsonSchema) {
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(jsonSchema);
    Schema schema = AvroConverter.fromAvroSchema(avroSchema);
    Schema column = schema.getField("before").getSchema().getNonNullable();

    List<Schema.Field> columnFields = new ArrayList<>();
    for (Schema.Field field : column.getFields()) {
      if (!field.getName().endsWith("_isMissing")) {
        columnFields.add(field);
      }
    }

    Schema ddlSchema = Schema.recordOf(Schemas.SCHEMA_RECORD, columnFields);
    LOG.debug("Schema for DDL {}", ddlSchema);
    return ddlSchema.toString();
  }

  private org.apache.avro.Schema getGenericWrapperMessageSchema() {
    String avroGenericWrapperSchema = "{\n" +
      "          \"type\" : \"record\",\n" +
      "          \"name\" : \"generic_wrapper\",\n" +
      "          \"namespace\" : \"oracle.goldengate\",\n" +
      "          \"fields\" : [ {\n" +
      "            \"name\" : \"table_name\",\n" +
      "            \"type\" : \"string\"\n" +
      "          }, {\n" +
      "            \"name\" : \"schema_fingerprint\",\n" +
      "            \"type\" : \"long\"\n" +
      "          }, {\n" +
      "            \"name\" : \"payload\",\n" +
      "            \"type\" : \"bytes\"\n" +
      "          } ]\n" +
      "        }";
    return new org.apache.avro.Schema.Parser().parse(avroGenericWrapperSchema);
  }

  private GenericRecord getRecord(byte[] message, org.apache.avro.Schema schema) throws IOException {
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
    return datumReader.read(null, DecoderFactory.get().binaryDecoder(message, null));
  }

  private List<StructuredRecord> getNormalizedDMLRecord(StructuredRecord record) throws IOException {
    List<StructuredRecord> normalizedRecords = new ArrayList<>();
    // This table name contains "." in it already
    String tableName = record.get("table");
    List<String> primaryKeys = record.get("primary_keys");
    OperationType opType = OperationType.fromShortName(record.get("op_type"));
    Map<Schema.Field, Object> suppliedFieldValues = new LinkedHashMap<>();
    switch (opType) {
      case INSERT:
        StructuredRecord insertRecord = record.get("after");
        for (Schema.Field field : insertRecord.getSchema().getFields()) {
          if (!field.getName().endsWith("_isMissing")) {
            suppliedFieldValues.put(field, insertRecord.get(field.getName()));
          }
        }
        break;
      case UPDATE:
        StructuredRecord afterUpdateRecord = record.get("after");
        StructuredRecord beforeUpdateRecord = record.get("before");
        boolean pkChanged = primaryKeyChanged(primaryKeys, beforeUpdateRecord, afterUpdateRecord);

        if (pkChanged) {
          // We need to emit two records
          // One for DELETE and one for INSERT
          suppliedFieldValues = addDeleteFields(record);
          normalizedRecords.add(createDMLRecord(tableName, OperationType.DELETE, primaryKeys, suppliedFieldValues));
        }

        suppliedFieldValues.clear();
        for (Schema.Field field : afterUpdateRecord.getSchema().getFields()) {
          if (!field.getName().endsWith("_isMissing")) {
            String fieldName = field.getName();
            if (!((boolean) afterUpdateRecord.get(fieldName + "_isMissing"))) {
              suppliedFieldValues.put(field, afterUpdateRecord.get(field.getName()));
            } else {
              // Field is not updated, use the field value from the before record
              suppliedFieldValues.put(field, beforeUpdateRecord.get(field.getName()));
            }
          }
        }
        if (pkChanged) {
          // Change the operation type to Insert if the primary key is changed
          opType = OperationType.INSERT;
        }
        break;
      case DELETE:
        suppliedFieldValues = addDeleteFields(record);
        break;
      default:
        break;
    }

    normalizedRecords.add(createDMLRecord(tableName, opType, primaryKeys, suppliedFieldValues));
    return normalizedRecords;
  }

  private boolean primaryKeyChanged(List<String> primaryKeys, StructuredRecord before, StructuredRecord after) {
    for (String key : primaryKeys) {
      if (!Objects.equals(before.get(key), after.get(key))) {
        return true;
      }
    }
    return false;
  }

  private Map<Schema.Field, Object> addDeleteFields(StructuredRecord record) {
    Map<Schema.Field, Object> fieldValues = new LinkedHashMap<>();
    StructuredRecord deleteRecord = record.get("before");
    for (Schema.Field field : deleteRecord.getSchema().getFields()) {
      if (!field.getName().endsWith("_isMissing")) {
        fieldValues.put(field, deleteRecord.get(field.getName()));
      }
    }
    return fieldValues;
  }

  private StructuredRecord createDMLRecord(String tableName, OperationType opType, List<String> primaryKeys,
                                           Map<Schema.Field, Object> changedFields) throws IOException {
    Schema changeSchema = Schema.recordOf(Schemas.SCHEMA_RECORD, changedFields.keySet());
    Map<String, Object> changes = new LinkedHashMap<>();
    for (Map.Entry<Schema.Field, Object> entry : changedFields.entrySet()) {
      changes.put(entry.getKey().getName(), entry.getValue());
    }
    return StructuredRecord.builder(Schemas.DML_SCHEMA)
      .set(Schemas.TABLE_FIELD, tableName)
      .set(Schemas.OP_TYPE_FIELD, opType.name())
      .set(Schemas.PRIMARY_KEYS_FIELD, primaryKeys)
      .set(Schemas.UPDATE_SCHEMA_FIELD, changeSchema.toString())
      .set(Schemas.UPDATE_VALUES_FIELD, changes)
      .build();
  }
}
