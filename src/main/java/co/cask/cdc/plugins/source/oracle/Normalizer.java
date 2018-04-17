/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdc.plugins.source.oracle;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.cdc.plugins.common.AvroConverter;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Class responsible for normalizing the StructuredRecords to be sent to the CDC sinks
 */
public class Normalizer {
  private static Logger LOG = LoggerFactory.getLogger(Normalizer.class);
  private static Gson GSON = new Gson();
  private static final Schema DDL_SCHEMA = Schema.recordOf("DDLRecord",
                                                           Schema.Field.of("table", Schema.of(Schema.Type.STRING)),
                                                           Schema.Field.of("schema", Schema.of(Schema.Type.STRING)));

  private static final String INPUT_FIELD = "message";

  /**
   * Normalize the input StructuredRecord containing byte array into the DDL or DML records.
   * One input record can result into multiple output records. For example in case of primary key
   * updates, the output record consist of two StructuredRcords, one represents delete and another represnents
   * insert.
   * @param input record containing message as byte array to be normalized
   * @return {@link List} of normalized records
   */
  public List<StructuredRecord> transform(StructuredRecord input) throws Exception {
    Object message = input.get(INPUT_FIELD);
    if (message == null) {
      throw new IllegalStateException(String.format("Input record does not contain the field '%s'.", INPUT_FIELD));
    }

    if (input.getSchema().getRecordName().equals("GenericWrapperSchema")) {
      // Do nothing for the generic wrapper schema message
      // Return empty list
      return new ArrayList<>();
    }

    byte[] messageBytes;
    if (message instanceof ByteBuffer) {
      ByteBuffer bb = (ByteBuffer) message;
      messageBytes = new byte[bb.remaining()];
      bb.mark();
      bb.get(messageBytes);
      bb.reset();
    } else {
      messageBytes = (byte[]) message;
    }

    String messageBody = new String(messageBytes, StandardCharsets.UTF_8);
    if (input.getSchema().getRecordName().equals("DDLRecord")) {
      JsonObject schemaObj = GSON.fromJson(messageBody, JsonObject.class);
      String namespaceName = schemaObj.get("namespace").getAsString();
      String tableName = schemaObj.get("name").getAsString();
      tableName = namespaceName + "." + tableName;
      StructuredRecord.Builder builder = StructuredRecord.builder(DDL_SCHEMA);
      builder.set("table", tableName);
      builder.set("schema", getNormalizedDDLSchema(messageBody));
      return Arrays.asList(builder.build());
    }

    // Current message is the Wrapped Avro binary message
    // Get the state map
    StructuredRecord stateRecord = input.get("staterecord");
    Map<Long, String> schemaCacheMap = stateRecord.get("data");
    org.apache.avro.Schema avroGenericWrapperSchema = getGenericWrapperMessageSchema();

    GenericRecord genericRecord = getRecord(messageBytes, avroGenericWrapperSchema);
    String tableName = genericRecord.get("table_name").toString();
    long schameHashId = (Long) genericRecord.get("schema_fingerprint");

    byte[] payload = genericRecord.get("payload") instanceof ByteBuffer
      ? Bytes.toBytes((ByteBuffer) genericRecord.get("payload"))
      : (byte[]) genericRecord.get("payload");

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

    LOG.debug("Schema for DDL {}", Schema.recordOf("columns", columnFields).toString());
    return Schema.recordOf("columns", columnFields).toString();
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
    LOG.info("XXX Record before normalizing is {}", StructuredRecordStringConverter.toJsonString(record));
    List<StructuredRecord> normalizedRecords = new ArrayList<>();
    // This table name contains "." in it already
    String tableName = record.get("table");
    List<String> primaryKeys = record.get("primary_keys");
    String opType = record.get("op_type");
    Map<Schema.Field, Object> suppliedFieldValues = new HashMap<>();
    switch(opType) {
      case "I":
        StructuredRecord insertRecord = record.get("after");
        for (co.cask.cdap.api.data.schema.Schema.Field field : insertRecord.getSchema().getFields()) {
          if (!field.getName().endsWith("_isMissing")) {
            suppliedFieldValues.put(field, insertRecord.get(field.getName()));
          }
        }
        break;
      case "U":
        StructuredRecord afterUpdateRecord = record.get("after");
        StructuredRecord beforeUpdateRecord = record.get("before");
        boolean pkChanged = primaryKeyChanged(primaryKeys, beforeUpdateRecord, afterUpdateRecord);

        if (pkChanged) {
          // We need to emit two records
          // One for DELETE and one for INSERT
          suppliedFieldValues = addDeleteFields(record);
          normalizedRecords.add(createDMLRecord(tableName, "D", primaryKeys, suppliedFieldValues));
        }

        suppliedFieldValues.clear();
        for (co.cask.cdap.api.data.schema.Schema.Field field : afterUpdateRecord.getSchema().getFields()) {
          if (!field.getName().endsWith("_isMissing")) {
            String fieldName = field.getName();
            if (!((boolean) afterUpdateRecord.get(fieldName + "_isMissing"))) {
              LOG.info("XXX Adding after field {}, {}", field.getName(), afterUpdateRecord.get(field.getName()));
              suppliedFieldValues.put(field, afterUpdateRecord.get(field.getName()));
            } else {
              // Field is not updated, use the field value from the before record
              LOG.info("XXX Adding before field {}, {}", field.getName(), beforeUpdateRecord.get(field.getName()));
              suppliedFieldValues.put(field, beforeUpdateRecord.get(field.getName()));
            }
          }
        }
        if (pkChanged) {
          // Change the operation type to Insert if the primary key is changed
          opType = "I";
        }
        break;
      case "D":
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
    Map<Schema.Field, Object> fieldValues = new HashMap<>();
    StructuredRecord deleteRecord = record.get("before");
    for (co.cask.cdap.api.data.schema.Schema.Field field : deleteRecord.getSchema().getFields()) {
      if (!field.getName().endsWith("_isMissing")) {
        fieldValues.put(field, deleteRecord.get(field.getName()));
      }
    }
    return fieldValues;
  }

  private StructuredRecord createDMLRecord(String tableName, String opType, List<String> primaryKeys,
                                           Map<Schema.Field, Object> changedFields) {
    Schema changeSchema = Schema.recordOf("change", changedFields.keySet());
    StructuredRecord.Builder changeBuilder = StructuredRecord.builder(changeSchema);
    for(Map.Entry<Schema.Field, Object> entry : changedFields.entrySet()) {
      changeBuilder.set(entry.getKey().getName(), entry.getValue());
    }

    Schema dmlSchema = Schema.recordOf("DMLRecord", Schema.Field.of("table", Schema.of(Schema.Type.STRING)),
                                       Schema.Field.of("op_type", Schema.of(Schema.Type.STRING)),
                                       Schema.Field.of("primary_keys", Schema.arrayOf(Schema.of(Schema.Type.STRING))),
                                       Schema.Field.of("change", changeSchema));

    StructuredRecord.Builder builder = StructuredRecord.builder(dmlSchema);
    builder.set("table", tableName);
    builder.set("op_type", opType);
    builder.set("primary_keys", primaryKeys);
    builder.set("change", changeBuilder.build());
    return builder.build();
  }
}
