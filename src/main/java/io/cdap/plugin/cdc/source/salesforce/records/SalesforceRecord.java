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

package io.cdap.plugin.cdc.source.salesforce.records;

import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.sobject.SObject;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cdc.common.OperationType;
import io.cdap.plugin.cdc.common.Schemas;
import io.cdap.plugin.cdc.source.salesforce.sobject.SObjectDescriptor;
import io.cdap.plugin.cdc.source.salesforce.sobject.SObjectsDescribeResult;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Converts salseforce data to cdap format
 */
public class SalesforceRecord {
  private static final String PRIMARY_FIELD_KEY = "Id";

  /**
   * Builds structured record for DDL message
   *
   * @param entityName name of entity
   * @param schema     schema for entity
   * @return structured record
   */
  public static StructuredRecord buildDDLStructuredRecord(String entityName, Schema schema) {
    return StructuredRecord.builder(Schemas.DDL_SCHEMA)
      .set(Schemas.TABLE_FIELD, entityName)
      .set(Schemas.SCHEMA_FIELD, schema.toString())
      .build();
  }

  /**
   * Builds structured record for DML message
   *
   * @param id            id of record
   * @param entityName    entity name of record
   * @param schema        schema for record
   * @param operationType type of operation
   * @param sObject       Salesforce object
   * @return structured record
   */
  public static StructuredRecord buildDMLStructuredRecord(String id, String entityName, Schema schema,
                                                          OperationType operationType, SObject sObject) {
    return StructuredRecord.builder(Schemas.DML_SCHEMA)
      .set(Schemas.TABLE_FIELD, entityName)
      .set(Schemas.PRIMARY_KEYS_FIELD, Collections.singletonList(PRIMARY_FIELD_KEY))
      .set(Schemas.OP_TYPE_FIELD, operationType.name())
      .set(Schemas.UPDATE_SCHEMA_FIELD, schema.toString())
      .set(Schemas.UPDATE_VALUES_FIELD, getChangeData(id, sObject, schema))
      .build();
  }

  /**
   * Builds schema from Salesforce object description
   *
   * @param sObjectDescriptor descriptor for Salesforce object
   * @param describeResult    JSON with change event
   * @return structured record
   */
  public static Schema getSchema(SObjectDescriptor sObjectDescriptor, SObjectsDescribeResult describeResult) {
    return Schema.recordOf(Schemas.SCHEMA_RECORD, getList(sObjectDescriptor, describeResult));
  }

  private static Map<String, Object> getChangeData(String id, SObject sObject, Schema changeSchema) {
    Optional<SObject> opSObject = Optional.ofNullable(sObject);

    if (opSObject.isPresent()) {
      Map<String, Object> changes = new HashMap<>();
      for (Schema.Field field : Objects.requireNonNull(changeSchema.getFields())) {
        changes.put(field.getName(), convertValue((String) sObject.getField(field.getName()), field));
      }
      return changes;
    } else {
      return Collections.singletonMap(PRIMARY_FIELD_KEY, id);
    }
  }

  private static Object convertValue(String value, Schema.Field field) {
    Schema fieldSchema = field.getSchema();

    if (fieldSchema.getType() == Schema.Type.NULL) {
      return null;
    }

    if (fieldSchema.isNullable()) {
      if (value == null) {
        return null;
      }
      fieldSchema = fieldSchema.getNonNullable();
    }

    Schema.Type fieldSchemaType = fieldSchema.getType();

    if (value.isEmpty() && fieldSchemaType != Schema.Type.STRING) {
      return null;
    }

    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (fieldSchema.getLogicalType() != null) {
      switch (logicalType) {
        case DATE:
          // date will be in yyyy-mm-dd format
          return Math.toIntExact(LocalDate.parse(value).toEpochDay());
        case TIMESTAMP_MILLIS:
          return Instant.parse(value).toEpochMilli();
        case TIMESTAMP_MICROS:
          return TimeUnit.MILLISECONDS.toMicros(Instant.parse(value).toEpochMilli());
        case TIME_MILLIS:
          return Math.toIntExact(TimeUnit.NANOSECONDS.toMillis(LocalTime.parse(value).toNanoOfDay()));
        case TIME_MICROS:
          return TimeUnit.NANOSECONDS.toMicros(LocalTime.parse(value).toNanoOfDay());
        default:
          throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'",
                                                            field.getName(), logicalType.getToken()));
      }
    }

    switch (fieldSchemaType) {
      case BOOLEAN:
        return Boolean.valueOf(value);
      case INT:
        return Integer.valueOf(value);
      case LONG:
        return Long.valueOf(value);
      case FLOAT:
        return Float.valueOf(value);
      case DOUBLE:
        return Double.valueOf(value);
      case BYTES:
        return Byte.valueOf(value);
      case STRING:
        return value;
    }

    throw new UnexpectedFormatException(
      String.format("Unsupported schema type: '%s' for field: '%s'. Supported types are 'boolean, int, long, float, " +
                      "double, binary and string'.", field.getSchema(), field.getName()));
  }

  private static List<Schema.Field> getList(SObjectDescriptor sObjectDescriptor,
                                            SObjectsDescribeResult describeResult) {
    List<Schema.Field> schemaFields = new ArrayList<>();

    for (SObjectDescriptor.FieldDescriptor fieldDescriptor : sObjectDescriptor.getFields()) {
      String parent = fieldDescriptor.hasParents() ? fieldDescriptor.getLastParent() : sObjectDescriptor.getName();
      Field field = describeResult.getField(parent, fieldDescriptor.getName());
      if (field == null) {
        throw new IllegalArgumentException(
          String.format("Field '%s' is absent in Salesforce describe result", fieldDescriptor.getFullName()));
      }
      Schema.Field schemaField = Schema.Field.of(fieldDescriptor.getFullName(), getCdapSchemaField(field));
      schemaFields.add(schemaField);
    }

    return schemaFields;
  }

  private static Schema getCdapSchemaField(Field field) {
    Schema fieldSchema;
    switch (field.getType()) {
      case _boolean:
        fieldSchema = Schema.of(Schema.Type.BOOLEAN);
        break;
      case _int:
        fieldSchema = Schema.of(Schema.Type.INT);
        break;
      case _long:
        fieldSchema = Schema.of(Schema.Type.LONG);
        break;
      case _double:
      case currency:
      case percent:
        fieldSchema = Schema.of(Schema.Type.DOUBLE);
        break;
      case date:
        fieldSchema = Schema.of(Schema.LogicalType.DATE);
        break;
      case datetime:
        fieldSchema = Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS);
        break;
      case time:
        fieldSchema = Schema.of(Schema.LogicalType.TIME_MILLIS);
        break;
      default:
        fieldSchema = Schema.of(Schema.Type.STRING);
    }
    return field.isNillable() ? Schema.nullableOf(fieldSchema) : fieldSchema;
  }
}
