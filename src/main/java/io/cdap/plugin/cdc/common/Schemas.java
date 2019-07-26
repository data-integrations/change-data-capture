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

package io.cdap.plugin.cdc.common;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.Field;
import io.cdap.cdap.api.data.schema.Schema.Type;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Helper class with common cdc schemes definitions.
 */
public class Schemas {

  private static final Schema SIMPLE_TYPES = Schema.unionOf(Arrays.stream(Type.values())
                                                              .filter(Type::isSimpleType)
                                                              .map(Schema::of)
                                                              .collect(Collectors.toList()));

  public static final String SCHEMA_RECORD = "schema";
  public static final String TABLE_FIELD = "table";
  public static final String SCHEMA_FIELD = "schema";
  public static final String OP_TYPE_FIELD = "op_type";
  public static final String PRIMARY_KEYS_FIELD = "primary_keys";
  public static final String DDL_FIELD = "ddl";
  public static final String DML_FIELD = "dml";
  public static final String UPDATE_SCHEMA_FIELD = "rows_schema";
  public static final String UPDATE_VALUES_FIELD = "rows_values";
  public static final String CHANGE_TRACKING_VERSION = "change_tracking_version";
  public static final String CDC_CURRENT_TIMESTAMP = "cdc_current_timestamp";

  public static final Schema DDL_SCHEMA = Schema.recordOf(
    "DDLRecord",
    Field.of(TABLE_FIELD, Schema.of(Type.STRING)),
    Field.of(SCHEMA_FIELD, Schema.of(Type.STRING))
  );

  public static final Schema DML_SCHEMA = Schema.recordOf(
    "DMLRecord",
    Field.of(OP_TYPE_FIELD, enumWith(OperationType.class)),
    Field.of(TABLE_FIELD, Schema.of(Type.STRING)),
    Field.of(PRIMARY_KEYS_FIELD, Schema.arrayOf(Schema.of(Type.STRING))),
    Field.of(UPDATE_SCHEMA_FIELD, Schema.of(Type.STRING)),
    Field.of(UPDATE_VALUES_FIELD, Schema.mapOf(Schema.of(Type.STRING), SIMPLE_TYPES)),
    Field.of(CHANGE_TRACKING_VERSION, Schema.of(Type.STRING)),
    Field.of(CDC_CURRENT_TIMESTAMP, Schema.of(Schema.LogicalType.TIME_MICROS))
  );

  public static final Schema CHANGE_SCHEMA = Schema.recordOf(
    "changeRecord",
    Field.of(DDL_FIELD, Schema.nullableOf(DDL_SCHEMA)),
    Field.of(DML_FIELD, Schema.nullableOf(DML_SCHEMA))
  );

  public static StructuredRecord toCDCRecord(StructuredRecord changeRecord) {
    String recordName = changeRecord.getSchema().getRecordName();
    if (Objects.equals(recordName, DDL_SCHEMA.getRecordName())) {
      return StructuredRecord.builder(CHANGE_SCHEMA)
        .set(DDL_FIELD, changeRecord)
        .build();
    } else if (Objects.equals(recordName, DML_SCHEMA.getRecordName())) {
      return StructuredRecord.builder(CHANGE_SCHEMA)
        .set(DML_FIELD, changeRecord)
        .build();
    }
    throw new IllegalArgumentException(String.format("Wrong schema name '%s' for record", recordName));
  }

  public static String getTableName(String namespacedTableName) {
    return namespacedTableName.split("\\.")[1];
  }

  private static Schema enumWith(Class<? extends Enum<?>> enumClass) {
    // this method may be removed when Schema.enumWith() method signature fixed
    Enum<?>[] enumConstants = enumClass.getEnumConstants();
    String[] names = new String[enumConstants.length];
    for (int i = 0; i < enumConstants.length; i++) {
      names[i] = enumConstants[i].name();
    }
    return Schema.enumWith(names);
  }

  private Schemas() {
    // utility class
  }
}
