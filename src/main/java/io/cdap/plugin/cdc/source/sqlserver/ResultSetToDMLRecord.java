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

package io.cdap.plugin.cdc.source.sqlserver;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.DBUtils;
import io.cdap.plugin.cdc.common.OperationType;
import io.cdap.plugin.cdc.common.Schemas;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A serializable class to allow invoking {@link scala.Function1} from Java. The function converts {@link ResultSet}
 * to {@link StructuredRecord} for dml records
 */
public class ResultSetToDMLRecord implements Function<ResultSet, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(ResultSetToDMLRecord.class);

  private static final int CHANGE_TABLE_COLUMNS_SIZE_WITHOUT_SQN = 3;
  private static final int CHANGE_TABLE_COLUMNS_SIZE_WITH_SQN = 2;
  private static int size = CHANGE_TABLE_COLUMNS_SIZE_WITHOUT_SQN;
  private final TableInformation tableInformation;
  private final boolean requireSeqNumber;

  ResultSetToDMLRecord(TableInformation tableInformation, boolean requireSeqNumber) {
    this.requireSeqNumber = requireSeqNumber;
    this.tableInformation = tableInformation;
    if (requireSeqNumber) {
      size = CHANGE_TABLE_COLUMNS_SIZE_WITH_SQN;
    }
  }

  @Override
  public StructuredRecord call(ResultSet row) throws Exception {
    LOG.info(row.toString());
    Schema changeSchema = getChangeSchema(row, size);
    Map<String, Object> map = getChangeData(row, changeSchema, size);
    return StructuredRecord.builder(Schemas.DML_SCHEMA)
      .set(Schemas.TABLE_FIELD, Joiner.on(".").join(tableInformation.getSchemaName(),
        tableInformation.getName()))
      .set(Schemas.PRIMARY_KEYS_FIELD, Lists.newArrayList(tableInformation.getPrimaryKeys()))
      .set(Schemas.OP_TYPE_FIELD, getChangeOperation(row).name())
      .set(Schemas.UPDATE_SCHEMA_FIELD, changeSchema.toString())
      .set(Schemas.UPDATE_VALUES_FIELD, map)
      .set(Schemas.CHANGE_TRACKING_VERSION, row.getString("CHANGE_TRACKING_VERSION"))
      .build();
  }

  private static OperationType getChangeOperation(ResultSet row) throws Exception {
    String operation = row.getString("SYS_CHANGE_OPERATION");
    switch (operation) {
      case "I":
        return OperationType.INSERT;
      case "U":
        return OperationType.UPDATE;
      case "D":
        return OperationType.DELETE;
    }
    throw new IllegalArgumentException(String.format("Unknown change operation '%s'", operation));
  }

  private static Map<String, Object> getChangeData(ResultSet resultSet, Schema changeSchema,
                                                   int size) throws Exception {
    ResultSetMetaData metadata = resultSet.getMetaData();
    Map<String, Object> changes = new HashMap<>();
    for (int i = 0; i < changeSchema.getFields().size(); i++) {

      Schema.Field field = changeSchema.getFields().get(i);
      int column = getColumnForFeild(metadata, field.getName());
      int sqlType = metadata.getColumnType(column);
      String sqlTypeName = metadata.getColumnTypeName(column);
      int sqlPrecision = metadata.getPrecision(column);
      int sqlScale = metadata.getScale(column);
      getColumnForFeild(metadata, field.getName());

      try {
        /**
         * Handling clob and blob data type ... JTDS does not support free() that throws exception from the DBUtils.
         */
        if (sqlType == 2005) {
          Clob clob = resultSet.getClob(field.getName());
          String retVal = (clob != null ? clob.getSubString(1, (int) clob.length()) : null);
          changes.put(field.getName(), "\"" + retVal.toString()  + "\"");
        } else if (sqlType == 2004) {
          Blob blob = resultSet.getBlob(field.getName());
          byte[] blobVal = (blob != null ? blob.getBytes(1L, (int) blob.length()) : null);
          changes.put(field.getName(), "\"" + blobVal.toString()  + "\"");
        } else if (sqlType == -8) {
          Object sqlValue = DBUtils.transformValue(sqlType, sqlPrecision, sqlScale, resultSet, field.getName());
          Object javaValue = transformSQLToJavaType(sqlValue);
          changes.put(field.getName(), "\"" + javaValue.toString() + "\"");
        } else {
          Object sqlValue = DBUtils.transformValue(sqlType, sqlPrecision, sqlScale, resultSet, field.getName());
          Object javaValue = transformSQLToJavaType(sqlValue);
          changes.put(field.getName(), "\"" + javaValue.toString() + "\"");
        }
      } catch (Exception e) {
        if (resultSet != null && resultSet.getObject(column) != null) {
          changes.put(field.getName(), "\"" + resultSet.getObject(column).toString() + "\"");
        } else {
          changes.put(field.getName(), null);
        }
      }
    }
    return changes;
  }

  private static int getColumnForFeild(ResultSetMetaData metadata, String columnName) throws Exception {
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      if (metadata.getColumnLabel(i).equals(columnName) || metadata.getColumnName(i).equals(columnName)) {
        return i;
      }
    }
    throw new Exception ("Can not find " + columnName);

  }
  private static Schema getChangeSchema(ResultSet resultSet, int size) throws Exception {
    List<Schema.Field> schemaFields = DBUtils.getSchemaFields(resultSet);
    // drop first three columns as they are from change tracking tables and does not represent the change data
    return Schema.recordOf(Schemas.SCHEMA_RECORD,
      schemaFields.subList(size, schemaFields.size()));
  }

  private static Object transformSQLToJavaType(Object sqlValue) {
    if (sqlValue instanceof java.sql.Date) {
      return ((java.sql.Date) sqlValue).getTime();
    } else if (sqlValue instanceof java.sql.Time) {
      return ((java.sql.Time) sqlValue).getTime();
    } else if (sqlValue instanceof java.sql.Timestamp) {
      return ((java.sql.Timestamp) sqlValue).getTime();
    } else {
      return sqlValue;
    }
  }
}
