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

package io.cdap.plugin.cdc.source;

import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Utility methods for Database plugins shared by Database plugins.
 */
public final class DBUtils {


  /**
   * Given the result set, get the metadata of the result set and return
   * list of {@link io.cdap.cdap.api.data.schema.Schema.Field}.
   *
   * @param resultSet result set of executed query
   * @return list of schema fields
   * @throws SQLException
   */
  public static List<Schema.Field> getSchemaFields(ResultSet resultSet) throws SQLException {
    List<Schema.Field> schemaFields = Lists.newArrayList();
    ResultSetMetaData metadata = resultSet.getMetaData();
    // ResultSetMetadata columns are numbered starting with 1
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      String columnName = metadata.getColumnName(i);
      int columnSqlType = metadata.getColumnType(i);
      int columnSqlPrecision = metadata.getPrecision(i); // total number of digits
      int columnSqlScale = metadata.getScale(i); // digits after the decimal point
      String columnTypeName = metadata.getColumnTypeName(i);
      Schema columnSchema = getSchema(columnTypeName, columnSqlType, columnSqlPrecision, columnSqlScale);
      if (ResultSetMetaData.columnNullable == metadata.isNullable(i)) {
        columnSchema = Schema.nullableOf(columnSchema);
      }
      Schema.Field field = Schema.Field.of(columnName, columnSchema);
      schemaFields.add(field);
    }
    return schemaFields;
  }

  // given a sql type return schema type
  private static Schema getSchema(String typeName, int sqlType, int precision, int scale) throws SQLException {
    // Type.STRING covers sql types - VARCHAR,CHAR,CLOB,LONGNVARCHAR,LONGVARCHAR,NCHAR,NCLOB,NVARCHAR
    Schema.Type type = Schema.Type.STRING;
    switch (sqlType) {
      case Types.NULL:
        type = Schema.Type.NULL;
        break;

      case Types.ROWID:
        break;

      case Types.BOOLEAN:
      case Types.BIT:
        type = Schema.Type.BOOLEAN;
        break;

      case Types.TINYINT:
      case Types.SMALLINT:
        type = Schema.Type.INT;
        break;
      case Types.INTEGER:
        // CDAP-12211 - handling unsigned integers in mysql
        type = "int unsigned".equalsIgnoreCase(typeName) ? Schema.Type.LONG : Schema.Type.INT;
        break;

      case Types.BIGINT:
        type = Schema.Type.LONG;
        break;

      case Types.REAL:
      case Types.FLOAT:
        type = Schema.Type.FLOAT;
        break;

      case Types.NUMERIC:
      case Types.DECIMAL:
        // if there are no digits after the point, use integer types
        type = scale != 0 ? Schema.Type.DOUBLE :
          // with 10 digits we can represent 2^32 and LONG is required
          precision > 9 ? Schema.Type.LONG : Schema.Type.INT;
        break;

      case Types.DOUBLE:
        type = Schema.Type.DOUBLE;
        break;

      case Types.DATE:
        return Schema.of(Schema.LogicalType.DATE);
      case Types.TIME:
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      case Types.TIMESTAMP:
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);

      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
        type = Schema.Type.BYTES;
        break;

      case Types.ARRAY:
      case Types.DATALINK:
      case Types.DISTINCT:
      case Types.JAVA_OBJECT:
      case Types.OTHER:
      case Types.REF:
      case Types.SQLXML:
      case Types.STRUCT:
        throw new SQLException(new UnsupportedTypeException("Unsupported SQL Type: " + sqlType));
    }

    return Schema.of(type);
  }

  @Nullable
  public static Object transformValue(int sqlType, int precision, int scale,
                                      ResultSet resultSet, String fieldName) throws SQLException {
    Object original = resultSet.getObject(fieldName);
    if (original != null) {
      switch (sqlType) {
        case Types.SMALLINT:
        case Types.TINYINT:
          return ((Number) original).intValue();
        case Types.NUMERIC:
        case Types.DECIMAL:
          BigDecimal decimal = (BigDecimal) original;
          if (scale != 0) {
            // if there are digits after the point, use double types
            return decimal.doubleValue();
          } else if (precision > 9) {
            // with 10 digits we can represent 2^32 and LONG is required
            return decimal.longValue();
          } else {
            return decimal.intValue();
          }
        case Types.DATE:
          return resultSet.getDate(fieldName);
        case Types.TIME:
          return resultSet.getTime(fieldName);
        case Types.TIMESTAMP:
          return resultSet.getTimestamp(fieldName);
        case Types.ROWID:
          return resultSet.getString(fieldName);
        case Types.BLOB:
          Blob blob = (Blob) original;
          return blob.getBytes(1, (int) blob.length());
        case Types.CLOB:
          Clob clob = (Clob) original;
          return clob.getSubString(1, (int) clob.length());
      }
    }
    return original;
  }

  private DBUtils() {
    throw new AssertionError("Should not instantiate static utility class.");
  }
}

