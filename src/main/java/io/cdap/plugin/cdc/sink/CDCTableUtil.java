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

package io.cdap.plugin.cdc.sink;

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cdc.common.OperationType;
import io.cdap.plugin.cdc.common.Schemas;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Utility methods for dealing with Tables, for CDC use cases.
 */
public class CDCTableUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CDCTableUtil.class);

  public static final String CDC_COLUMN_FAMILY = "cdc";

  /**
   * Creates a table using the HBase Admin API.
   *
   * @param admin     the HBase Admin to use to create the table
   * @param tableName the name of the table
   */
  public static void createHBaseTable(Admin admin, String tableName) throws IOException {
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
      descriptor.addFamily(new HColumnDescriptor(CDC_COLUMN_FAMILY));
      LOG.debug("Creating HBase table {}.", tableName);
      admin.createTable(descriptor);
    }
  }

  /**
   * Updates an HBase API Table with a CDC record.
   *
   * @param table        the HBase API Table to update
   * @param dmlRecord the StructuredRecord containing the CDC data
   */
  public static void updateHBaseTable(Table table, StructuredRecord dmlRecord) throws Exception {
    OperationType operationType = OperationType.valueOf(dmlRecord.get(Schemas.OP_TYPE_FIELD));
    List<String> primaryKeys = dmlRecord.get(Schemas.PRIMARY_KEYS_FIELD);
    Schema updateSchema = Schema.parseJson((String) dmlRecord.get(Schemas.UPDATE_SCHEMA_FIELD));
    Map<String, Object> changes = dmlRecord.get(Schemas.UPDATE_VALUES_FIELD);

    switch (operationType) {
      case INSERT:
      case UPDATE:
        Put put = new Put(getRowKey(primaryKeys, changes));
        for (Schema.Field field : updateSchema.getFields()) {
          setPutField(put, field, changes.get(field.getName()));
        }
        table.put(put);
        LOG.debug("Putting row {}", Bytes.toString(getRowKey(primaryKeys, changes)));
        break;
      case DELETE:
        Delete delete = new Delete(getRowKey(primaryKeys, changes));
        table.delete(delete);
        LOG.debug("Deleting row {}", Bytes.toString(getRowKey(primaryKeys, changes)));
        break;
      default:
        LOG.warn("Operation of type '{}' will be ignored.", operationType);
    }
  }

  private static byte[] getRowKey(List<String> primaryKeys, Map<String, Object> changes) {
    // the primary keys are always in sorted order
    String joinedValue = primaryKeys.stream()
      .sorted()
      .map(primaryKey -> changes.get(primaryKey).toString())
      .collect(Collectors.joining(":"));
    return Bytes.toBytes(joinedValue);
  }

  // get the non-nullable type of the field and check that it's a simple type.
  private static Schema.Type validateAndGetType(Schema.Field field) {
    Schema.Type type;
    if (field.getSchema().isNullable()) {
      type = field.getSchema().getNonNullable().getType();
    } else {
      type = field.getSchema().getType();
    }
    Preconditions.checkArgument(type.isSimpleType(),
                                "only simple types are supported (boolean, int, long, float, double, bytes).");
    return type;
  }

  private static void setPutField(Put put, Schema.Field field, @Nullable Object val) {
    Schema.Type type = validateAndGetType(field);
    String column = field.getName();
    if (val == null) {
      put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column), null);
      return;
    }

    switch (type) {
      case BOOLEAN:
        put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column), Bytes.toBytes((Boolean) val));
        break;
      case INT:
        put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column),
                      Bytes.toBytes(((Number) val).intValue()));
        break;
      case LONG:
        put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column),
                      Bytes.toBytes(((Number) val).longValue()));
        break;
      case FLOAT:
        put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column),
                      Bytes.toBytes(((Number) val).floatValue()));
        break;
      case DOUBLE:
        put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column),
                      Bytes.toBytes(((Number) val).doubleValue()));
        break;
      case BYTES:
        if (val instanceof ByteBuffer) {
          put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column), Bytes.toBytes((ByteBuffer) val));
        } else {
          put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column), (byte[]) val);
        }
        break;
      case STRING:
        put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column), Bytes.toBytes((String) val));
        break;
      default:
        throw new IllegalArgumentException("Field " + field.getName() + " is of unsupported type " + type);
    }
  }
}
