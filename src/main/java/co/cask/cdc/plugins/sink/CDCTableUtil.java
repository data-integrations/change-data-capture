/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdc.plugins.sink;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for dealing with Tables, for CDC use cases.
 */
public class CDCTableUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CDCTableUtil.class);

  private static final String CDC_COLUMN_FAMILY = "cdc";

  /**
   * Creates a table using the HBase Admin API.
   *
   * @param admin the HBase Admin to use to create the table
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
   * @param table the HBase API Table to update
   * @param input the StructuredRecord containing the CDC data
   */
  public static void updateHBaseTable(Table table, StructuredRecord input) throws Exception {
    String operationType = input.get("op_type");
    List<String> primaryKeys = input.get("primary_keys");
    StructuredRecord change = input.get("change");
    List<Schema.Field> fields = change.getSchema().getFields();

    switch (operationType) {
      case "I":
      case "U":
        Put put = new Put(getRowKey(primaryKeys, change));
        for (Schema.Field field : fields) {
          setPutField(put, field, change.get(field.getName()));
        }
        table.put(put);
        LOG.debug("Putting row {}", Bytes.toString(getRowKey(primaryKeys, change)));
        break;
      case "D":
        Delete delete = new Delete(getRowKey(primaryKeys, change));
        table.delete(delete);
        LOG.debug("Deleting row {}", Bytes.toString(getRowKey(primaryKeys, change)));
        break;
      default:
        LOG.warn("Operation of type '{}' will be ignored.", operationType);
    }
  }

  private static byte[] getRowKey(List<String> primaryKeys, StructuredRecord change) {
    // the primary keys are always in sorted order
    List<String> primaryValues = new ArrayList<>();
    String [] primaryKeysArray = primaryKeys.toArray(new String[primaryKeys.size()]);
    Arrays.sort(primaryKeysArray);
    for(String primaryKey : primaryKeysArray) {
      primaryValues.add(change.get(primaryKey).toString());
    }
    String joinedValue = Joiner.on(":").join(primaryValues);
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
        put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column), Bytes.toBytes((Integer) val));
        break;
      case LONG:
        put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column), Bytes.toBytes((Long) val));
        break;
      case FLOAT:
        put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column), Bytes.toBytes((Float) val));
        break;
      case DOUBLE:
        put.addColumn(Bytes.toBytes(CDC_COLUMN_FAMILY), Bytes.toBytes(column), Bytes.toBytes((Double) val));
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
