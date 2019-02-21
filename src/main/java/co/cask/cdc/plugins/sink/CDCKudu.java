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

package co.cask.cdc.plugins.sink;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.api.validation.InvalidStageException;
import co.cask.cdc.plugins.common.Schemas;
import com.google.common.collect.Sets;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.client.Update;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Spark compute plugin
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("CDCKudu")
public class CDCKudu extends SparkSink<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(CDCKudu.class);
  private final CDCKuduConfig config;
  private final Set<String> existingTables = new HashSet<>();

  public CDCKudu(CDCKuduConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
    if (!Schemas.CHANGE_SCHEMA.isCompatible(pipelineConfigurer.getStageConfigurer().getInputSchema())) {
      throw new InvalidStageException("Input schema is incompatible with change record schema");
    }
  }

  private boolean updateKuduTableSchema(KuduClient client, StructuredRecord ddlRecord) throws Exception {
    String tableName = Schemas.getTableName(ddlRecord.get(Schemas.TABLE_FIELD));
    if (!existingTables.contains(tableName) && !client.tableExists(tableName)) {
      // Table does not exists in the Kudu yet.
      // Creation of table will be attempted when we first see the DML Record.
      // Since at that point we know the primary keys to used.
      return false;
    }
    Schema newSchema = Schema.parseJson((String) ddlRecord.get(Schemas.SCHEMA_FIELD));

    KuduTable table = client.openTable(tableName);
    org.apache.kudu.Schema kuduTableSchema = table.getSchema();
    Set<String> oldColumns = new HashSet<>();
    for (ColumnSchema schema : kuduTableSchema.getColumns()) {
      oldColumns.add(schema.getName());
    }

    Set<String> newColumns = new HashSet<>();
    for (Schema.Field field : newSchema.getFields()) {
      newColumns.add(field.getName());
    }

    Sets.SetView<String> columnDiff = Sets.symmetricDifference(newColumns, oldColumns);
    Set<String> columnsToDelete = new HashSet<>();
    Set<String> columnsToAdd = new HashSet<>();
    for (String column : columnDiff) {
      if (oldColumns.contains(column)) {
        // This column is removed
        columnsToDelete.add(column);
      } else {
        // This column is added
        columnsToAdd.add(column);
      }
    }

    AlterTableOptions alterTableOptions = new AlterTableOptions();
    for (String column : columnsToDelete) {
      alterTableOptions.dropColumn(column);
      LOG.debug("Column {} will be dropped.", column);
    }

    for (String column : columnsToAdd) {
      Schema.Field newField = newSchema.getField(column);
      Type kuduType = toKuduType(column, newField.getSchema(), new HashSet<>());
      alterTableOptions.addNullableColumn(column, kuduType);
      LOG.debug("Column {} of type {} will be added to the Kudu table {}.", column, kuduType, table.getName());
    }

    if (!(columnsToAdd.isEmpty() && columnsToDelete.isEmpty())) {
      LOG.debug("Altering table {}, {}", table.getName(), alterTableOptions);
      client.alterTable(table.getName(), alterTableOptions);
      if (!client.isAlterTableDone(table.getName())) {
        LOG.warn("Failed to alter table {}", table.getName());
      }
      table = client.openTable(table.getName());
      LOG.debug("Columns after alter table {}", table.getSchema().getColumns());
    }

    return true;
  }

  private void updateKuduTableRecord(KuduClient client, KuduSession session, StructuredRecord dmlRecord)
    throws Exception {
    String tableName = Schemas.getTableName(dmlRecord.get(Schemas.TABLE_FIELD));
    String operationType = dmlRecord.get(Schemas.OP_TYPE_FIELD);
    List<String> primaryKeys = dmlRecord.get(Schemas.PRIMARY_KEYS_FIELD);
    Schema updateSchema = Schema.parseJson((String) dmlRecord.get(Schemas.UPDATE_SCHEMA_FIELD));
    Map<String, Object> updateValues = dmlRecord.get(Schemas.UPDATE_VALUES_FIELD);
    List<Schema.Field> fields = updateSchema.getFields();
    if (!existingTables.contains(tableName) && !client.tableExists(tableName)) {
      createKuduTable(client, tableName, fields, primaryKeys);
      existingTables.add(tableName);
    }

    KuduTable table = client.openTable(tableName);
    HashSet<String> primaryKeysSet = new HashSet<>(primaryKeys);
    switch (operationType) {
      case "I":
        Insert insert = table.newInsert();
        for (Schema.Field field : fields) {
          addColumnDataBasedOnType(insert.getRow(), field, updateValues.get(field.getName()), primaryKeysSet);
        }
        session.apply(insert);
        break;
      case "U":
        Update update = table.newUpdate();
        for (Schema.Field field : fields) {
          addColumnDataBasedOnType(update.getRow(), field, updateValues.get(field.getName()), primaryKeysSet);
        }
        session.apply(update);
        break;
      case "D":
        Delete delete = table.newDelete();
        for (String keyColumn : primaryKeys) {
          for (Schema.Field field : fields) {
            if (field.getName().equals(keyColumn)) {
              addColumnDataBasedOnType(delete.getRow(), field, updateValues.get(field.getName()), primaryKeysSet);
              break;
            }
          }
        }
        session.apply(delete);
        break;
      default:
        throw new RuntimeException("Illegal operation type " + operationType);
    }
  }

  private void addColumnDataBasedOnType(PartialRow row, Schema.Field field,
                                        @Nullable Object value, Set<String> primaryKeys)
    throws TypeConversionException {
    String columnName = field.getName();
    if (value == null) {
      row.setNull(columnName);
      return;
    }

    Type type = toKuduType(field.getName(), field.getSchema(), primaryKeys);
    switch (type) {
      case STRING:
        row.addString(columnName, String.valueOf(value));
        break;
      case INT32:
        row.addInt(columnName, (int) value);
        break;
      case INT64:
        row.addLong(columnName, (long) value);
        break;
      case BINARY:
        if (value instanceof ByteBuffer) {
          row.addBinary(columnName, (ByteBuffer) value);
        } else {
          row.addBinary(columnName, (byte[]) value);
        }
        break;
      case DOUBLE:
        row.addDouble(columnName, (double) value);
        break;
      case FLOAT:
        row.addFloat(columnName, (float) value);
        break;
      case BOOL:
        row.addBoolean(columnName, (boolean) value);
        break;
      default:
        throw new RuntimeException(String.format("Unexpected Kudu type '%s' found.", type));
    }
  }

  private void createKuduTable(KuduClient client, String tableName, List<Schema.Field> fields,
                               List<String> primaryKeys) throws Exception {
    // Convert the writeSchema into Kudu schema.
    List<ColumnSchema> columnSchemas = toKuduSchema(fields, new HashSet<>(primaryKeys),
                                                    config.getCompression(), config.getEncoding());
    org.apache.kudu.Schema kuduSchema = new org.apache.kudu.Schema(getOrderedSchemaColumns(primaryKeys, columnSchemas));
    CreateTableOptions options = new CreateTableOptions();
    options.addHashPartitions(primaryKeys, config.getBuckets(), config.getSeed());

    try {
      KuduTable table = client.createTable(tableName, kuduSchema, options);
      LOG.info("Successfully created Kudu table '{}', Table ID '{}'", tableName, table.getTableId());
    } catch (KuduException e) {
      throw new RuntimeException(String.format("Unable to create table '%s'. Reason : %s",
                                               tableName, e.getMessage()), e);
    }
  }

  // Create list of ColumnSchema where columns corresponding to the primary key will appear first.
  // This is required because of column ordering constraing in Kudu (https://issues.apache.org/jira/browse/KUDU-1271)
  private List<ColumnSchema> getOrderedSchemaColumns(List<String> primaryKeys, List<ColumnSchema> columnSchemas) {
    if (primaryKeys.isEmpty()) {
      return columnSchemas;
    }

    List<ColumnSchema> orderedColumnSchemas = new ArrayList<>();
    // First insert all the columns corresponding to the primary key
    for (String key : primaryKeys) {
      for (ColumnSchema columnSchema : columnSchemas) {
        if (columnSchema.getName().equals(key)) {
          orderedColumnSchemas.add(columnSchema);
          break;
        }
      }
    }

    HashSet<String> keySet = new HashSet<>(primaryKeys);
    for (ColumnSchema columnSchema : columnSchemas) {
      if (!keySet.contains(columnSchema.getName())) {
        orderedColumnSchemas.add(columnSchema);
      }
    }
    return orderedColumnSchemas;
  }

  /**
   * Converts from CDAP field types to Kudu types.
   *
   * @param fields CDAP Schema fields
   * @param columns List of columns that are considered as keys
   * @param algorithm Compression algorithm to be used for the column.
   * @param encoding Encoding type
   * @return List of {@link ColumnSchema}
   * @throws TypeConversionException thrown when CDAP schema cannot be converted to Kudu Schema.
   */
  private List<ColumnSchema> toKuduSchema(List<Schema.Field> fields, Set<String> columns,
                                          ColumnSchema.CompressionAlgorithm algorithm,
                                          ColumnSchema.Encoding encoding)
    throws TypeConversionException {
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    for (Schema.Field field : fields) {
      String name = field.getName();
      Type kuduType = toKuduType(name, field.getSchema(), columns);
      ColumnSchema.ColumnSchemaBuilder builder = new ColumnSchema.ColumnSchemaBuilder(name, kuduType);
      if (field.getSchema().isNullable() && !columns.contains(name)) {
        builder.nullable(true);
      }
      builder.encoding(encoding);
      builder.compressionAlgorithm(algorithm);
      if (columns.contains(name)) {
        builder.key(true);
      }
      columnSchemas.add(builder.build());
    }
    return columnSchemas;
  }

  /**
   * Convert from {@link Schema.Type} to {@link Type}.
   *
   * @param schema {@link StructuredRecord} field schema.
   * @return {@link Type} Kudu type.
   * @throws TypeConversionException thrown when can't be converted.
   */
  private Type toKuduType(String name, Schema schema, Set<String> primaryKeys)
    throws TypeConversionException {
    Schema.Type type = schema.getType();

    if (primaryKeys.contains(name)) {
      // primary key cannot be BOOL, FLOAT, or DOUBLE in Kudu
      // so if type is one of them, then convert to String
      if (type == Schema.Type.DOUBLE || type == Schema.Type.FLOAT || type == Schema.Type.BOOLEAN) {
        return Type.STRING;
      }
    }

    if (type == Schema.Type.STRING) {
      return Type.STRING;
    } else if (type == Schema.Type.INT) {
      return Type.INT32;
    } else if (type == Schema.Type.LONG) {
      return Type.INT64;
    } else if (type == Schema.Type.BYTES) {
      return Type.BINARY;
    } else if (type == Schema.Type.DOUBLE) {
      return Type.DOUBLE;
    } else if (type == Schema.Type.FLOAT) {
      return Type.FLOAT;
    } else if (type == Schema.Type.BOOLEAN) {
      return Type.BOOL;
    } else if (type == Schema.Type.UNION) { // Recursively drill down into the non-nullable type.
      return toKuduType(name, schema.getNonNullable(), primaryKeys);
    } else {
      throw new TypeConversionException(
        String.format("Field '%s' is having a type '%s' that is not supported by Kudu. Please change the type.",
                      name, type.toString())
      );
    }
  }

  @Override
  public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> javaRDD) throws Exception {
    javaRDD.foreachPartition(structuredRecordIterator -> {
      try (KuduClient client = getClient()) {
        KuduSession session = client.newSession();
        try {
          // Buffer 100 operations
          session.setMutationBufferSpace(100);
          session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);

          while (structuredRecordIterator.hasNext()) {
            StructuredRecord input = structuredRecordIterator.next();
            StructuredRecord changeRecord = input.get(Schemas.CHANGE_FIELD);
            if (changeRecord.getSchema().getRecordName().equals(Schemas.DDL_SCHEMA.getRecordName())) {
              if (updateKuduTableSchema(client, changeRecord)) {
                // Schema for the table is updated. Flush the session now
                session.flush();
              }
            } else {
              updateKuduTableRecord(client, session, changeRecord);
            }
          }
        } finally {
          session.close();
        }
      }
    });
  }

  private KuduClient getClient() {
    return new KuduClient.KuduClientBuilder(config.getMasterAddress())
      .defaultOperationTimeoutMs(config.getOperationTimeout())
      .defaultAdminOperationTimeoutMs(config.getAdministrationTimeout())
      .disableStatistics()
      .bossCount(config.getThreads())
      .build();
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws Exception {
    // no-op
  }
}
