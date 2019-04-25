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

package io.cdap.plugin.cdc.integration.source;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.plugin.cdc.common.OperationType;
import io.cdap.plugin.cdc.common.Schemas;
import io.cdap.plugin.cdc.integration.CDCPluginIntegrationTestBase;
import io.cdap.plugin.cdc.integration.StructuredRecordRepresentation;
import io.cdap.plugin.cdc.source.sqlserver.CTSQLServerConfig;
import io.cdap.plugin.common.Constants;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@RunWith(Enclosed.class)
public class CTSQLServerPluginIntegrationTest {
  private static final String PLUGIN_NAME = "CTSQLServer";
  private static final String HOST = System.getProperty("test.sql-server.host", "localhost");
  private static final String PORT = System.getProperty("test.sql-server.port", "1433");
  private static final String USERNAME = System.getProperty("test.sql-server.username", "SA");
  private static final String PASSWORD = System.getProperty("test.sql-server.password", "123Qwe123");
  private static final String DB_NAMESPACE = System.getProperty("test.sql-server.namespace", "dbo");

  private static void createDatabaseWithTracking(String dbName) throws SQLException {
    try (Connection connection = getConnectionToRoot();
         Statement statement = connection.createStatement()) {
      statement.executeUpdate(String.format("create database %s", dbName));
      statement.executeUpdate(String.format("alter database %s set change_tracking = ON", dbName));
    }
  }

  private static void dropDatabaseIfExists(String dbName) throws SQLException {
    try (Connection connection = getConnectionToRoot();
         Statement statement = connection.createStatement()) {
      statement.executeUpdate(String.format("drop database if exists %s", dbName));
    }
  }

  private static Connection getConnectionToRoot() throws SQLException {
    String connectionString = String.format("jdbc:sqlserver://%s:%s", HOST, PORT);
    return DriverManager.getConnection(connectionString, USERNAME, PASSWORD);
  }

  public static class Validations extends CDCPluginIntegrationTestBase {
    @Rule
    public TestName testName = new TestName();

    private String dbName;
    private String outputTable;

    @BeforeClass
    public static void setUpClass() throws Exception {
      Class.forName(SQLServerDriver.class.getName());
    }

    @Before
    public void setUp() throws Exception {
      dbName = "CTSQLServerPluginTest_" + testName.getMethodName();
      outputTable = testName.getMethodName() + "_out";
      dropDatabaseIfExists(dbName);
    }

    @After
    @Override
    public void afterTest() throws Exception {
      super.afterTest();
      dropDatabaseIfExists(dbName);
    }

    @Test
    public void testDeploymentFailedWithoutConnectionToDB() throws Exception {
      ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
        .put(CTSQLServerConfig.HOST_NAME, HOST)
        .put(CTSQLServerConfig.PORT, PORT)
        .put(CTSQLServerConfig.USERNAME, USERNAME)
        .put(CTSQLServerConfig.PASSWORD, PASSWORD)
        .put(CTSQLServerConfig.DATABASE_NAME, "non_existing_db")
        .put(Constants.Reference.REFERENCE_NAME, "CTSQLServerSource")
        .build();
      ETLPlugin sourceConfig = new ETLPlugin(PLUGIN_NAME, StreamingSource.PLUGIN_TYPE, sourceProps);

      ETLPlugin sinkConfig = MockSink.getPlugin(outputTable);

      Assertions.assertThatThrownBy(() -> deployETL(sourceConfig, sinkConfig, "testChangesSource"))
        .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testDeploymentFailedWithoutTrackingEnabled() throws Exception {
      try (Connection connection = getConnectionToRoot();
           Statement statement = connection.createStatement()) {
        statement.executeUpdate(String.format("create database %s", dbName));
      }

      ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
        .put(CTSQLServerConfig.HOST_NAME, HOST)
        .put(CTSQLServerConfig.PORT, PORT)
        .put(CTSQLServerConfig.USERNAME, USERNAME)
        .put(CTSQLServerConfig.PASSWORD, PASSWORD)
        .put(CTSQLServerConfig.DATABASE_NAME, dbName)
        .put(Constants.Reference.REFERENCE_NAME, "CTSQLServerSource")
        .build();
      ETLPlugin sourceConfig = new ETLPlugin(PLUGIN_NAME, StreamingSource.PLUGIN_TYPE, sourceProps);

      ETLPlugin sinkConfig = MockSink.getPlugin(outputTable);

      Assertions.assertThatThrownBy(() -> deployETL(sourceConfig, sinkConfig, "testChangesSource"))
        .isInstanceOf(IllegalStateException.class);
    }

  }

  public static class SuccessFlow extends CDCPluginIntegrationTestBase {
    @Rule
    public TestName testName = new TestName();

    private String dbName;
    private String dbTableName;
    private String outputTable;
    private SparkManager programManager;

    @BeforeClass
    public static void setUpClass() throws Exception {
      Class.forName(SQLServerDriver.class.getName());
    }

    @Before
    public void setUp() throws Exception {
      dbName = "CTSQLServerPluginTest_" + testName.getMethodName();
      dropDatabaseIfExists(dbName);
      createDatabaseWithTracking(dbName);
      dbTableName = testName.getMethodName();
      outputTable = testName.getMethodName() + "_out";

      ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
        .put(CTSQLServerConfig.HOST_NAME, HOST)
        .put(CTSQLServerConfig.PORT, PORT)
        .put(CTSQLServerConfig.USERNAME, USERNAME)
        .put(CTSQLServerConfig.PASSWORD, PASSWORD)
        .put(CTSQLServerConfig.DATABASE_NAME, dbName)
        .put(Constants.Reference.REFERENCE_NAME, "CTSQLServerSource")
        .build();
      ETLPlugin sourceConfig = new ETLPlugin(PLUGIN_NAME, StreamingSource.PLUGIN_TYPE, sourceProps);

      ETLPlugin sinkConfig = MockSink.getPlugin(outputTable);

      programManager = deployETL(sourceConfig, sinkConfig, "testChangesSource");
      programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

      createTableWithTracking(dbTableName);
    }

    @After
    @Override
    public void afterTest() throws Exception {
      if (programManager != null) {
        programManager.stop();
        programManager.waitForStopped(10, TimeUnit.SECONDS);
        programManager.waitForRun(ProgramRunStatus.KILLED, 10, TimeUnit.SECONDS);
      }
      super.afterTest();
      dropDatabaseIfExists(dbName);
    }

    @Test
    public void testTrackMultipleInserts() throws Exception {
      try (Connection connection = getConnectionToDb();
           Statement statement = connection.createStatement()) {
        statement.executeUpdate(String.format("insert into %s(value) values ('val 1'), ('val 2'), ('val 3')",
                                              dbTableName));
      }

      Schema tableSchema = Schema.recordOf(
        Schemas.SCHEMA_RECORD,
        Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
        Schema.Field.of("value", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
      );
      List<String> primaryKeys = Collections.singletonList("id");
      StructuredRecord ddlRecord = StructuredRecord.builder(Schemas.DDL_SCHEMA)
        .set(Schemas.TABLE_FIELD, Joiner.on(".").join(DB_NAMESPACE, dbTableName))
        .set(Schemas.SCHEMA_FIELD, tableSchema.toString())
        .build();
      StructuredRecord dmlRecord1 = StructuredRecord.builder(Schemas.DML_SCHEMA)
        .set(Schemas.TABLE_FIELD, Joiner.on(".").join(DB_NAMESPACE, dbTableName))
        .set(Schemas.PRIMARY_KEYS_FIELD, primaryKeys)
        .set(Schemas.OP_TYPE_FIELD, OperationType.INSERT.name())
        .set(Schemas.UPDATE_SCHEMA_FIELD, tableSchema.toString())
        .set(Schemas.UPDATE_VALUES_FIELD, ImmutableMap.<String, Object>builder()
          .put("id", 1)
          .put("value", "val 1")
          .build())
        .build();
      StructuredRecord dmlRecord2 = StructuredRecord.builder(Schemas.DML_SCHEMA)
        .set(Schemas.TABLE_FIELD, Joiner.on(".").join(DB_NAMESPACE, dbTableName))
        .set(Schemas.PRIMARY_KEYS_FIELD, primaryKeys)
        .set(Schemas.OP_TYPE_FIELD, OperationType.INSERT.name())
        .set(Schemas.UPDATE_SCHEMA_FIELD, tableSchema.toString())
        .set(Schemas.UPDATE_VALUES_FIELD, ImmutableMap.<String, Object>builder()
          .put("id", 2)
          .put("value", "val 2")
          .build())
        .build();
      StructuredRecord dmlRecord3 = StructuredRecord.builder(Schemas.DML_SCHEMA)
        .set(Schemas.TABLE_FIELD, Joiner.on(".").join(DB_NAMESPACE, dbTableName))
        .set(Schemas.PRIMARY_KEYS_FIELD, primaryKeys)
        .set(Schemas.OP_TYPE_FIELD, OperationType.INSERT.name())
        .set(Schemas.UPDATE_SCHEMA_FIELD, tableSchema.toString())
        .set(Schemas.UPDATE_VALUES_FIELD, ImmutableMap.<String, Object>builder()
          .put("id", 3)
          .put("value", "val 3")
          .build())
        .build();

      waitForRecords(outputTable, ImmutableList.of(ddlRecord, dmlRecord1, dmlRecord2, dmlRecord3));
    }

    @Test
    public void testTrackInsertUpdateOperations() throws Exception {
      try (Connection connection = getConnectionToDb();
           Statement statement = connection.createStatement()) {
        statement.executeUpdate(String.format("insert into %s(value) values ('initial value')", dbTableName));
        statement.executeUpdate(String.format("update %s set value='updated value' where id=1", dbTableName));
      }

      Schema tableSchema = Schema.recordOf(
        Schemas.SCHEMA_RECORD,
        Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
        Schema.Field.of("value", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
      );
      List<String> primaryKeys = Collections.singletonList("id");
      StructuredRecord ddlRecord = StructuredRecord.builder(Schemas.DDL_SCHEMA)
        .set(Schemas.TABLE_FIELD, Joiner.on(".").join(DB_NAMESPACE, dbTableName))
        .set(Schemas.SCHEMA_FIELD, tableSchema.toString())
        .build();
      StructuredRecord dmlRecord1 = StructuredRecord.builder(Schemas.DML_SCHEMA)
        .set(Schemas.TABLE_FIELD, Joiner.on(".").join(DB_NAMESPACE, dbTableName))
        .set(Schemas.PRIMARY_KEYS_FIELD, primaryKeys)
        .set(Schemas.OP_TYPE_FIELD, OperationType.INSERT.name())
        .set(Schemas.UPDATE_SCHEMA_FIELD, tableSchema.toString())
        .set(Schemas.UPDATE_VALUES_FIELD, ImmutableMap.<String, Object>builder()
          .put("id", 1)
          .put("value", "updated value")
          .build())
        .build();

      waitForRecords(outputTable, ImmutableList.of(ddlRecord, dmlRecord1));
    }

    @Test
    public void testTrackDeleteOperations() throws Exception {
      try (Connection connection = getConnectionToDb();
           Statement statement = connection.createStatement()) {
        statement.executeUpdate(String.format("insert into %s(value) values ('initial value')", dbTableName));
        statement.executeUpdate(String.format("delete from %s where id=1", dbTableName));
      }

      Schema tableSchema = Schema.recordOf(
        Schemas.SCHEMA_RECORD,
        Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
        Schema.Field.of("value", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
      );
      List<String> primaryKeys = Collections.singletonList("id");
      Map<Object, Object> updatedValuesMap = new HashMap<>();
      updatedValuesMap.put("id", 1);
      updatedValuesMap.put("value", null);
      StructuredRecord dmlRecord1 = StructuredRecord.builder(Schemas.DML_SCHEMA)
        .set(Schemas.TABLE_FIELD, Joiner.on(".").join(DB_NAMESPACE, dbTableName))
        .set(Schemas.PRIMARY_KEYS_FIELD, primaryKeys)
        .set(Schemas.OP_TYPE_FIELD, OperationType.DELETE.name())
        .set(Schemas.UPDATE_SCHEMA_FIELD, tableSchema.toString())
        .set(Schemas.UPDATE_VALUES_FIELD, updatedValuesMap)
        .build();

      waitForRecords(outputTable, ImmutableList.of(dmlRecord1));
    }

    @Test
    public void testTrackInsertUpdateDeleteOperations() throws Exception {
      // INSERT operation
      try (Connection connection = getConnectionToDb();
           Statement statement = connection.createStatement()) {
        statement.executeUpdate(String.format("insert into %s(value) values ('initial value')", dbTableName));
      }

      Schema tableSchema = Schema.recordOf(
        Schemas.SCHEMA_RECORD,
        Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
        Schema.Field.of("value", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
      );
      List<String> primaryKeys = Collections.singletonList("id");
      StructuredRecord ddlRecord = StructuredRecord.builder(Schemas.DDL_SCHEMA)
        .set(Schemas.TABLE_FIELD, Joiner.on(".").join(DB_NAMESPACE, dbTableName))
        .set(Schemas.SCHEMA_FIELD, tableSchema.toString())
        .build();
      StructuredRecord dmlRecord1 = StructuredRecord.builder(Schemas.DML_SCHEMA)
        .set(Schemas.TABLE_FIELD, Joiner.on(".").join(DB_NAMESPACE, dbTableName))
        .set(Schemas.PRIMARY_KEYS_FIELD, primaryKeys)
        .set(Schemas.OP_TYPE_FIELD, OperationType.INSERT.name())
        .set(Schemas.UPDATE_SCHEMA_FIELD, tableSchema.toString())
        .set(Schemas.UPDATE_VALUES_FIELD, ImmutableMap.<String, Object>builder()
          .put("id", 1)
          .put("value", "initial value")
          .build())
        .build();

      waitForRecords(outputTable, ImmutableList.of(ddlRecord, dmlRecord1));

      // UPDATE operation
      try (Connection connection = getConnectionToDb();
           Statement statement = connection.createStatement()) {
        statement.executeUpdate(String.format("update %s set value='updated value' where id=1", dbTableName));
      }

      StructuredRecord dmlRecord2 = StructuredRecord.builder(Schemas.DML_SCHEMA)
        .set(Schemas.TABLE_FIELD, Joiner.on(".").join(DB_NAMESPACE, dbTableName))
        .set(Schemas.PRIMARY_KEYS_FIELD, primaryKeys)
        .set(Schemas.OP_TYPE_FIELD, OperationType.UPDATE.name())
        .set(Schemas.UPDATE_SCHEMA_FIELD, tableSchema.toString())
        .set(Schemas.UPDATE_VALUES_FIELD, ImmutableMap.<String, Object>builder()
          .put("id", 1)
          .put("value", "updated value")
          .build())
        .build();

      waitForRecords(outputTable, ImmutableList.of(ddlRecord, dmlRecord1, dmlRecord2));

      // DELETE operation
      try (Connection connection = getConnectionToDb();
           Statement statement = connection.createStatement()) {
        statement.executeUpdate(String.format("delete from %s where id=1", dbTableName));
      }

      Map<Object, Object> updatedValuesMap = new HashMap<>();
      updatedValuesMap.put("id", 1);
      updatedValuesMap.put("value", null);
      StructuredRecord dmlRecord3 = StructuredRecord.builder(Schemas.DML_SCHEMA)
        .set(Schemas.TABLE_FIELD, Joiner.on(".").join(DB_NAMESPACE, dbTableName))
        .set(Schemas.PRIMARY_KEYS_FIELD, primaryKeys)
        .set(Schemas.OP_TYPE_FIELD, OperationType.DELETE.name())
        .set(Schemas.UPDATE_SCHEMA_FIELD, tableSchema.toString())
        .set(Schemas.UPDATE_VALUES_FIELD, updatedValuesMap)
        .build();

      waitForRecords(outputTable, ImmutableList.of(ddlRecord, dmlRecord1, dmlRecord2, dmlRecord3));
    }

    private void createTableWithTracking(String tableName) throws SQLException {
      try (Connection connection = getConnectionToDb();
           Statement statement = connection.createStatement()) {
        statement.executeUpdate(String.format("create table %s (id bigint identity primary key, value text)",
                                              tableName));
        statement.executeUpdate(String.format("alter table %s enable change_tracking", tableName));
      }
    }

    private Connection getConnectionToDb() throws SQLException {
      String connectionString = String.format("jdbc:sqlserver://%s:%s;DatabaseName=%s", HOST, PORT, dbName);
      return DriverManager.getConnection(connectionString, USERNAME, PASSWORD);
    }

    private void waitForRecords(String outputTable, List<StructuredRecord> expectedRecords) throws Exception {
      List<StructuredRecord> expectedCDCRecords = expectedRecords.stream()
        .map(Schemas::toCDCRecord)
        .collect(Collectors.toList());

      Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
        DataSetManager<Table> outputManager = getDataset(outputTable);
        List<StructuredRecord> output = MockSink.readOutput(outputManager);
        Assertions.assertThat(output)
          .withRepresentation(new StructuredRecordRepresentation())
          .hasSizeGreaterThanOrEqualTo(expectedRecords.size());
      });

      DataSetManager<Table> outputManager = getDataset(outputTable);
      List<StructuredRecord> output = MockSink.readOutput(outputManager);

      Assertions.assertThat(output)
        .withRepresentation(new StructuredRecordRepresentation())
        .isEqualTo(expectedCDCRecords);
    }
  }
}
