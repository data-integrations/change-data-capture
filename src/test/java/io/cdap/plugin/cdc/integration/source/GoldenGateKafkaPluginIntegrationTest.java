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
import io.cdap.plugin.cdc.source.oracle.GoldenGateKafkaConfig;
import io.cdap.plugin.common.Constants;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@RunWith(Enclosed.class)
public class GoldenGateKafkaPluginIntegrationTest {
  private static final String PLUGIN_NAME = "CDCDatabase";
  private static final String HOST = System.getProperty("test.oracle-db.host", "localhost");
  private static final String PORT = System.getProperty("test.oracle-db.port", "1521");
  private static final String SERVICE = System.getProperty("test.oracle-db.service", "xe");
  private static final String USERNAME = System.getProperty("test.oracle-db.username", "trans_user");
  private static final String PASSWORD = System.getProperty("test.oracle-db.password", "trans_user");
  private static final String GOLDENGATE_BROKER = System.getProperty("test.goldengate.broker", "localhost:9092");
  private static final String GOLDENGATE_TOPIC = System.getProperty("test.goldengate.topic", "oggtopic");

  private static final String ORACLE_DRIVER_JAR_PATH = System.getProperty("test.oracle-db.driver.jar");
  private static final String ORACLE_DRIVER_CLASS
    = System.getProperty("test.oracle-db.driver.class", "oracle.jdbc.OracleDriver");

  private static final String DB_NAMESPACE = USERNAME;
  private static final String ID_COLUMN = "ID";
  private static final String VALUE_COLUMN = "VALUE";

  private static Driver driver;

  private static Connection getConnection() throws SQLException {
    String connectionString = String.format("jdbc:oracle:thin:@//%s:%s/%s", HOST, PORT, SERVICE);
    Properties properties = new Properties();
    if (USERNAME != null) {
      properties.put("user", USERNAME);
    }
    if (PASSWORD != null) {
      properties.put("password", PASSWORD);
    }
    return driver.connect(connectionString, properties);
  }

  public static class Validations extends CDCPluginIntegrationTestBase {
    @Rule
    public TestName testName = new TestName();

    private String outputTable;

    @BeforeClass
    public static void setUpClass() throws Exception {
      initOracleDriver();
    }

    @Before
    public void setUp() throws Exception {
      outputTable = testName.getMethodName() + "_out";
    }

    @Test
    public void testDeploymentFailedWithoutConnectionToBroker() throws Exception {
      ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
        .put(GoldenGateKafkaConfig.BROKER, "non_existing_broker")
        .put(GoldenGateKafkaConfig.TOPIC, GOLDENGATE_TOPIC)
        .put(Constants.Reference.REFERENCE_NAME, "GoldenGateKafkaSource")
        .build();
      ETLPlugin sourceConfig = new ETLPlugin(PLUGIN_NAME, StreamingSource.PLUGIN_TYPE, sourceProps);

      ETLPlugin sinkConfig = MockSink.getPlugin(outputTable);

      Assertions.assertThatThrownBy(() -> deployETL(sourceConfig, sinkConfig, "testChangesSource"))
        .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testDeploymentFailedWithoutKafkaTopic() throws Exception {
      ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
        .put(GoldenGateKafkaConfig.BROKER, GOLDENGATE_BROKER)
        .put(GoldenGateKafkaConfig.TOPIC, "non_existing_topic_" + System.nanoTime())
        .put(Constants.Reference.REFERENCE_NAME, "GoldenGateKafkaSource")
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

    private String dbTableName;
    private String outputTable;
    private SparkManager programManager;

    @BeforeClass
    public static void setUpClass() throws Exception {
      Assume.assumeNotNull(ORACLE_DRIVER_JAR_PATH);
      initOracleDriver();
    }

    @Before
    public void setUp() throws Exception {
      // table name should be unique to recieve DDL message from GoldenGate
      dbTableName = testName.getMethodName() + '_' + System.nanoTime();
      outputTable = testName.getMethodName() + "_out";

      createTable(dbTableName);

      ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
        .put(GoldenGateKafkaConfig.BROKER, GOLDENGATE_BROKER)
        .put(GoldenGateKafkaConfig.TOPIC, GOLDENGATE_TOPIC)
        .put(Constants.Reference.REFERENCE_NAME, "GoldenGateKafkaSource")
        .build();
      ETLPlugin sourceConfig = new ETLPlugin(PLUGIN_NAME, StreamingSource.PLUGIN_TYPE, sourceProps);

      ETLPlugin sinkConfig = MockSink.getPlugin(outputTable);

      programManager = deployETL(sourceConfig, sinkConfig, "testChangesSource");
      programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
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
    }

    @Test
    public void testTrackMultipleInserts() throws Exception {
      String insertQuery = String.format("insert into %s(id, value) values (?, ?)", dbTableName);
      try (Connection connection = getConnection();
           PreparedStatement statement = connection.prepareStatement(insertQuery)) {
        for (int i = 1; i <= 3; i++) {
          statement.setInt(1, i);
          statement.setString(2, "val " + i);
          statement.execute();
        }
      }

      Schema tableSchema = Schema.recordOf(
        Schemas.SCHEMA_RECORD,
        Schema.Field.of(ID_COLUMN, Schema.unionOf(Schema.of(Schema.Type.NULL), Schema.of(Schema.Type.LONG))),
        Schema.Field.of(VALUE_COLUMN, Schema.unionOf(Schema.of(Schema.Type.NULL), Schema.of(Schema.Type.STRING)))
      );
      List<String> primaryKeys = Collections.singletonList(ID_COLUMN);
      String tableName = Joiner.on(".").join(DB_NAMESPACE, dbTableName).toUpperCase();
      StructuredRecord ddlRecord = StructuredRecord.builder(Schemas.DDL_SCHEMA)
        .set(Schemas.TABLE_FIELD, tableName)
        .set(Schemas.SCHEMA_FIELD, tableSchema.toString())
        .build();
      StructuredRecord dmlRecord1 = StructuredRecord.builder(Schemas.DML_SCHEMA)
        .set(Schemas.TABLE_FIELD, tableName)
        .set(Schemas.PRIMARY_KEYS_FIELD, primaryKeys)
        .set(Schemas.OP_TYPE_FIELD, OperationType.INSERT.name())
        .set(Schemas.UPDATE_SCHEMA_FIELD, tableSchema.toString())
        .set(Schemas.UPDATE_VALUES_FIELD, ImmutableMap.<String, Object>builder()
          .put(ID_COLUMN, 1)
          .put(VALUE_COLUMN, "val 1")
          .build())
        .build();
      StructuredRecord dmlRecord2 = StructuredRecord.builder(Schemas.DML_SCHEMA)
        .set(Schemas.TABLE_FIELD, tableName)
        .set(Schemas.PRIMARY_KEYS_FIELD, primaryKeys)
        .set(Schemas.OP_TYPE_FIELD, OperationType.INSERT.name())
        .set(Schemas.UPDATE_SCHEMA_FIELD, tableSchema.toString())
        .set(Schemas.UPDATE_VALUES_FIELD, ImmutableMap.<String, Object>builder()
          .put(ID_COLUMN, 2)
          .put(VALUE_COLUMN, "val 2")
          .build())
        .build();
      StructuredRecord dmlRecord3 = StructuredRecord.builder(Schemas.DML_SCHEMA)
        .set(Schemas.TABLE_FIELD, tableName)
        .set(Schemas.PRIMARY_KEYS_FIELD, primaryKeys)
        .set(Schemas.OP_TYPE_FIELD, OperationType.INSERT.name())
        .set(Schemas.UPDATE_SCHEMA_FIELD, tableSchema.toString())
        .set(Schemas.UPDATE_VALUES_FIELD, ImmutableMap.<String, Object>builder()
          .put(ID_COLUMN, 3)
          .put(VALUE_COLUMN, "val 3")
          .build())
        .build();

      waitForRecords(outputTable, ImmutableList.of(ddlRecord, dmlRecord1, dmlRecord2, dmlRecord3));
    }

    @Test
    public void testTrackInsertUpdateDeleteOperations() throws Exception {
      // INSERT operation
      try (Connection connection = getConnection();
           Statement statement = connection.createStatement()) {
        statement.executeUpdate(String.format("insert into %s(id, value) values (1, 'initial value')", dbTableName));
      }

      Schema tableSchema = Schema.recordOf(
        Schemas.SCHEMA_RECORD,
        Schema.Field.of(ID_COLUMN, Schema.unionOf(Schema.of(Schema.Type.NULL), Schema.of(Schema.Type.LONG))),
        Schema.Field.of(VALUE_COLUMN, Schema.unionOf(Schema.of(Schema.Type.NULL), Schema.of(Schema.Type.STRING)))
      );
      List<String> primaryKeys = Collections.singletonList(ID_COLUMN);
      String tableName = Joiner.on(".").join(DB_NAMESPACE, dbTableName).toUpperCase();
      StructuredRecord ddlRecord = StructuredRecord.builder(Schemas.DDL_SCHEMA)
        .set(Schemas.TABLE_FIELD, tableName)
        .set(Schemas.SCHEMA_FIELD, tableSchema.toString())
        .build();
      StructuredRecord dmlRecord1 = StructuredRecord.builder(Schemas.DML_SCHEMA)
        .set(Schemas.TABLE_FIELD, tableName)
        .set(Schemas.PRIMARY_KEYS_FIELD, primaryKeys)
        .set(Schemas.OP_TYPE_FIELD, OperationType.INSERT.name())
        .set(Schemas.UPDATE_SCHEMA_FIELD, tableSchema.toString())
        .set(Schemas.UPDATE_VALUES_FIELD, ImmutableMap.<String, Object>builder()
          .put(ID_COLUMN, 1)
          .put(VALUE_COLUMN, "initial value")
          .build())
        .build();

      waitForRecords(outputTable, ImmutableList.of(ddlRecord, dmlRecord1));

      // UPDATE operation
      try (Connection connection = getConnection();
           Statement statement = connection.createStatement()) {
        statement.executeUpdate(String.format("update %s set value='updated value' where id=1", dbTableName));
      }

      StructuredRecord dmlRecord2 = StructuredRecord.builder(Schemas.DML_SCHEMA)
        .set(Schemas.TABLE_FIELD, tableName)
        .set(Schemas.PRIMARY_KEYS_FIELD, primaryKeys)
        .set(Schemas.OP_TYPE_FIELD, OperationType.UPDATE.name())
        .set(Schemas.UPDATE_SCHEMA_FIELD, tableSchema.toString())
        .set(Schemas.UPDATE_VALUES_FIELD, ImmutableMap.<String, Object>builder()
          .put(ID_COLUMN, 1)
          .put(VALUE_COLUMN, "updated value")
          .build())
        .build();

      waitForRecords(outputTable, ImmutableList.of(ddlRecord, dmlRecord1, dmlRecord2));

      // DELETE operation
      try (Connection connection = getConnection();
           Statement statement = connection.createStatement()) {
        statement.executeUpdate(String.format("delete from %s where id=1", dbTableName));
      }

      StructuredRecord dmlRecord3 = StructuredRecord.builder(Schemas.DML_SCHEMA)
        .set(Schemas.TABLE_FIELD, tableName)
        .set(Schemas.PRIMARY_KEYS_FIELD, primaryKeys)
        .set(Schemas.OP_TYPE_FIELD, OperationType.DELETE.name())
        .set(Schemas.UPDATE_SCHEMA_FIELD, tableSchema.toString())
        .set(Schemas.UPDATE_VALUES_FIELD, ImmutableMap.<String, Object>builder()
          .put(ID_COLUMN, 1)
          .put(VALUE_COLUMN, "updated value")
          .build())
        .build();

      waitForRecords(outputTable, ImmutableList.of(ddlRecord, dmlRecord1, dmlRecord2, dmlRecord3));
    }

    private static void createTable(String tableName) throws SQLException {
      try (Connection connection = getConnection();
           Statement statement = connection.createStatement()) {
        statement.executeUpdate(String.format("create table %s (id NUMBER(5) primary key not null, value VARCHAR2(15))",
                                              tableName));
      }
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

  private static void initOracleDriver() throws Exception {
    File driverJar = new File(ORACLE_DRIVER_JAR_PATH);
    URLClassLoader child = new URLClassLoader(
      new URL[]{driverJar.toURI().toURL()},
      GoldenGateKafkaPluginIntegrationTest.class.getClassLoader()
    );
    Class<?> aClass = Class.forName(ORACLE_DRIVER_CLASS, true, child);
    driver = (Driver) aClass.newInstance();
  }
}
