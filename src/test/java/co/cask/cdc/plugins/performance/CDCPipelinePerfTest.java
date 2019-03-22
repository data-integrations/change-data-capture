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

package co.cask.cdc.plugins.performance;

import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.test.SparkManager;
import co.cask.cdc.plugins.common.BigtableOperations;
import co.cask.cdc.plugins.sink.CDCBigTableConfig;
import co.cask.cdc.plugins.source.sqlserver.CTSQLServerConfig;
import co.cask.hydrator.common.Constants;
import com.google.bigtable.repackaged.com.google.cloud.ServiceOptions;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.grpc.io.IOExceptionWithStatus;
import com.google.bigtable.repackaged.io.grpc.StatusRuntimeException;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CDCPipelinePerfTest extends CDCPluginPerfTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(CDCPipelinePerfTest.class);
  private static final String APP_NAME = CDCPipelinePerfTest.class.getSimpleName();

  // Common properties
  private static final boolean TEST_DATA_LOAD =
    Boolean.parseBoolean(System.getProperty("ptest.test-data.load", "true"));
  private static final int TEST_DATA_INSERTS =
    Integer.parseInt(System.getProperty("ptest.test-data.inserts", "5000"));
  private static final int TEST_TARGET_TABLE_CREATED_TIMEOUT_SECONDS =
    Integer.parseInt(System.getProperty("ptest.target-table-created-timeout.seconds", "300"));
  private static final int TEST_DATA_TRANSFERED_TIMEOUT_SECONDS =
    Integer.parseInt(System.getProperty("ptest.data-transferred-timeout.seconds", "600"));

  // Bigtable properties
  private static final String BIGTABLE_PROJECT
    = System.getProperty("ptest.bigtable.project", ServiceOptions.getDefaultProjectId());
  private static final String BIGTABLE_INSTANCE = System.getProperty("ptest.bigtable.instance");
  private static final String BIGTABLE_SERVICE_ACCOUNT_FILE_PATH
    = System.getProperty("ptest.bigtable.serviceFilePath", System.getenv("CREDENTIAL_ENV_NAME"));

  // SQL Server properties
  private static final String SQL_HOST = System.getProperty("ptest.sql-server.host", "localhost");
  private static final String SQL_PORT = System.getProperty("ptest.sql-server.port", "1433");
  private static final String SQL_USERNAME = System.getProperty("ptest.sql-server.username", "SA");
  private static final String SQL_PASSWORD = System.getProperty("ptest.sql-server.password", "123Qwe123");

  @Rule
  public TestName testName = new TestName();

  private String dbName;
  private String dbTableName;
  private SparkManager programManager;
  private Connection connection;

  @Before
  @Override
  public void setUp() throws Exception {
    Assume.assumeNotNull(BIGTABLE_PROJECT);
    Assume.assumeNotNull(BIGTABLE_INSTANCE);

    super.setUp();

    dbName = CDCPipelinePerfTest.class.getSimpleName() + '_' + testName.getMethodName();
    dbTableName = testName.getMethodName();

    connection = BigtableOperations.connect(BIGTABLE_PROJECT, BIGTABLE_INSTANCE, BIGTABLE_SERVICE_ACCOUNT_FILE_PATH);

    // cleanup Bigtable
    LOG.info("Cleaning up Bigtable");
    BigtableOperations.dropTableIfExists(connection, dbTableName);

    if (TEST_DATA_LOAD) {
      LOG.info("Preparing test data");
      // cleanup SQL Server
      LOG.info("Cleaning up SQL Server");
      dropDatabaseIfExists(dbName);
      // prepare data
      LOG.info("Inserting test data ({} records)", TEST_DATA_INSERTS);
      createDatabaseWithTracking(dbName);
      createTableWithTracking(dbTableName);
      try (java.sql.Connection connection = getConnectionToDb();
           Statement statement = connection.createStatement()) {
        for (int i = 0; i < TEST_DATA_INSERTS; i++) {
          statement.executeUpdate(String.format("insert into %s(value) values ('initial value')", dbTableName));
        }
      }
    }

    LOG.info("Deploying application");

    ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
      .put(CTSQLServerConfig.HOST_NAME, SQL_HOST)
      .put(CTSQLServerConfig.PORT, SQL_PORT)
      .put(CTSQLServerConfig.USERNAME, SQL_USERNAME)
      .put(CTSQLServerConfig.PASSWORD, SQL_PASSWORD)
      .put(CTSQLServerConfig.DATABASE_NAME, dbName)
      .put(Constants.Reference.REFERENCE_NAME, "CTSQLServerSource")
      .build();
    ETLPlugin sourceConfig = new ETLPlugin("CTSQLServer", StreamingSource.PLUGIN_TYPE, sourceProps);

    Map<String, String> sinkProps = ImmutableMap.<String, String>builder()
      .put(CDCBigTableConfig.PROJECT, BIGTABLE_PROJECT)
      .put(CDCBigTableConfig.INSTANCE, BIGTABLE_INSTANCE)
      .put(CDCBigTableConfig.SERVICE_ACCOUNT_FILE_PATH, BIGTABLE_SERVICE_ACCOUNT_FILE_PATH)
      .put(Constants.Reference.REFERENCE_NAME, "CDCBigTableSink")
      .build();
    ETLPlugin sinkConfig = new ETLPlugin("CDCBigTable", SparkSink.PLUGIN_TYPE, sinkProps);

    programManager = deployETL(sourceConfig, sinkConfig, APP_NAME);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    if (programManager != null) {
      programManager.stop();
      programManager.waitForStopped(10, TimeUnit.SECONDS);
      programManager.waitForRun(ProgramRunStatus.KILLED, 10, TimeUnit.SECONDS);
    }
    super.tearDown();
    if (connection != null) {
      connection.close();
    }
  }

  @Test
  public void testSqlServerToBigtablePipeline() throws Exception {
    long testStart = System.currentTimeMillis();
    LOG.info("Starting pipeline");
    programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    LOG.info("Waiting until {} records are present in Bigtable", TEST_DATA_INSERTS);

    Awaitility.await()
      .atMost(TEST_TARGET_TABLE_CREATED_TIMEOUT_SECONDS, TimeUnit.SECONDS)
      .pollInterval(Duration.TEN_SECONDS)
      .ignoreException(StatusRuntimeException.class)
      .untilAsserted(() -> {
        TableName tableName = TableName.valueOf(dbTableName);
        Assert.assertTrue(String.format("Table '%s' was not created", tableName),
                          connection.getAdmin().tableExists(tableName));
        Assert.assertTrue(connection.getAdmin().isTableAvailable(tableName));
        Assert.assertTrue(connection.getAdmin().isTableEnabled(tableName));
    });

    Awaitility.await()
      .atMost(TEST_DATA_TRANSFERED_TIMEOUT_SECONDS, TimeUnit.SECONDS)
      .pollInterval(Duration.TEN_SECONDS)
      .ignoreException(IOExceptionWithStatus.class)
      .untilAsserted(() -> {
        TableName tableName = TableName.valueOf(dbTableName);
        Table table = connection.getTable(tableName);
        int rowCount = getRowCount(table);
        LOG.info("Currently {} records are present in Bigtable", rowCount);
        Assert.assertEquals(TEST_DATA_INSERTS, rowCount);
      });

    long testEnd = System.currentTimeMillis();
    long elapsedSeconds = (testEnd - testStart) / 1000;
    long recordsPerSecond = TEST_DATA_INSERTS / elapsedSeconds;
    LOG.info("Test finished. Transferred '{}' records. Elapsed time is '{} seconds' ({} records/second)",
             TEST_DATA_INSERTS, elapsedSeconds, recordsPerSecond);
  }

  private static int getRowCount(Table table) throws IOException {
    int rowCount = 0;
    try (ResultScanner scanner = table.getScanner(new Scan())) {
      for (Result rs = scanner.next(); rs != null; rs = scanner.next()) {
        rowCount++;
      }
    }
    return rowCount;
  }

  private static void dropDatabaseIfExists(String dbName) throws SQLException {
    try (java.sql.Connection connection = getConnectionToRoot();
         Statement statement = connection.createStatement()) {
      statement.executeUpdate(String.format("drop database if exists %s", dbName));
    }
  }

  private static java.sql.Connection getConnectionToRoot() throws SQLException {
    String connectionString = String.format("jdbc:sqlserver://%s:%s", SQL_HOST, SQL_PORT);
    return DriverManager.getConnection(connectionString, SQL_USERNAME, SQL_PASSWORD);
  }

  private static void createDatabaseWithTracking(String dbName) throws SQLException {
    try (java.sql.Connection connection = getConnectionToRoot();
         Statement statement = connection.createStatement()) {
      statement.executeUpdate(String.format("create database %s", dbName));
      statement.executeUpdate(String.format("alter database %s set change_tracking = ON", dbName));
    }
  }

  private void createTableWithTracking(String tableName) throws SQLException {
    try (java.sql.Connection connection = getConnectionToDb();
         Statement statement = connection.createStatement()) {
      statement.executeUpdate(String.format("create table %s (id bigint identity primary key, value text)",
                                            tableName));
      statement.executeUpdate(String.format("alter table %s enable change_tracking", tableName));
    }
  }

  private java.sql.Connection getConnectionToDb() throws SQLException {
    String connectionString = String.format("jdbc:sqlserver://%s:%s;DatabaseName=%s", SQL_HOST, SQL_PORT, dbName);
    return DriverManager.getConnection(connectionString, SQL_USERNAME, SQL_PASSWORD);
  }
}
