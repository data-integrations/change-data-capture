package co.cask.cdc.plugins.integration.sink;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.mock.spark.streaming.MockSource;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.test.SparkManager;
import co.cask.cdc.plugins.common.OperationType;
import co.cask.cdc.plugins.common.Schemas;
import co.cask.cdc.plugins.integration.CDCPluginTestBase;
import co.cask.cdc.plugins.sink.CDCBigTableConfig;
import co.cask.cdc.plugins.sink.CDCTableUtil;
import co.cask.hydrator.common.Constants;
import com.google.bigtable.repackaged.com.google.cloud.ServiceOptions;
import com.google.bigtable.repackaged.io.grpc.StatusRuntimeException;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RunWith(Enclosed.class)
public class CDCBigTablePluginTest {
  private static final String PLUGIN_NAME = "CDCBigTable";
  private static final String APP_NAME = CDCBigTablePluginTest.class.getSimpleName();
  private static final String PROJECT
    = System.getProperty("test.bigtable.project", ServiceOptions.getDefaultProjectId());
  private static final String INSTANCE = System.getProperty("test.bigtable.instance");
  private static final String SERVICE_ACCOUNT_FILE_PATH
    = System.getProperty("test.bigtable.serviceFilePath", System.getenv("CREDENTIAL_ENV_NAME"));
  private static final String DB_NAMESPACE = "dbNamespace";

  private static void dropTableIfExists(Connection connection, String dbTableName) throws IOException {
    TableName tableName = TableName.valueOf(dbTableName);
    if (connection.getAdmin().tableExists(tableName)) {
      connection.getAdmin().disableTable(tableName);
      connection.getAdmin().deleteTable(tableName);
    }
  }

  private static byte[] getColumnValue(Result result, String column) {
    return result.getValue(Bytes.toBytes(CDCTableUtil.CDC_COLUMN_FAMILY), Bytes.toBytes(column));
  }

  public static class Validations extends CDCPluginTestBase {
    @Rule
    public TestName testName = new TestName();

    @Before
    public void beforeMethod() {
      Assume.assumeNotNull(PROJECT);
      Assume.assumeNotNull(INSTANCE);
    }

    @Test
    public void testDeploymentFailedWhenCredentialsFileIsMissing() throws Exception {
      ETLPlugin sourceConfig = MockSource.getPlugin(Schemas.CHANGE_SCHEMA, Collections.emptyList());

      Map<String, String> props = ImmutableMap.<String, String>builder()
        .put(CDCBigTableConfig.PROJECT, PROJECT)
        .put(CDCBigTableConfig.INSTANCE, INSTANCE)
        .put(CDCBigTableConfig.SERVICE_ACCOUNT_FILE_PATH, "/tmp/non_existing_file")
        .put(Constants.Reference.REFERENCE_NAME, "CDCBigTableSink")
        .build();
      ETLPlugin sinkConfig = new ETLPlugin(PLUGIN_NAME, SparkSink.PLUGIN_TYPE, props);

      Assertions.assertThatThrownBy(() -> deployETL(sourceConfig, sinkConfig, APP_NAME))
        .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testDeploymentFailedWhenProjectIsNotDefined() throws Exception {
      ETLPlugin sourceConfig = MockSource.getPlugin(Schemas.CHANGE_SCHEMA, Collections.emptyList());

      Map<String, String> props = ImmutableMap.<String, String>builder()
        .put(CDCBigTableConfig.INSTANCE, INSTANCE)
        .put(CDCBigTableConfig.SERVICE_ACCOUNT_FILE_PATH, SERVICE_ACCOUNT_FILE_PATH)
        .put(Constants.Reference.REFERENCE_NAME, "CDCBigTableSink")
        .build();
      ETLPlugin sinkConfig = new ETLPlugin(PLUGIN_NAME, SparkSink.PLUGIN_TYPE, props);

      Assertions.assertThatThrownBy(() -> deployETL(sourceConfig, sinkConfig, APP_NAME))
        .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testDeploymentFailedWhenInstanceIsNotDefined() throws Exception {
      ETLPlugin sourceConfig = MockSource.getPlugin(Schemas.CHANGE_SCHEMA, Collections.emptyList());

      Map<String, String> props = ImmutableMap.<String, String>builder()
        .put(CDCBigTableConfig.PROJECT, PROJECT)
        .put(CDCBigTableConfig.SERVICE_ACCOUNT_FILE_PATH, SERVICE_ACCOUNT_FILE_PATH)
        .put(Constants.Reference.REFERENCE_NAME, "CDCBigTableSink")
        .build();
      ETLPlugin sinkConfig = new ETLPlugin(PLUGIN_NAME, SparkSink.PLUGIN_TYPE, props);

      Assertions.assertThatThrownBy(() -> deployETL(sourceConfig, sinkConfig, APP_NAME))
        .isInstanceOf(IllegalStateException.class);
    }
  }

  public static class SuccessFlow extends CDCPluginTestBase {

    private static final Duration TEST_TIMEOUT = new Duration(3, TimeUnit.MINUTES);

    @Rule
    public TestName testName = new TestName();

    private String dbTableName;
    private SparkManager programManager;
    private ETLPlugin sinkConfig;
    private Connection connection;

    @Before
    public void setUp() throws Exception {
      Assume.assumeNotNull(PROJECT);
      Assume.assumeNotNull(INSTANCE);

      dbTableName = testName.getMethodName();

      Map<String, String> props = ImmutableMap.<String, String>builder()
        .put(CDCBigTableConfig.PROJECT, PROJECT)
        .put(CDCBigTableConfig.INSTANCE, INSTANCE)
        .put(CDCBigTableConfig.SERVICE_ACCOUNT_FILE_PATH, SERVICE_ACCOUNT_FILE_PATH)
        .put(Constants.Reference.REFERENCE_NAME, "CDCBigTableSink")
        .build();
      sinkConfig = new ETLPlugin(PLUGIN_NAME, SparkSink.PLUGIN_TYPE, props);

      Configuration configuration = BigtableConfiguration.configure(PROJECT, INSTANCE);
      if (SERVICE_ACCOUNT_FILE_PATH != null) {
        configuration.set(BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY,
                          SERVICE_ACCOUNT_FILE_PATH);
      }
      connection = BigtableConfiguration.connect(configuration);

      dropTableIfExists(connection, dbTableName);
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
      if (connection != null) {
        dropTableIfExists(connection, dbTableName);
        connection.close();
      }
    }

    @Test
    public void testHandleDDLRecord() throws Exception {
      Schema tableSchema = Schema.recordOf(
        Schemas.SCHEMA_RECORD,
        Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
        Schema.Field.of("value", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
      );
      StructuredRecord ddlRecord = StructuredRecord.builder(Schemas.DDL_SCHEMA)
        .set(Schemas.TABLE_FIELD, Joiner.on(".").join(DB_NAMESPACE, dbTableName))
        .set(Schemas.SCHEMA_FIELD, tableSchema.toString())
        .build();

      List<StructuredRecord> input = Stream.of(ddlRecord)
        .map(Schemas::toCDCRecord)
        .collect(Collectors.toList());

      ETLPlugin sourceConfig = MockSource.getPlugin(Schemas.CHANGE_SCHEMA, input);

      programManager = deployETL(sourceConfig, sinkConfig, APP_NAME);
      programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

      Awaitility.await().atMost(TEST_TIMEOUT).pollInterval(Duration.ONE_SECOND).untilAsserted(() -> {
        TableName tableName = TableName.valueOf(dbTableName);
        Assertions.assertThat(connection.getAdmin().tableExists(tableName))
          .as("Table '%s' was not created", tableName)
          .isTrue();
        Assertions.assertThat(connection.getAdmin().isTableAvailable(tableName)).isTrue();
        Assertions.assertThat(connection.getAdmin().isTableEnabled(tableName)).isTrue();
      });
    }

    @Test
    public void testHandleMultipleInserts() throws Exception {
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
          .put("id", 1L)
          .put("value", "val 1")
          .build())
        .build();
      StructuredRecord dmlRecord2 = StructuredRecord.builder(Schemas.DML_SCHEMA)
        .set(Schemas.TABLE_FIELD, Joiner.on(".").join(DB_NAMESPACE, dbTableName))
        .set(Schemas.PRIMARY_KEYS_FIELD, primaryKeys)
        .set(Schemas.OP_TYPE_FIELD, OperationType.INSERT.name())
        .set(Schemas.UPDATE_SCHEMA_FIELD, tableSchema.toString())
        .set(Schemas.UPDATE_VALUES_FIELD, ImmutableMap.<String, Object>builder()
          .put("id", 2L)
          .put("value", "val 2")
          .build())
        .build();

      List<StructuredRecord> input = Stream.of(ddlRecord, dmlRecord1, dmlRecord2)
        .map(Schemas::toCDCRecord)
        .collect(Collectors.toList());

      ETLPlugin sourceConfig = MockSource.getPlugin(Schemas.CHANGE_SCHEMA, input);

      programManager = deployETL(sourceConfig, sinkConfig, APP_NAME);
      programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

      Awaitility.await().atMost(TEST_TIMEOUT).pollInterval(Duration.ONE_SECOND)
        .ignoreException(StatusRuntimeException.class).untilAsserted(() -> {
        TableName tableName = TableName.valueOf(dbTableName);
        Table table = connection.getTable(tableName);
        Result result1 = table.get(new Get(Bytes.toBytes("1")));
        Assertions.assertThat(result1.isEmpty()).isFalse();
        Assertions.assertThat(Bytes.toString(getColumnValue(result1, "value")))
          .isEqualTo("val 1");
        Result result2 = table.get(new Get(Bytes.toBytes("2")));
        Assertions.assertThat(result2.isEmpty()).isFalse();
        Assertions.assertThat(Bytes.toString(getColumnValue(result2, "value")))
          .isEqualTo("val 2");
      });
    }

    @Test
    public void testHandleInsertUpdateOperations() throws Exception {
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

      List<StructuredRecord> input = Stream.of(ddlRecord, dmlRecord1, dmlRecord2)
        .map(Schemas::toCDCRecord)
        .collect(Collectors.toList());

      ETLPlugin sourceConfig = MockSource.getPlugin(Schemas.CHANGE_SCHEMA, input);

      programManager = deployETL(sourceConfig, sinkConfig, APP_NAME);
      programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

      Awaitility.await().atMost(TEST_TIMEOUT).pollInterval(Duration.ONE_SECOND)
        .ignoreException(StatusRuntimeException.class).untilAsserted(() -> {
        TableName tableName = TableName.valueOf(dbTableName);
        Table table = connection.getTable(tableName);
        Result result1 = table.get(new Get(Bytes.toBytes("1")));
        Assertions.assertThat(result1.isEmpty()).isFalse();
        Assertions.assertThat(Bytes.toString(getColumnValue(result1, "value")))
          .isEqualTo("updated value");
      });
    }

    @Test
    public void testHandleInsertDeleteOperations() throws Exception {
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
        .set(Schemas.OP_TYPE_FIELD, OperationType.DELETE.name())
        .set(Schemas.UPDATE_SCHEMA_FIELD, tableSchema.toString())
        .set(Schemas.UPDATE_VALUES_FIELD, ImmutableMap.<String, Object>builder()
          .put("id", 1)
          .put("value", "val 1")
          .build())
        .build();

      List<StructuredRecord> input = Stream.of(ddlRecord, dmlRecord1, dmlRecord2, dmlRecord3)
        .map(Schemas::toCDCRecord)
        .collect(Collectors.toList());

      ETLPlugin sourceConfig = MockSource.getPlugin(Schemas.CHANGE_SCHEMA, input);

      programManager = deployETL(sourceConfig, sinkConfig, APP_NAME);
      programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

      Awaitility.await().atMost(TEST_TIMEOUT).pollInterval(Duration.ONE_SECOND)
        .ignoreException(StatusRuntimeException.class).untilAsserted(() -> {
        TableName tableName = TableName.valueOf(dbTableName);
        Table table = connection.getTable(tableName);
        Result result2 = table.get(new Get(Bytes.toBytes("2")));
        Assertions.assertThat(result2.isEmpty()).isFalse();
        Assertions.assertThat(Bytes.toString(getColumnValue(result2, "value")))
          .isEqualTo("val 2");
        Result result1 = table.get(new Get(Bytes.toBytes("1")));
        Assertions.assertThat(result1.isEmpty()).isTrue();
      });
    }
  }
}
