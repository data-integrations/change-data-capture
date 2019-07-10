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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.cdc.common.Schemas;
import io.cdap.plugin.common.Constants;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Streaming source for reading changes from SQL Server.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("CTSQLServer")
@Description("SQL Server Change Tracking Streaming Source")
public class CTSQLServer extends StreamingSource<StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(CTSQLServer.class);
  private final CTSQLServerConfig conf;
  private final boolean requireSeqNumber;
  private long offset;
  private final String tableName;
  private final int maxBatchSize;

  public CTSQLServer(CTSQLServerConfig conf) {
    this.conf = conf;
    requireSeqNumber = conf.getSqn();
    offset = conf.getCdcnumber();
    tableName = conf.getTableName();
    maxBatchSize = conf.getMaxBatchSize();
    LOG.debug("requireSeqNumber" + requireSeqNumber + " ==>" + Boolean.toString(requireSeqNumber));
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    conf.validate();
    pipelineConfigurer.createDataset(conf.referenceName, Constants.EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(Schemas.CHANGE_SCHEMA);
    Connection connection;
    try {
      Class.forName(conf.getDriverClassName());
      if (conf.getUsername() != null && conf.getPassword() != null) {
        LOG.info("Creating connection with url {}, username {} ", getConnectionString(), conf.getUsername());
        connection = DriverManager.getConnection(getConnectionString(),
          conf.getUsername(), conf.getPassword());

      } else {
        LOG.info("Creating connection with url {}", getConnectionString());
        connection = DriverManager.getConnection(getConnectionString(), null, null);
      }
    } catch (Exception e) {
      if (e instanceof SQLException) {
        LOG.error("Failed to establish connection with SQL Server with the given configuration.");
      }
      throw new InvalidStageException(e.toString(), e);
    }
    try {
      checkDBCTEnabled(connection, conf.getDbName());
    } catch (InvalidStageException e) {
      throw e;
    } catch (Exception e) {
      throw new InvalidStageException(String.format("Failed to check tracking status. Error: %s", e.getMessage()), e);
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    context.registerLineage(conf.referenceName);
    try {
      Class.forName(conf.getDriverClassName());
      if (conf.getUsername() != null && conf.getPassword() != null) {
        LOG.info("Creating connection with url {}, username {}, " +
          "password *****", getConnectionString(), conf.getUsername());
        DriverManager.getConnection(getConnectionString(),
          conf.getUsername(), conf.getPassword());
      } else {
        LOG.info("Creating connection with url {}", getConnectionString());
        DriverManager.getConnection(getConnectionString(), null, null);
      }
    } catch (Exception e) {
      if (e instanceof SQLException) {
        LOG.error("Failed to establish connection with SQL Server with the given configuration.");
      }
      throw e;
    }
    // get change information dtream. This dstream has both schema and data changes
    LOG.info("Creating change information dstream");
    ClassTag<StructuredRecord> tag = ClassTag$.MODULE$.<StructuredRecord>apply(StructuredRecord.class);
    CTInputDStream dstream = new CTInputDStream(context.getSparkStreamingContext().ssc(),
      new SQLServerConnection(conf.getConnectionString(),
        conf.getUsername(), conf.getPassword(),
        conf.getDriverClassName()),
      getConnectionString(), conf.getUsername(), conf.getPassword(),
      requireSeqNumber, offset, tableName, maxBatchSize);
    return JavaDStream.fromDStream(dstream, tag)
      .mapToPair(structuredRecord -> new Tuple2<>("", structuredRecord))
      .mapWithState(StateSpec.function(schemaStateFunction()))
      .map(Schemas::toCDCRecord);
  }

  private void checkDBCTEnabled(Connection connection, String dbName) throws SQLException {
    String query = "SELECT * FROM sys.change_tracking_databases WHERE database_id=DB_ID(?)";
    try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
      preparedStatement.setString(1, dbName);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        if (resultSet.next()) {
          // if resultset is not empty it means that our select with where clause returned data meaning ct is enabled.
          return;
        }
      }
    }
    throw new InvalidStageException(String.format("Change Tracking is not enabled on the specified database '%s'." +
      " Please enable it first.", dbName));
  }

  private String getConnectionString() {
    return conf.getConnectionString();
  }

  private static Function4<Time, String, Optional<StructuredRecord>, State<Map<String, String>>,
    Optional<StructuredRecord>> schemaStateFunction() {
    return (time, key, value, state) -> {
      if (!value.isPresent()) {
        return Optional.empty();
      }
      StructuredRecord input = value.get();
      // for dml record we don't need to maintain any state so skip it
      if (Schemas.DML_SCHEMA.getRecordName().equals(input.getSchema().getRecordName())) {
        return Optional.of(input);
      }

      // we know now that its a ddl record so process it
      String tableName = input.get(Schemas.TABLE_FIELD);
      String tableSchemaStructure = input.get(Schemas.SCHEMA_FIELD);
      Map<String, String> newState;
      if (state.exists()) {
        newState = state.get();
        if (newState.containsKey(tableName) && newState.get(tableName).equals(tableSchemaStructure)) {
          // schema hasn't changed so emit with false so that we can later filter this record out
          return Optional.empty();
        }
      } else {
        newState = new HashMap<>();
      }
      // update the state
      newState.put(tableName, tableSchemaStructure);
      state.update(newState);
      LOG.debug("Update schema state store for table {}. New schema will be emitted.", tableName);
      return Optional.of(input);
    };
  }
}
