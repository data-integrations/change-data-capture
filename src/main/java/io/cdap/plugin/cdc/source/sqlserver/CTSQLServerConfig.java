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
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.cdc.common.CDCReferencePluginConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Defines the {@link PluginConfig} for the {@link CTSQLServer}.
 */
public class CTSQLServerConfig extends CDCReferencePluginConfig {

  public static final String HOST_NAME = "hostname";
  public static final String PORT = "port";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String DATABASE_NAME = "dbname";
  public static final String SEQUENCE_START_NUM = "sequenceStartNum";
  public static final String MAX_RETRY_SECONDS = "maxRetrySeconds";
  public static final String MAX_BATCH_SIZE = "maxBatchSize";
  public static final String TABLE_WHITELIST = "tableWhitelist";
  public static final String JDBC_PLUGIN_NAME = "jdbcPluginName";
  public static final String CONNECTION_STRING = "connectionString";
  public static final String OPERATIONS_LIST = "operationsList";
  public static final String RETENTION_DAYS = "retentionDays";
  public static final String RETENTION_COLUMN = "retentionColumn";

  @Name(HOST_NAME)
  @Description("SQL Server hostname. This is not required if a connection string was specified.")
  @Nullable
  private final String hostname;

  @Name(PORT)
  @Description("SQL Server port. This is not required if a connection string was specified.")
  @Nullable
  private final Integer port;

  @Name(DATABASE_NAME)
  @Description("SQL Server database name. Note: CT must be enabled on the database for change tracking.")
  @Nullable
  private String dbName;

  @Name(USERNAME)
  @Description("User to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  private final String username;

  @Name(PASSWORD)
  @Description("Password to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  private final String password;

  @Name(MAX_RETRY_SECONDS)
  @Description("Maximum amount of time to retry reading change events if there is an error. "
    + "If no retries should be done, this should be set to 0. "
    + "If there should not be a retry limit, this should be set to a negative number or left empty.")
  @Nullable
  private final Long maxRetrySeconds;

  @Name(SEQUENCE_START_NUM)
  @Description("The Change Tracking sequence number to start from.")
  @Nullable
  private final Long sequenceStartNum;

  @Name(MAX_BATCH_SIZE)
  @Description("Maximum number of changes to consume in a single batch interval.")
  @Nullable
  private final Integer maxBatchSize;

  @Name(TABLE_WHITELIST)
  @Description("A whitelist of tables to consume changes from. "
    + "If none is specified, changes from all tables will be consumed.")
  @Nullable
  private final String tableWhitelist;

  @Description("Name of the JDBC plugin to use if something different than the built-in sql server driver is required.")
  @Nullable
  private final String jdbcPluginName;

  @Description("Connection string to use when connecting to the database through JDBC. "
    + "This is required if a JDBC plugin was specified.")
  @Nullable
  private final String connectionString;

  @Name(OPERATIONS_LIST)
  @Description("The list of operations to read (INSERT,UPDATE,DELETE)")
  @Nullable
  private final String operationsList;

  @Name(RETENTION_DAYS)
  @Description("Retention period for a table if DELETE operation is turned off. " +
          "In this period all deletes will be imported. Set to 0 to ignore all deletes")
  @Nullable
  private final int retentionDays;

  @Name(RETENTION_COLUMN)
  @Description("Timestamp column to use for retention period (should be a part of Primary Key)")
  @Nullable
  private final String retentionColumn;

  public CTSQLServerConfig() {
    super("");
    this.hostname = null;
    this.port = 1433;
    this.dbName = null;
    this.username = null;
    this.password = null;
    this.sequenceStartNum = 0L;
    this.maxRetrySeconds = -1L;
    this.maxBatchSize = 100000;
    this.tableWhitelist = null;
    this.jdbcPluginName = null;
    this.connectionString = null;
    this.operationsList = null;
    this.retentionDays = 0;
    this.retentionColumn = null;
  }

  public String getHostname() {
    return hostname;
  }

  public int getPort() {
    return port;
  }

  public String getDbName() {
    return dbName;
  }

  @Nullable
  public String getUsername() {
    return username;
  }

  @Nullable
  public String getPassword() {
    return password;
  }

  public long getSequenceStartNum() {
    return sequenceStartNum == null ? 0L : sequenceStartNum;
  }

  public long getMaxRetrySeconds() {
    return maxRetrySeconds == null ? -1L : maxRetrySeconds;
  }

  public int getMaxBatchSize() {
    return maxBatchSize == null ? 100000 : maxBatchSize;
  }

  public Set<String> getTableWhitelist() {
    return tableWhitelist == null ? Collections.emptySet() :
      Arrays.stream(tableWhitelist.split(",")).map(String::trim).collect(Collectors.toSet());
  }

  @Nullable
  public String getJdbcPluginName() {
    return jdbcPluginName;
  }

  public String getConnectionString() {
    if (connectionString != null) {
      return connectionString;
    }
    return String.format("jdbc:sqlserver://%s:%s;DatabaseName=%s", hostname, port, dbName);
  }

  public Set<String> getOperationsList() {
    return operationsList == null ? Collections.emptySet() :
            Arrays.stream(operationsList.split(",")).map(String::trim).collect(Collectors.toSet());
  }


  public int getRetentionDays() {
    return retentionDays;
  }

  @Nullable
  public String getRetentionColumn() {
    return retentionColumn;
  }

  @Override
  public void validate() {
    super.validate();
    if (jdbcPluginName != null && connectionString == null) {
      throw new InvalidConfigPropertyException(
        "A connection string must be specified when a custom jdbc driver is used.", CONNECTION_STRING);
    }

    if (dbName == null) {
      throw new InvalidConfigPropertyException("A database name must be specified", DATABASE_NAME);
    }

    if (connectionString == null) {
      if (hostname == null) {
        throw new InvalidConfigPropertyException("A hostname must be specified", HOST_NAME);
      }
      if (port == null) {
        throw new InvalidConfigPropertyException("A port must be specified", PORT);
      }
    }

    if (port != null && (port < 0 || port > 65535)) {
      throw new InvalidConfigPropertyException("Port number should be in range 0-65535", PORT);
    }

    if (operationsList == null || operationsList.length() == 0) {
      throw new InvalidConfigPropertyException("Operations list couldn't be empty", OPERATIONS_LIST);
    }

    if (retentionDays > 0 && retentionColumn == null) {
      throw new InvalidConfigPropertyException("Please set the retention timestamp column", RETENTION_COLUMN);
    }

  }
}
