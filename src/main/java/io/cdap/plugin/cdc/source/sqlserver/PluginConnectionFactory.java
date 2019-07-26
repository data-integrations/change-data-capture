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

import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.plugin.cdc.common.DBUtils;
import org.apache.spark.rdd.JdbcRDD;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Map;

/**
 * Serializable jdbc connection factory that uses CDAP plugin context to instantiate the jdbc driver.
 */
public class PluginConnectionFactory implements JdbcRDD.ConnectionFactory, Serializable {
  private static final long serialVersionUID = -7897960584858589314L;
  private final String stageName;
  private final String connectionString;
  private final PluginContext pluginContext;
  private transient String user;
  private transient String password;
  private transient boolean initialized;

  PluginConnectionFactory(PluginContext pluginContext, String stageName, String connectionString) {
    this.stageName = stageName;
    this.connectionString = connectionString;
    this.pluginContext = pluginContext;
  }

  @Override
  public Connection getConnection() throws Exception {
    if (!initialized) {
      Class<? extends Driver> driverClass = pluginContext.loadPluginClass(stageName + ":" + CTSQLServer.JDBC_PLUGIN_ID);
      DBUtils.ensureJDBCDriverIsAvailable(driverClass, connectionString);
      Map<String, String> stageProperties = pluginContext.getPluginProperties(stageName).getProperties();
      user = stageProperties.get(CTSQLServerConfig.USERNAME);
      password = stageProperties.get(CTSQLServerConfig.PASSWORD);
      initialized = true;
    }
    return DriverManager.getConnection(connectionString, user, password);
  }

  private void initialize() {

  }
}
