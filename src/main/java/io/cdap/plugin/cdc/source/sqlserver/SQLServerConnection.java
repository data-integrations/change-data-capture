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

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import org.apache.spark.rdd.JdbcRDD;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * A class which can provide a {@link Connection} using {@link SQLServerDriver} which is
 * serializable.
 * Note: This class does not do any connection management. Its the responsibility of the client
 * to manage/close the connection.
 */
class SQLServerConnection implements JdbcRDD.ConnectionFactory {
  private final String connectionUrl;
  private final String userName;
  private final String password;

  SQLServerConnection(String connectionUrl, String userName, String password) {
    this.connectionUrl = connectionUrl;
    this.userName = userName;
    this.password = password;
  }

  @Override
  public Connection getConnection() throws Exception {
    Class.forName(SQLServerDriver.class.getName());
    return DriverManager.getConnection(connectionUrl, userName, password);
  }
}
