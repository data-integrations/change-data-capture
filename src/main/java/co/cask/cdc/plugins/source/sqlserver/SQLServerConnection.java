package co.cask.cdc.plugins.source.sqlserver;

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
