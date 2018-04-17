package co.cask.cdc.plugins.source.sqlserver;

import com.google.common.base.Throwables;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import scala.Serializable;
import scala.runtime.AbstractFunction0;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * A class which can provide a {@link Connection} using {@link SQLServerDriver} which is
 * serializable.
 * Note: This class does not do any connection management. Its the responsibility of the client
 * to manage/close the connection.
 */
class SQLServerConnection extends AbstractFunction0<Connection> implements Serializable {
  private String connectionUrl;
  private String userName;
  private String password;

  SQLServerConnection(String connectionUrl, String userName, String password) {
    this.connectionUrl = connectionUrl;
    this.userName = userName;
    this.password = password;
  }

  @Override
  public Connection apply() {
    try {
      Class.forName(SQLServerDriver.class.getName());
      Properties properties = new Properties();
      properties.setProperty("user", userName);
      properties.setProperty("password", password);
      return DriverManager.getConnection(connectionUrl, properties);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
