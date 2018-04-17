package co.cask.cdc.plugins.source.sqlserver;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.common.ReferencePluginConfig;

import javax.annotation.Nullable;

/**
 * Defines the {@link PluginConfig} for the {@link CTSQLServer}.
 */
public class ConnectionConfig extends ReferencePluginConfig {


  public static final String HOST_NAME = "hostname";
  public static final String PORT = "port";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String DATABASE_NAME = "dbname";

  @Name(HOST_NAME)
  @Description("SQL Server hostname. Ex: mysqlsever.net")
  @Macro
  public String hostname;

  @Name(PORT)
  @Description("SQL Server port. Defaults to 1433")
  @Macro
  public int port;

  @Name(DATABASE_NAME)
  @Description("SQL Server database name. Note: CT must be enabled on the database for change tracking.")
  @Macro
  public String dbName;

  @Name(USERNAME)
  @Description("User to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  public String username;

  @Name(PASSWORD)
  @Description("Password to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  @Macro
  public String password;

  public ConnectionConfig() {
    super("");
    port = 1433;
    username = null;
    password = null;
  }

  public ConnectionConfig(String referenceName, String hostname, int port, String dbName, String username,
                          String password) {
    super(referenceName);
    this.hostname = hostname;
    this.port = port;
    this.dbName = dbName;
    this.username = username;
    this.password = password;
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
}
