package co.cask.cdc.plugins.source.sqlserver;

import co.cask.cdap.api.data.format.StructuredRecord;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * A {@link InputDStream} which reads chnage tracking data from SQL Server and emits {@link StructuredRecord}
 */
public class CTInputDStream extends InputDStream<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(CTInputDStream.class);
  private ClassTag<StructuredRecord> tag;
  private String connection;
  private String username;
  private String password;
  private SQLServerConnection dbConnection;
  private long trackingOffset;

  CTInputDStream(StreamingContext ssc, ClassTag<StructuredRecord> tag, String connection, String username,
                 String password, long trackingOffset) {
    super(ssc, tag);
    this.tag = tag;
    this.connection = connection;
    this.username = username;
    this.password = password;
    this.trackingOffset = trackingOffset;
  }

  CTInputDStream(StreamingContext ssc, ClassTag<StructuredRecord> tag, String connection, String username,
                 String password) {
    super(ssc, tag);
    this.tag = tag;
    this.connection = connection;
    this.username = username;
    this.password = password;
    // if not current tracking version is given initialize it to 0
    this.trackingOffset = 0;
  }

  @Override
  public Option<RDD<StructuredRecord>> compute(Time validTime) {
    // get the table information of all tables which have ct enabled.
    List<TableInformation> tableInformations = getCTEnabledTables();

    List<RDD<StructuredRecord>> changeRDDs = new LinkedList<>();

    // Get the schema of tables. We get the schema of tables every microbatch because we want to update  the downstream
    // dataset with the DDL changes if any.
    for (TableInformation tableInformation : tableInformations) {
      changeRDDs.add(getColumnns(tableInformation));
    }

    // retrieve the current highest tracking version
    long prev = trackingOffset;
    long cur = 0;
    try {
      cur = getCurrentTrackingVersion(dbConnection.apply());
    } catch (SQLException e) {
      e.printStackTrace();
    }

    // get all the data changes (DML) for the ct enabled tables till current tracking version
    for (TableInformation tableInformation : tableInformations) {
      changeRDDs.add(getChangeData(tableInformation, prev, cur));
    }

    // union the above ddl (Schema) and dml (data changes) rdd
    // The union does not maintain any order so its possible that dml changes might be before updated ddl (schema)
    // changes. Downstream sinks expects to see DDL before DML so order the rdd such that all DDL (schema) changes of
    // this microbatch appear before DML. Its the caller's responsibility to order it in this way
    RDD<StructuredRecord> changes = ssc().sc().union(JavaConversions.asScalaBuffer(changeRDDs), tag);
    // update the tracking version
    trackingOffset = cur;
    return Option.apply(changes);
  }

  @Override
  public void start() {
    dbConnection = new SQLServerConnection(connection, username, password);
  }

  @Override
  public void stop() {
    // no-op
    // Also no need to close the dbconnection as JdbcRDD takes care of closing it
  }

  private RDD<StructuredRecord> getChangeData(TableInformation tableInformation, long prev, long cur) {

    String stmt = String.format("SELECT [CT].[SYS_CHANGE_VERSION], [CT].[SYS_CHANGE_CREATION_VERSION], " +
                                  "[CT].[SYS_CHANGE_OPERATION], %s, %s FROM [%s] as [CI] RIGHT OUTER JOIN " +
                                  "CHANGETABLE (CHANGES [%s], %s) as [CT] on %s where [CT]" +
                                  ".[SYS_CHANGE_VERSION] > ? " +
                                  "and [CT].[SYS_CHANGE_VERSION] <= ? ORDER BY [CT]" +
                                  ".[SYS_CHANGE_VERSION]",
                                getSelectColumns("CT", tableInformation.getPrimaryKeys()),
                                getSelectColumns("CI", tableInformation.getValueColumnNames()),
                                tableInformation.getName(), tableInformation.getName(), prev, getJoinCondition
                                  (tableInformation.getPrimaryKeys()));

    LOG.info("Querying for change data with statement {}", stmt);

    //TODO Currently we are not partitioning the data. We should partition it for scalability
    return new JdbcRDD<>(ssc().sc(), dbConnection, stmt, prev, cur, 1,
                         new ResultSetToDMLRecord(tableInformation),
                         ClassManifestFactory$.MODULE$.fromClass(StructuredRecord.class));
  }

  private long getCurrentTrackingVersion(Connection connection) throws SQLException {
    ResultSet resultSet = connection.createStatement().executeQuery("SELECT CHANGE_TRACKING_CURRENT_VERSION()");
    long changeVersion = 0;
    while (resultSet.next()) {
      LOG.info("Current tracking version is {}", changeVersion);
      changeVersion = resultSet.getLong(1);
    }
    connection.close();
    return changeVersion;
  }

  private JdbcRDD<StructuredRecord> getColumnns(TableInformation tableInformation) {
    String stmt = String.format("SELECT TOP 1 * FROM [%s].[%s] where ?=?", tableInformation.getSchemaName(),
                                tableInformation.getName());

    return new JdbcRDD<>(ssc().sc(), dbConnection, stmt, 1, 1, 1,
                         new ResultSetToDDLRecord(tableInformation.getSchemaName(), tableInformation.getName()),
                         ClassManifestFactory$.MODULE$.fromClass(StructuredRecord.class));
  }

  private Set<String> getColumnns(Connection connection, String schema, String table) throws SQLException {
    String query = String.format("SELECT * from [%s].[%s]", schema, table);
    Statement statement = connection.createStatement();
    statement.setMaxRows(1);
    ResultSet resultSet = statement.executeQuery(query);
    ResultSetMetaData metadata = resultSet.getMetaData();
    Set<String> columns = new LinkedHashSet<>();
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      columns.add(metadata.getColumnName(i));
    }
    return columns;
  }

  private Set<String> getKeyColumns(Connection connection, String schema, String table) throws SQLException {
    String stmt = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE " +
      "OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA+'.'+CONSTRAINT_NAME), 'IsPrimaryKey') = 1 AND " +
      "TABLE_SCHEMA = ? AND TABLE_NAME = ?";
    Set<String> keyColumns = new LinkedHashSet<>();
    try (PreparedStatement primaryKeyStatement = connection.prepareStatement(stmt)) {
      primaryKeyStatement.setString(1, schema);
      primaryKeyStatement.setString(2, table);
      try (ResultSet resultSet = primaryKeyStatement.executeQuery()) {
        while (resultSet.next()) {
          keyColumns.add(resultSet.getString(1));
        }
      }
    }
    return keyColumns;
  }

  private List<TableInformation> getCTEnabledTables() {
    Connection connection = dbConnection.apply();
    List<TableInformation> tableInformations = new LinkedList<>();
    String stmt = "SELECT s.name as schema_name, t.name AS table_name, ctt.* FROM sys.change_tracking_tables ctt " +
      "INNER JOIN sys.tables t on t.object_id = ctt.object_id INNER JOIN sys.schemas s on s.schema_id = t.schema_id";
    try {
      ResultSet rs = connection.createStatement().executeQuery(stmt);
      while (rs.next()) {
        String schemaName = rs.getString("schema_name");
        String tableName = rs.getString("table_name");
        tableInformations.add(new TableInformation(schemaName, tableName,
                                                   getColumnns(connection, schemaName, tableName),
                                                   getKeyColumns(connection, schemaName, tableName)));
      }
      connection.close();
      return tableInformations;
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    }
  }

  private static String getJoinCondition(Set<String> keyColumns) {
    StringBuilder joinCondition = new StringBuilder();
    for (String keyColumn : keyColumns) {
      if (joinCondition.length() > 0) {
        joinCondition.append(" AND ");
      }
      joinCondition.append(String.format("[CT].[%s] = [CI].[%s]", keyColumn, keyColumn));
    }
    return joinCondition.toString();
  }

  private String getSelectColumns(String tableName, Collection<String> cols) {
    List<String> selectColumns = new ArrayList<>(cols.size());
    for (String columns : cols) {
      selectColumns.add(String.format("[%s].[%s]", tableName, columns));
    }
    return Joiner.on(", ").join(selectColumns);
  }
}
