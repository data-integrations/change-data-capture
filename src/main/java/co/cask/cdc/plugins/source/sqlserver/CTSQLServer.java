package co.cask.cdc.plugins.source.sqlserver;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import com.google.common.base.Throwables;
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

import java.sql.Connection;
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
  private final SQLServerConnection dbConnection;

  public CTSQLServer(CTSQLServerConfig conf) {
    this.conf = conf;
    dbConnection = new SQLServerConnection(getConnectionString(), conf.getUsername(), conf.getPassword());
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    if (conf.getUsername() != null && conf.getPassword() != null) {
      LOG.info("Creating connection with url {}, username {}, password *****",
               getConnectionString(), conf.getUsername());
    } else {
      LOG.info("Creating connection with url {}", getConnectionString());
    }
    try (Connection connection = dbConnection.apply()) {
      // check that CDC is enabled on the database
      checkDBCTEnabled(connection, conf.getDbName());
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    // get change information dtream. This dstream has both schema and data changes
    LOG.info("Creating change information dstream");
    ClassTag<StructuredRecord> tag = scala.reflect.ClassTag$.MODULE$.apply(StructuredRecord.class);
    CTInputDStream dstream = new CTInputDStream(context.getSparkStreamingContext().ssc(), tag, dbConnection);
    return JavaDStream.fromDStream(dstream, tag)
      .mapToPair(structuredRecord -> new Tuple2<>("", structuredRecord))
      // map the dstream with schema state store to detect changes in schema
      // filter out the ddl record whose schema hasn't changed and then drop all the keys
      .mapWithState(StateSpec.function(schemaStateFunction()))
      // key by record name DDLRecord or DMLRecord and the record
      .mapToPair(record -> new Tuple2<>(record.getSchema().getRecordName(), record))
      // sort by key so that all DDLRecord comes first
      .transformToPair(pairRDD -> pairRDD.sortByKey())
      .map(Tuple2::_2);
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
    throw new IllegalArgumentException(String.format("Change Tracking is not enabled on the specified database '%s'." +
      " Please enable it first.", dbName));
  }

  private String getConnectionString() {
    return String.format("jdbc:sqlserver://%s:%s;DatabaseName=%s", conf.hostname, conf.port, conf.dbName);
  }

  private static Function4<Time, String, Optional<StructuredRecord>, State<Map<String, String>>,
      Optional<StructuredRecord>> schemaStateFunction() {
    return (time, key, value, state) -> {
      if (!value.isPresent()) {
        return Optional.empty();
      }
      StructuredRecord input = value.get();
      // for dml record we don't need to maintain any state so skip it
      if (ResultSetToDMLRecord.RECORD_NAME.equalsIgnoreCase(input.getSchema().getRecordName())) {
        return Optional.of(input);
      }

      // we know now that its a ddl record so process it
      String tableName = input.get("table");
      String tableSchemaStructure = input.get("schema");
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
