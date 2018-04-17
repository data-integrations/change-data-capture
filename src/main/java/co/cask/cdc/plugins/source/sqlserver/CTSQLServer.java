package co.cask.cdc.plugins.source.sqlserver;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import com.google.common.base.Optional;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("CTSQLServer")
@Description("SQL Server Change Tracking Streaming Source")
public class CTSQLServer extends StreamingSource<StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(CTSQLServer.class);
  private final ConnectionConfig conf;

  public CTSQLServer(ConnectionConfig conf) {
    this.conf = conf;
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws Exception {

    Connection connection;
    try {
      Class.forName(SQLServerDriver.class.getName());
      if (conf.username != null && conf.password != null) {
        LOG.info("Creating connection with url {}, username {}, password *****", getConnectionString(), conf.username);
        connection = DriverManager.getConnection(getConnectionString(), conf.username, conf.password);
      } else {
        LOG.info("Creating connection with url {}", getConnectionString());
        connection = DriverManager.getConnection(getConnectionString(), null, null);
      }
    } catch (Exception e) {
      if (e instanceof SQLException) {
        LOG.error("Failed to establish connection with SQL Server with the given configuration.");
      }
      throw e;
    }

    // check that CDC is enabled on the database
    checkDBCTEnabled(connection, conf.dbName);

    // get change information dtream. This dstream has both schema and data changes
    LOG.info("Creating change information dstream");
    ClassTag<StructuredRecord> tag = scala.reflect.ClassTag$.MODULE$.apply(StructuredRecord.class);
    JavaDStream<StructuredRecord> changeDStream =
      JavaDStream.fromDStream(new CTInputDStream(streamingContext.getSparkStreamingContext().ssc(), tag,
                                                 getConnectionString(), conf.username, conf
                                                   .password), tag);

    JavaPairDStream<String, StructuredRecord> pairedChangeDStream =
      changeDStream.mapToPair(new PairFunction<StructuredRecord, String, StructuredRecord>() {
        @Override
        public Tuple2<String, StructuredRecord> call(StructuredRecord structuredRecord) throws Exception {
          return new Tuple2<>("", structuredRecord);
        }
      });

    // map the dstream with schema state store to detect changes in schema
    JavaMapWithStateDStream<String, StructuredRecord, Map<String, String>,
      Tuple2<StructuredRecord, Boolean>> stateMappedChangeDStream =
      pairedChangeDStream.mapWithState(StateSpec.function(new SchemaStateFunction()));

    // filter out the ddl record whose schema hasn't changed and then drop all the keys
    JavaDStream<StructuredRecord> map = stateMappedChangeDStream.filter(new Function<Tuple2<StructuredRecord, Boolean>, Boolean>() {
      @Override
      public Boolean call(Tuple2<StructuredRecord, Boolean> v1) throws Exception {
        return v1._2();
      }
    }).map(new Function<Tuple2<StructuredRecord, Boolean>, StructuredRecord>() {
      @Override
      public StructuredRecord call(Tuple2<StructuredRecord, Boolean> v1) throws Exception {
        return v1._1();
      }
    });

    return map.mapToPair(new PairFunction<StructuredRecord, String, StructuredRecord>() {
      @Override
      public Tuple2<String, StructuredRecord> call(StructuredRecord structuredRecord) throws Exception {
        // key by record name DDLRecord or DMLRecord and the record
        return new Tuple2<>(structuredRecord.getSchema().getRecordName(), structuredRecord);
      }
    }).transformToPair(new Function<JavaPairRDD<String, StructuredRecord>,
      JavaPairRDD<String, StructuredRecord>>() {
      @Override
      public JavaPairRDD<String, StructuredRecord> call(JavaPairRDD<String, StructuredRecord> v1) throws Exception {
        // sort by key so that all DDLRecord comes first
        return v1.sortByKey();
      }
    }).map(new Function<Tuple2<String,
      StructuredRecord>, StructuredRecord>() {
      @Override
      public StructuredRecord call(Tuple2<String, StructuredRecord> v1) throws Exception {
        // drop the keys
        return v1._2();
      }
    });
  }

  private void checkDBCTEnabled(Connection connection, String dbName) throws SQLException {
    String query = "SELECT * FROM sys.change_tracking_databases WHERE database_id=DB_ID(?)";
    PreparedStatement preparedStatement = connection.prepareStatement(query);
    preparedStatement.setString(1, dbName);
    ResultSet resultSet = preparedStatement.executeQuery();
    if (resultSet.next()) {
      // if resultset is not empty it means that our select with where clause returned data meaning ct is enabled.
      return;
    }
    throw new RuntimeException(String.format("Change Tracking is not enabled on the specified database '%s'. Please " +
                                               "enable it first.", dbName));
  }

  private String getConnectionString() {
    return String.format("jdbc:sqlserver://%s:%s;DatabaseName=%s", conf.hostname, conf.port,
                         conf.dbName);
  }

  private static class SchemaStateFunction implements Function3<String, Optional<StructuredRecord>,
    State<Map<String, String>>, Tuple2<StructuredRecord, Boolean>> {
    @Override
    public Tuple2<StructuredRecord, Boolean> call(String v1, Optional<StructuredRecord> value, State<Map<String, String>>
      stateStore) throws Exception {
      StructuredRecord input = value.get();
      // for dml record we don't need to maintain any state so skip it
      if (input.getSchema().getRecordName().equalsIgnoreCase(ResultSetToDMLRecord.RECORD_NAME)) {
        return new Tuple2<>(value.get(), true);
      }

      // we know now that its a ddl record so process it
      String tableName = input.get("table");
      String tableSchemaStructure = input.get("schema");
      Map<String, String> state;
      if (stateStore.exists()) {
        state = stateStore.get();
        if (state.containsKey(tableName) && state.get(tableName).equals(tableSchemaStructure)) {
          // schema hasn't changed so emit with false so that we can later filter this record out
          return new Tuple2<>(value.get(), false);
        }
      } else {
        state = new HashMap<>();
      }
      // update the state
      state.put(tableName, tableSchemaStructure);
      stateStore.update(state);
      LOG.info("Update schema state store for table {}. New schema will be emitted.", tableName);
      return new Tuple2<>(value.get(), true);
    }
  }
}
