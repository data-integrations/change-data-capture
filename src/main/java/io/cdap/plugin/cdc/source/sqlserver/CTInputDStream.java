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

import com.google.common.base.Throwables;
import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.reflect.ClassTag$;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A {@link InputDStream} which reads chnage tracking data from SQL Server and emits {@link StructuredRecord}
 */
public class CTInputDStream extends InputDStream<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(CTInputDStream.class);
  private final JdbcRDD.ConnectionFactory connectionFactory;
  private final long maxRetrySeconds;
  private final int maxBatchSize;
  private final Set<String> tableWhitelist;
  private long trackingOffset;
  private long failureStartTime;
  private boolean isFailing;
  private boolean forceFailure;
  private final Set<String> operationsList;

  CTInputDStream(StreamingContext ssc, JdbcRDD.ConnectionFactory connectionFactory, Set<String> operationsList,
                 Set<String> tableWhitelist, long startingOffset, long maxRetrySeconds, int maxBatchSize) {
    super(ssc, ClassTag$.MODULE$.apply(StructuredRecord.class));
    this.connectionFactory = connectionFactory;
    this.maxRetrySeconds = maxRetrySeconds;
    this.maxBatchSize = maxBatchSize;
    this.tableWhitelist = Collections.unmodifiableSet(new HashSet<>(tableWhitelist));
    this.operationsList = Collections.unmodifiableSet(new HashSet<>(operationsList));
    // if not current tracking version is given initialize it to 0
    trackingOffset = startingOffset;
  }

  @Override
  public Option<RDD<StructuredRecord>> compute(Time validTime) {
    try {
      return doCompute(validTime);
    } catch (Exception e) {
      if (shouldFail()) {
        throw Throwables.propagate(e);
      } else {
        if (!isFailing) {
          failureStartTime = nowInSeconds();
        }
        isFailing = true;
        LOG.warn("Failed to read events. Will retry at the next interval.", e);
      }
      return Option.empty();
    }
  }

  private boolean shouldFail() {
    long timeElapsed = nowInSeconds() - failureStartTime;
    return forceFailure || maxRetrySeconds == 0 || (isFailing && timeElapsed > maxRetrySeconds);
  }

  private static long nowInSeconds() {
    return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
  }

  private Option<RDD<StructuredRecord>> doCompute(Time validTime) throws Exception {
    try (Connection connection = connectionFactory.getConnection()) {
      // get the table information of all tables which have ct enabled.
      List<TableInformation> tableInformations = getCTEnabledTables(connection);

      LinkedList<JavaRDD<StructuredRecord>> changeRDDs = new LinkedList<>();

      // Get the schema of tables. We get the schema of tables every microbatch because we want to update the downstream
      // dataset with the DDL changes if any.
      for (TableInformation tableInformation : tableInformations) {
        changeRDDs.add(getColumns(tableInformation));
      }

      // retrieve the current highest tracking version
      long prev = trackingOffset;
      long cur = Math.min(getCurrentTrackingVersion(connection), prev + maxBatchSize);
      LOG.info("Fetching changes from {} to {}", prev, cur);

      if (cur < prev) {
        this.forceFailure = true;
        throw Throwables.propagate(new Exception("Current CT version is less than the previous",
                new Error(String.format("Previous CT version: %s Current CT version: %s", prev, cur))));
      }

      // get all the data changes (DML) for the ct enabled tables till current tracking version
      for (TableInformation tableInformation : tableInformations) {
        changeRDDs.add(getChangeData(tableInformation, prev, cur));
      }
      // update the tracking version
      trackingOffset = cur;

      if (changeRDDs.isEmpty()) {
        return Option.empty();
      }

      // union the above ddl (Schema) and dml (data changes) rdd
      // The union does not maintain any order so its possible that dml changes might be before updated ddl (schema)
      // changes. Downstream sinks expects to see DDL before DML so order the rdd such that all DDL (schema) changes of
      // this microbatch appear before DML. Its the caller's responsibility to order it in this way
      RDD<StructuredRecord> changes = getJavaSparkContext().union(changeRDDs.pollFirst(), changeRDDs).rdd();
      return Option.apply(changes);
    }
  }

  @Override
  public void start() {
    // no-op
  }

  @Override
  public void stop() {
    // no-op
    // Also no need to close the dbconnection as JdbcRDD takes care of closing it
  }

  private JavaRDD<StructuredRecord> getChangeData(TableInformation tableInformation, long prev, long cur) {
    String stmt = String.format("SELECT [CT].[SYS_CHANGE_VERSION] as CHANGE_TRACKING_VERSION, " +
                                  "[CT].[SYS_CHANGE_CREATION_VERSION], " +
                                  "[CT].[SYS_CHANGE_OPERATION], " +
                                  "SYSUTCDATETIME() as CDC_CURRENT_TIMESTAMP, " +
                                  "%s, %s FROM [%s] (nolock) " +
                                  "as [CI] RIGHT OUTER JOIN " +
                                  "CHANGETABLE (CHANGES [%s], %s) as [CT] on %s " +
                                  "where [CT].[SYS_CHANGE_VERSION] > ? " +
                                  "and [CT].[SYS_CHANGE_VERSION] <= ? " +
                                  "and [CT].[SYS_CHANGE_OPERATION] IN " +
                                  "('" + String.join("','", this.operationsList) + "') " +
                                  "ORDER BY [CT].[SYS_CHANGE_VERSION]",
                                getSelectColumns("CT", tableInformation.getPrimaryKeys()),
                                getSelectColumns("CI", tableInformation.getValueColumnNames()),
                                tableInformation.getName(), tableInformation.getName(), prev,
                                getJoinCondition(tableInformation.getPrimaryKeys()));

    LOG.debug("Querying for change data with statement {}", stmt);

    //TODO Currently we are not partitioning the data. We should p
    //53™1artition it for scalability
    return JdbcRDD.create(getJavaSparkContext(), connectionFactory, stmt, prev, cur, 1,
                          new ResultSetToDMLRecord(tableInformation));
  }

  private long getCurrentTrackingVersion(Connection connection) throws SQLException {
    ResultSet resultSet = connection.createStatement().executeQuery("SELECT CHANGE_TRACKING_CURRENT_VERSION()");
    long changeVersion = 0;
    if (resultSet.next()) {
      changeVersion = resultSet.getLong(1);
      LOG.debug("Current tracking version is {}", changeVersion);
    }
    return changeVersion;
  }

  private JavaRDD<StructuredRecord> getColumns(TableInformation tableInformation) {
    String stmt = String.format("SELECT TOP 1 * FROM [%s].[%s](nolock) where ?=?", tableInformation.getSchemaName(),
                                tableInformation.getName());
    return JdbcRDD.create(getJavaSparkContext(), connectionFactory, stmt, 1, 1, 1,
                          new ResultSetToDDLRecord(tableInformation.getSchemaName(), tableInformation.getName()));
  }

  private JavaSparkContext getJavaSparkContext() {
    return JavaSparkContext.fromSparkContext(ssc().sc());
  }

  private Set<String> getColumns(Connection connection, String schema, String table) throws SQLException {
    String query = String.format("SELECT TOP 1 * from [%s].[%s](nolock)", schema, table);
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

  private List<TableInformation> getCTEnabledTables(Connection connection) throws SQLException {
    List<TableInformation> tableInformations = new LinkedList<>();
    String stmt = "SELECT s.name as schema_name, t.name AS table_name, ctt.* FROM sys.change_tracking_tables ctt " +
      "INNER JOIN sys.tables t on t.object_id = ctt.object_id INNER JOIN sys.schemas s on s.schema_id = t.schema_id";
    try (ResultSet rs = connection.createStatement().executeQuery(stmt)) {
      while (rs.next()) {
        String schemaName = rs.getString("schema_name");
        String tableName = rs.getString("table_name");
        if (tableWhitelist.isEmpty() || tableWhitelist.contains(tableName)) {
          tableInformations.add(new TableInformation(schemaName, tableName,
                                                     getColumns(connection, schemaName, tableName),
                                                     getKeyColumns(connection, schemaName, tableName)));
        }
      }
      return tableInformations;
    }
  }

  private static String getJoinCondition(Set<String> keyColumns) {
    return keyColumns.stream()
      .map(keyColumn -> String.format("[CT].[%s] = [CI].[%s]", keyColumn, keyColumn))
      .collect(Collectors.joining(" AND "));
  }

  private static String getSelectColumns(String tableName, Collection<String> cols) {
    return cols.stream()
      .map(column -> String.format("[%s].[%s]", tableName, column))
      .collect(Collectors.joining(", "));
  }
}
