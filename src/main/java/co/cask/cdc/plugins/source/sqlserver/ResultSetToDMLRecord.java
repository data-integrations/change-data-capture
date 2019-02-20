package co.cask.cdc.plugins.source.sqlserver;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdc.plugins.common.Schemes;
import co.cask.hydrator.plugin.DBUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.function.Function;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A serializable class to allow invoking {@link scala.Function1} from Java. The function converts {@link ResultSet}
 * to {@link StructuredRecord} for dml records
 */
public class ResultSetToDMLRecord implements Function<ResultSet, StructuredRecord> {
  private static final int CHANGE_TABLE_COLUMNS_SIZE = 3;
  private final TableInformation tableInformation;

  ResultSetToDMLRecord(TableInformation tableInformation) {
    this.tableInformation = tableInformation;
  }

  @Override
  public StructuredRecord call(ResultSet row) throws SQLException {
    Schema changeSchema = getChangeSchema(row);
    return StructuredRecord.builder(Schemes.DML_SCHEMA)
      .set(Schemes.TABLE_FIELD, Joiner.on(".").join(tableInformation.getSchemaName(), tableInformation.getName()))
      .set(Schemes.PRIMARY_KEYS_FIELD, Lists.newArrayList(tableInformation.getPrimaryKeys()))
      .set(Schemes.OP_TYPE_FIELD, row.getString("SYS_CHANGE_OPERATION"))
      .set(Schemes.UPDATE_SCHEMA_FIELD, changeSchema.toString())
      .set(Schemes.UPDATE_VALUES_FIELD, getChangeData(row, changeSchema))
      .build();
  }

  private static Map<String, Object> getChangeData(ResultSet resultSet, Schema changeSchema) throws SQLException {
    ResultSetMetaData metadata = resultSet.getMetaData();
    Map<String, Object> changes = new HashMap<>();
    for (int i = 0; i < changeSchema.getFields().size(); i++) {
      int column = i + CHANGE_TABLE_COLUMNS_SIZE;
      int sqlType = metadata.getColumnType(column);
      int sqlPrecision = metadata.getPrecision(column);
      int sqlScale = metadata.getScale(column);
      Schema.Field field = changeSchema.getFields().get(i);
      Object sqlValue = DBUtils.transformValue(sqlType, sqlPrecision, sqlScale, resultSet, field.getName());
      Object javaValue = transformSQLToJavaType(sqlValue);
      changes.put(field.getName(), javaValue);
    }
    return changes;
  }

  private static Schema getChangeSchema(ResultSet resultSet) throws SQLException {
    List<Schema.Field> schemaFields = DBUtils.getSchemaFields(resultSet);
    // drop first three columns as they are from change tracking tables and does not represent the change data
    return Schema.recordOf(Schemes.CHANGED_ROWS_RECORD,
                           schemaFields.subList(CHANGE_TABLE_COLUMNS_SIZE, schemaFields.size()));
  }

  private static Object transformSQLToJavaType(Object sqlValue) {
    if (sqlValue instanceof Date) {
      return ((Date) sqlValue).getTime();
    } else if (sqlValue instanceof Time) {
      return ((Time) sqlValue).getTime();
    } else if (sqlValue instanceof Timestamp) {
      return ((Timestamp) sqlValue).getTime();
    } else {
      return sqlValue;
    }
  }
}
