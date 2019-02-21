package co.cask.cdc.plugins.source.sqlserver;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdc.plugins.common.Schemas;
import co.cask.hydrator.plugin.DBUtils;
import com.google.common.base.Joiner;
import org.apache.spark.api.java.function.Function;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A serializable class to allow invoking {@link scala.Function1} from Java. The function converts {@link ResultSet}
 * to {@link StructuredRecord} for DDL i.e. schema changes
 */
public class ResultSetToDDLRecord implements Function<ResultSet, StructuredRecord> {

  private final String schemaName;
  private final String tableName;

  ResultSetToDDLRecord(String schemaName, String tableName) {
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  @Override
  public StructuredRecord call(ResultSet row) throws SQLException {
    Schema tableSchema = Schema.recordOf("schema", DBUtils.getSchemaFields(row));
    return StructuredRecord.builder(Schemas.DDL_SCHEMA)
      .set(Schemas.TABLE_FIELD, Joiner.on(".").join(schemaName, tableName))
      .set(Schemas.SCHEMA_FIELD, tableSchema.toString())
      .build();
  }
}
