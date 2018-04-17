package co.cask.cdc.plugins.source.sqlserver;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.DBUtils;
import com.google.common.base.Joiner;
import scala.Serializable;
import scala.runtime.AbstractFunction1;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A serializable class to allow invoking {@link scala.Function1} from Java. The function converts {@link ResultSet}
 * to {@link StructuredRecord} for DDL i.e. schema changes
 */
public class ResultSetToDDLRecord extends AbstractFunction1<ResultSet, StructuredRecord> implements Serializable {

  private static final String RECORD_NAME = "DDLRecord";
  private static final Schema DDL_SCHEMA = Schema.recordOf(RECORD_NAME,
                                                           Schema.Field.of("table", Schema.of(Schema.Type.STRING)),
                                                           Schema.Field.of("schema", Schema.of(Schema.Type.STRING)));

  private final String schemaName;
  private final String tableName;

  ResultSetToDDLRecord(String schemaName, String tableName) {
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  public StructuredRecord apply(ResultSet row) {
    try {
      return transform(row);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private StructuredRecord transform(ResultSet resultSet) throws SQLException {
    Schema tableSchema = Schema.recordOf("schema", DBUtils.getSchemaFields(resultSet));
    StructuredRecord.Builder builder = StructuredRecord.builder(DDL_SCHEMA);
    builder.set("table", Joiner.on(".").join(schemaName, tableName));
    builder.set("schema", tableSchema.toString());
    return builder.build();
  }
}
