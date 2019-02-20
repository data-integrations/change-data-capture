package co.cask.cdc.plugins.common;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.Schema.Field;
import co.cask.cdap.api.data.schema.Schema.Type;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Helper class with common cdc schemes definitions.
 */
public class Schemes {

  private static final Schema SIMPLE_TYPES = Schema.unionOf(Arrays.stream(Type.values())
                                                              .filter(Type::isSimpleType)
                                                              .map(Schema::of)
                                                              .collect(Collectors.toList()));

  public static final String CHANGED_ROWS_RECORD = "changedRows";
  public static final String TABLE_FIELD = "table";
  public static final String SCHEMA_FIELD = "schema";
  public static final String OP_TYPE_FIELD = "op_type";
  public static final String PRIMARY_KEYS_FIELD = "primary_keys";
  public static final String CHANGE_FIELD = "change";
  public static final String UPDATE_SCHEMA_FIELD = "rows_schema";
  public static final String UPDATE_VALUES_FIELD = "rows_values";

  public static final Schema DDL_SCHEMA = Schema.recordOf(
    "DDLRecord",
    Field.of(TABLE_FIELD, Schema.of(Type.STRING)),
    Field.of(SCHEMA_FIELD, Schema.of(Type.STRING))
  );

  public static final Schema DML_SCHEMA = Schema.recordOf(
    "DMLRecord",
    Field.of(OP_TYPE_FIELD, Schema.of(Type.STRING)),
    Field.of(TABLE_FIELD, Schema.of(Type.STRING)),
    Field.of(PRIMARY_KEYS_FIELD, Schema.arrayOf(Schema.of(Type.STRING))),
    Field.of(UPDATE_SCHEMA_FIELD, Schema.of(Type.STRING)),
    Field.of(UPDATE_VALUES_FIELD, Schema.mapOf(Schema.of(Type.STRING), SIMPLE_TYPES))
  );

  public static final Schema CHANGE_SCHEMA = Schema.recordOf(
    "changeRecord",
    Field.of(CHANGE_FIELD, Schema.unionOf(DDL_SCHEMA, DML_SCHEMA))
  );

  public static String getTableName(String namespacedTableName) {
    return namespacedTableName.split("\\.")[1];
  }

  private Schemes() {
    // utility class
  }
}
