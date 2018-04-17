package co.cask.cdc.plugins.source.sqlserver;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import scala.Serializable;

import java.util.Set;

/**
 * Represents SQL Server Table information
 */
class TableInformation implements Serializable {
  private final String schemaName;
  private final String name;
  private final Set<String> columnSchema;
  private final Set<String> primaryKeys;
  private final Set<String> valueColumns;

  TableInformation(String schemaName, String name, Set<String> columnSchema, Set<String> primaryKeys) {
    this.schemaName = schemaName;
    this.name = name;
    this.columnSchema = columnSchema;
    this.primaryKeys = primaryKeys;
    this.valueColumns = ImmutableSet.copyOf(Sets.difference(columnSchema, primaryKeys));
  }

  @Override
  public String toString() {
    return "TableInformation{" +
      "schemaName='" + schemaName + '\'' +
      ", name='" + name + '\'' +
      ", columnSchema=" + columnSchema +
      ", primaryKeys=" + primaryKeys +
      ", valueColumns=" + valueColumns +
      '}';
  }

  String getSchemaName() {
    return schemaName;
  }

  String getName() {
    return name;
  }

  Set<String> getColumnSchema() {
    return columnSchema;
  }

  Set<String> getPrimaryKeys() {
    return primaryKeys;
  }

  Set<String> getValueColumnNames() {
    return valueColumns;
  }

}
