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
