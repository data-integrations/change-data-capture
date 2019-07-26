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

import com.google.common.base.Joiner;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cdc.common.DBUtils;
import io.cdap.plugin.cdc.common.Schemas;
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
    Schema tableSchema = Schema.recordOf(Schemas.SCHEMA_RECORD, DBUtils.getSchemaFields(row));
    return StructuredRecord.builder(Schemas.DDL_SCHEMA)
      .set(Schemas.TABLE_FIELD, Joiner.on(".").join(schemaName, tableName))
      .set(Schemas.SCHEMA_FIELD, tableSchema.toString())
      .build();
  }
}
