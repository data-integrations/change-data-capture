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
import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.DBUtils;
import io.cdap.plugin.cdc.common.Schemas;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A serializable class to allow invoking {@link scala.Function1} from Java. The function converts {@link ResultSet}
 * to {@link StructuredRecord} for DDL i.e. schema changes
 */
public class ResultSetToDDLRecord implements Function<ResultSet, StructuredRecord> {

  private final String schemaName;
  private final String tableName;
  private final boolean requireSeqNumber;
  private static final Logger LOG = LoggerFactory.getLogger(CTSQLServer.class);

  ResultSetToDDLRecord(String schemaName, String tableName, boolean requireSeqNumber) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.requireSeqNumber = requireSeqNumber;
  }

  @Override
  public StructuredRecord call(ResultSet row) throws SQLException {
    java.util.List<io.cdap.cdap.api.data.schema.Schema.Field> fields = Lists.newArrayList();
    if (requireSeqNumber) {
      fields.add(io.cdap.cdap.api.data.schema.Schema.Field.of("CHANGE_TRACKING_VERSION", Schema.of(Schema.Type.LONG)));
      fields.add(io.cdap.cdap.api.data.schema.Schema.Field.of("CDC_CURRENT_TIMESTAMP", Schema.of(Schema.Type.LONG)));
    }
    fields.addAll(DBUtils.getSchemaFields(row));
    Schema tableSchema = Schema.recordOf(Schemas.SCHEMA_RECORD, fields);
    return StructuredRecord.builder(Schemas.DDL_SCHEMA)
      .set(Schemas.TABLE_FIELD, Joiner.on(".").join(schemaName, tableName))
      .set(Schemas.SCHEMA_FIELD, tableSchema.toString())
      .build();
  }
}
