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

package io.cdap.plugin.cdc.common;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Utility methods for common Bigtable operations.
 */
public class BigtableOperations {
  private BigtableOperations() {
    // utility class
  }

  public static Connection connect(String projectId, String instanceId, @Nullable String serviceAccountFilepath) {
    Configuration configuration = BigtableConfiguration.configure(projectId, instanceId);
    if (serviceAccountFilepath != null) {
      configuration.set(BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY,
                        serviceAccountFilepath);
    }
    return BigtableConfiguration.connect(configuration);
  }

  public static void dropTableIfExists(Connection connection, String dbTableName) throws IOException {
    TableName tableName = TableName.valueOf(dbTableName);
    if (connection.getAdmin().tableExists(tableName)) {
      connection.getAdmin().disableTable(tableName);
      connection.getAdmin().deleteTable(tableName);
    }
  }
}
