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

package io.cdap.plugin.cdc.sink;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.cdc.common.Schemas;
import io.cdap.plugin.cdc.common.SparkConfigs;
import io.cdap.plugin.common.batch.JobUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.util.Map;

/**
 * BigTable sink for CDC
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("CDCBigTable")
public class CDCBigTable extends SparkSink<StructuredRecord> {
  private final CDCBigTableConfig config;

  public CDCBigTable(CDCBigTableConfig config) {
    this.config = config;
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws Exception {
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
    if (!Schemas.CHANGE_SCHEMA.isCompatible(pipelineConfigurer.getStageConfigurer().getInputSchema())) {
      throw new InvalidStageException("Input schema is incompatible with change record schema");
    }
  }

  @Override
  public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> javaRDD) throws Exception {
    Map<String, String> hadoopConfigs = SparkConfigs.getHadoopConfigs(javaRDD);
    // maps data sets to each block of computing resources
    javaRDD.foreachPartition(structuredRecordIterator -> {
      try (Connection conn = getConnection(hadoopConfigs);
           Admin hBaseAdmin = conn.getAdmin()) {
        while (structuredRecordIterator.hasNext()) {
          StructuredRecord input = structuredRecordIterator.next();
          StructuredRecord ddlRecord = input.get(Schemas.DDL_FIELD);
          if (ddlRecord != null) {
            // Notes: In BigTable, there no such thing as namespace.
            // Dots are allowed in table names, but colons are not.
            // If you try a table name with a colon in it, you will get:
            // io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Invalid id for collection tables : \
            // Should match [_a-zA-Z0-9][-_.a-zA-Z0-9]* but found 'ns:abcd'
            String tableName = Schemas.getTableName(ddlRecord.get(Schemas.TABLE_FIELD));
            CDCTableUtil.createHBaseTable(hBaseAdmin, tableName);
          }
          StructuredRecord dmlRecord = input.get(Schemas.DML_FIELD);
          if (dmlRecord != null) {
            String tableName = Schemas.getTableName(dmlRecord.get(Schemas.TABLE_FIELD));
            Table table = hBaseAdmin.getConnection().getTable(TableName.valueOf(tableName));
            CDCTableUtil.updateHBaseTable(table, dmlRecord);
          }
        }
      }
    });
  }

  private Connection getConnection(Map<String, String> hadoopConfigs) throws IOException {
    ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
    // Switch the context classloader to plugin class' classloader (PluginClassLoader) so that
    // when Job/Configuration is created, it uses PluginClassLoader to load resources (hbase-default.xml)
    // which is present in the plugin jar and is not visible in the CombineClassLoader (which is what oldClassLoader
    // points to).
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    try {
      Job job = JobUtils.createInstance();

      Configuration conf = job.getConfiguration();

      for (Map.Entry<String, String> configEntry : hadoopConfigs.entrySet()) {
        conf.set(configEntry.getKey(), configEntry.getValue());
      }

      String projectId = config.resolveProject();
      String serviceAccountFilePath = config.resolveServiceAccountFilePath();
      BigtableConfiguration.configure(conf, projectId, config.instance);
      if (serviceAccountFilePath != null) {
        conf.set(BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY, serviceAccountFilePath);
      }

      return BigtableConfiguration.connect(conf);
    } finally {
      // Switch back to the original
      Thread.currentThread().setContextClassLoader(oldClassLoader);
    }
  }
}
