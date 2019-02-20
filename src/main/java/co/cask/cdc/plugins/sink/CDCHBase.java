/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdc.plugins.sink;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.api.validation.InvalidStageException;
import co.cask.cdc.plugins.common.Schemes;
import co.cask.cdc.plugins.common.SparkConfigs;
import co.cask.hydrator.common.batch.JobUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.util.Map;

/**
 * HBase sink for CDC
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("CDCHBase")
public class CDCHBase extends SparkSink<StructuredRecord> {
  private final CDCHBaseConfig config;

  public CDCHBase(CDCHBaseConfig config) {
    this.config = config;
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws Exception {
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
    if (!Schemes.CHANGE_SCHEMA.isCompatible(pipelineConfigurer.getStageConfigurer().getInputSchema())) {
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
          StructuredRecord changeRecord = input.get(Schemes.CHANGE_FIELD);
          String tableName = Schemes.getTableName(changeRecord.get(Schemes.TABLE_FIELD));
          if (changeRecord.getSchema().getRecordName().equals(Schemes.DDL_SCHEMA.getRecordName())) {
            CDCTableUtil.createHBaseTable(hBaseAdmin, tableName);
          } else {
            Table table = hBaseAdmin.getConnection().getTable(TableName.valueOf(tableName));
            CDCTableUtil.updateHBaseTable(table, changeRecord);
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
    Job job;
    try {
      job = JobUtils.createInstance();
    } finally {
      // Switch back to the original
      Thread.currentThread().setContextClassLoader(oldClassLoader);
    }
    Configuration conf = job.getConfiguration();

    for (Map.Entry<String, String> configEntry : hadoopConfigs.entrySet()) {
      conf.set(configEntry.getKey(), configEntry.getValue());
    }

    return ConnectionFactory.createConnection(conf);
  }
}
