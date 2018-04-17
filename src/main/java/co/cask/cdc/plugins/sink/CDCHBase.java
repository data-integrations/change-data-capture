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
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.batch.JobUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * HBase sink for CDC
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("CDCHBase")
public class CDCHBase extends SparkSink<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(CDCHBase.class);
  private final CDCHBaseConfig config;

  public CDCHBase(CDCHBaseConfig config) {
    this.config = config;
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws Exception { }

  @Override
  public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> javaRDD) throws Exception {
    // Get the hadoop configurations and passed it as a Map to the closure
    Iterator<Map.Entry<String, String>> iterator = javaRDD.context().hadoopConfiguration().iterator();
    final Map<String, String> configs = new HashMap<>();
    while (iterator.hasNext()) {
      Map.Entry<String, String> next = iterator.next();
      configs.put(next.getKey(), next.getValue());
    }

    // maps data sets to each block of computing resources
    javaRDD.foreachPartition(new VoidFunction<Iterator<StructuredRecord>>() {

      @Override
      public void call(Iterator<StructuredRecord> structuredRecordIterator) throws Exception {

        Job job;
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        // Switch the context classloader to plugin class' classloader (PluginClassLoader) so that
        // when Job/Configuration is created, it uses PluginClassLoader to load resources (hbase-default.xml)
        // which is present in the plugin jar and is not visible in the CombineClassLoader (which is what oldClassLoader
        // points to).
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
        try {
          job = JobUtils.createInstance();
        } finally {
          // Switch back to the original
          Thread.currentThread().setContextClassLoader(oldClassLoader);
        }

        Configuration conf = job.getConfiguration();

        for(Map.Entry<String, String> configEntry : configs.entrySet()) {
          conf.set(configEntry.getKey(), configEntry.getValue());
        }

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin hBaseAdmin = connection.getAdmin()) {
          while (structuredRecordIterator.hasNext()) {
            StructuredRecord input = structuredRecordIterator.next();
            LOG.debug("Received StructuredRecord in Kudu {}", StructuredRecordStringConverter.toJsonString(input));
            String tableName = getTableName((String) input.get("table"));
            if (input.getSchema().getRecordName().equals("DDLRecord")) {
              CDCTableUtil.createHBaseTable(hBaseAdmin, tableName);
            } else {
              Table table = hBaseAdmin.getConnection().getTable(TableName.valueOf(tableName));
              CDCTableUtil.updateHBaseTable(table, input);
            }
          }
        }
      }
    });
  }


  public static String getTableName(String namespacedTableName) {
    return namespacedTableName.split("\\.")[1];
  }

  public static class CDCHBaseConfig extends ReferencePluginConfig {
    public CDCHBaseConfig(String referenceName) {
      super(referenceName);
    }
  }
}

