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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.batch.JobUtils;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * BigTable sink for CDC
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("CDCBigTable")
public class CDCBigTable extends SparkSink<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(CDCBigTable.class);

  private final CDCBigTableConfig config;

  public CDCBigTable(CDCBigTableConfig config) {
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
        Connection connection;
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        // Switch the context classloader to plugin class' classloader (PluginClassLoader) so that
        // when Job/Configuration is created, it uses PluginClassLoader to load resources (hbase-default.xml)
        // which is present in the plugin jar and is not visible in the CombineClassLoader (which is what oldClassLoader
        // points to).
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
        try {
          job = JobUtils.createInstance();

          Configuration conf = job.getConfiguration();

          for (Map.Entry<String, String> configEntry : configs.entrySet()) {
            conf.set(configEntry.getKey(), configEntry.getValue());
          }

          BigtableConfiguration.configure(conf, config.project, config.instance);
          conf.set(BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY,
                   config.serviceAccountFilePath);

          connection = BigtableConfiguration.connect(conf);

        } finally {
          // Switch back to the original
          Thread.currentThread().setContextClassLoader(oldClassLoader);
        }

        try (Connection conn = connection;
             Admin hBaseAdmin = conn.getAdmin()) {
          while (structuredRecordIterator.hasNext()) {
            StructuredRecord input = structuredRecordIterator.next();
            LOG.debug("Received StructuredRecord {}", StructuredRecordStringConverter.toJsonString(input));
            String tableName = CDCHBase.getTableName((String) input.get("table"));
            if (input.getSchema().getRecordName().equals("DDLRecord")) {
              LOG.info("Creating table {}", tableName);
              // Notes: In BigTable, there no such thing as namespace. Dots are allowed in table names, but colons are not.
              // If you try a table name with a colon in it, you will get:
              // io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Invalid id for collection tables : \
              // Should match [_a-zA-Z0-9][-_.a-zA-Z0-9]* but found 'ns:abcd'
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


  public static class CDCBigTableConfig extends ReferencePluginConfig {

    @Name("instance")
    @Description("Instance ID")
    @Macro
    public String instance;


    @Name("project")
    @Description("Project ID")
    @Macro
    public String project;

    @Name("serviceFilePath")
    @Description("Service Account File Path")
    @Macro
    public String serviceAccountFilePath;

    public CDCBigTableConfig(String referenceName) {
      super(referenceName);
    }
  }

}
