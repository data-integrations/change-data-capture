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

package co.cask.cdc.plugins.common;

import org.apache.spark.api.java.JavaRDD;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Utility methods for dealing with Spark configuration and data.
 */
public class SparkConfigs {
  private SparkConfigs() {
    // utility class
  }

  /**
   * Get the hadoop configurations and passed it as a Map to the closure
   *
   * @param javaRDD Spark RDD object
   * @return configuration Map
   */
  public static Map<String, String> getHadoopConfigs(JavaRDD<?> javaRDD) {
    Iterator<Map.Entry<String, String>> iterator = javaRDD.context().hadoopConfiguration().iterator();
    Map<String, String> configs = new HashMap<>();
    while (iterator.hasNext()) {
      Map.Entry<String, String> next = iterator.next();
      configs.put(next.getKey(), next.getValue());
    }
    return configs;
  }
}
