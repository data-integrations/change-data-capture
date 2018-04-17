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
package co.cask.cdc.plugins.source.oracle;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.hydrator.common.ReferencePluginConfig;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Configurations to be used for Golden Gate Kafka source.
 */
public class GoldenGateKafkaConfig extends ReferencePluginConfig implements Serializable {

  private static final long serialVersionUID = 8069169417140954175L;

  @Description("Kafka broker specified in host:port form. For example, example.com:9092")
  @Macro
  private String broker;

  @Description("Name of the topic to which Golden Gate publishes the DDL and DML changes.")
  @Macro
  private String topic;

  @Description("The default initial offset to read from. " +
    "An offset of -2 means the smallest offset. An offset of -1 means the latest offset. Defaults to -1. " +
    "Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read. ")
  @Nullable
  @Macro
  private Long defaultInitialOffset;

  @Description("Max number of records to read per second per partition. 0 means there is no limit. Defaults to 1000.")
  @Nullable
  private Integer maxRatePerPartition;

  public GoldenGateKafkaConfig() {
    super("");
    defaultInitialOffset = -1L;
    maxRatePerPartition = 1000;
  }

  public GoldenGateKafkaConfig(String referenceName, String broker, String topic, Long defaultInitialOffset) {
    super(referenceName);
    this.broker = broker;
    this.topic = topic;
    this.defaultInitialOffset = defaultInitialOffset;
  }

  public String getBroker() {
    return broker;
  }

  public String getHost() {
    return broker.split(":")[0];
  }

  public int getPort() {
    return Integer.valueOf(broker.split(":")[1]);
  }

  public String getTopic() {
    return topic;
  }

  public Long getDefaultInitialOffset() {
    return defaultInitialOffset;
  }

  public Integer getMaxRatePerPartition() {
    return maxRatePerPartition == null ? 1000 : maxRatePerPartition;
  }

  /**
   * Method to validate the broker address which should be in the form 'host:port'.
   * throws IllegalArgumentException if validation fails
   */
  public void validate() {
    try {
      getHost();
      getPort();
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("Broker address '%s' should be in the form of 'host:port'.",
                                                       broker));
    }
  }
}
