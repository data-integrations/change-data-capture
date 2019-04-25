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
package io.cdap.plugin.cdc.source.oracle;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.cdc.common.CDCReferencePluginConfig;
import com.google.common.base.Strings;
import org.apache.commons.lang3.ObjectUtils;

import javax.annotation.Nullable;

/**
 * Configurations to be used for Golden Gate Kafka source.
 */
public class GoldenGateKafkaConfig extends CDCReferencePluginConfig {

  private static final long serialVersionUID = 8069169417140954175L;

  public static final String BROKER = "broker";
  public static final String TOPIC = "topic";
  public static final String DEFAULT_INITIAL_OFFSET = "defaultInitialOffset";
  public static final String MAX_RATE_PER_PARTITION = "maxRatePerPartition";

  @Name(BROKER)
  @Description("Kafka broker specified in host:port form. For example, example.com:9092")
  @Macro
  private final String broker;

  @Name(TOPIC)
  @Description("Name of the topic to which Golden Gate publishes the DDL and DML changes.")
  @Macro
  private final String topic;

  @Name(DEFAULT_INITIAL_OFFSET)
  @Description("The default initial offset to read from. " +
    "An offset of -2 means the smallest offset. An offset of -1 means the latest offset. Defaults to -1. " +
    "Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read. ")
  @Macro
  @Nullable
  private final Long defaultInitialOffset;

  @Name(MAX_RATE_PER_PARTITION)
  @Description("Max number of records to read per second per partition. 0 means there is no limit. Defaults to 1000.")
  @Macro
  @Nullable
  private final Integer maxRatePerPartition;

  public GoldenGateKafkaConfig(String referenceName, @Nullable String broker, @Nullable String topic,
                               @Nullable Long defaultInitialOffset, @Nullable Integer maxRatePerPartition) {
    super(referenceName);
    this.broker = broker;
    this.topic = topic;
    this.defaultInitialOffset = defaultInitialOffset;
    this.maxRatePerPartition = maxRatePerPartition;
  }

  @Nullable
  public String getBroker() {
    return broker;
  }

  public String getHost() {
    return broker.split(":")[0];
  }

  public int getPort() {
    return Integer.valueOf(broker.split(":")[1]);
  }

  @Nullable
  public String getTopic() {
    return topic;
  }

  public Long getDefaultInitialOffset() {
    return ObjectUtils.defaultIfNull(defaultInitialOffset, -1L);
  }

  public Integer getMaxRatePerPartition() {
    return ObjectUtils.defaultIfNull(maxRatePerPartition, 1000);
  }

  /**
   * Method to validate the broker address which should be in the form 'host:port'.
   * throws IllegalArgumentException if validation fails
   */
  @Override
  public void validate() {
    super.validate();
    if (!containsMacro(BROKER)) {
      if (Strings.isNullOrEmpty(broker)) {
        throw new InvalidConfigPropertyException("Broker address cannot be null or empty", BROKER);
      }
      try {
        getHost();
        getPort();
      } catch (Exception e) {
        throw new InvalidConfigPropertyException(
          String.format("Broker address '%s' should be in the form of 'host:port'.", broker), e, BROKER);
      }
    }
    if (!containsMacro(TOPIC) && Strings.isNullOrEmpty(topic)) {
      throw new InvalidConfigPropertyException("Topic cannot be null or empty", TOPIC);
    }
    if (!containsMacro(DEFAULT_INITIAL_OFFSET) && defaultInitialOffset != null && defaultInitialOffset < -2) {
      throw new InvalidConfigPropertyException("'defaultInitialOffset' should be equal to -2, -1, 0 or positive number",
                                               DEFAULT_INITIAL_OFFSET);
    }
    if (!containsMacro(MAX_RATE_PER_PARTITION) && maxRatePerPartition != null && maxRatePerPartition < 0) {
      throw new InvalidConfigPropertyException("'maxRatePerPartition' should be equal to 0 or positive number",
                                               MAX_RATE_PER_PARTITION);
    }
  }
}
