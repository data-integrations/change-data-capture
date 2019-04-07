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

package co.cask.cdc.plugins.source.oracle;

import co.cask.cdap.etl.api.validation.InvalidConfigPropertyException;
import co.cask.hydrator.common.Constants;
import org.junit.Assert;
import org.junit.Test;

public class GoldenGateKafkaConfigUnitTest {
  private static final String VALID_REF = "test-ref";
  private static final String VALID_BROKER = "localhost:9092";
  private static final String VALID_TOPIC = "topic1";
  private static final Long VALID_DEFAULT_INITIAL_OFFSET = 0L;
  private static final Integer VALID_MAX_RATE_PER_PARTITION = 0;

  @Test
  public void testValidateValidConfig() {
    GoldenGateKafkaConfig config = new GoldenGateKafkaConfig(
      VALID_REF,
      VALID_BROKER,
      VALID_TOPIC,
      VALID_DEFAULT_INITIAL_OFFSET,
      VALID_MAX_RATE_PER_PARTITION
    );

    config.validate();
  }

  @Test
  public void testValidateReference() {
    GoldenGateKafkaConfig config = new GoldenGateKafkaConfig(
      "",
      VALID_BROKER,
      VALID_TOPIC,
      VALID_DEFAULT_INITIAL_OFFSET,
      VALID_MAX_RATE_PER_PARTITION
    );

    try {
      config.validate();
      Assert.fail(String.format("Expected to throw %s", InvalidConfigPropertyException.class.getName()));
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(Constants.Reference.REFERENCE_NAME, e.getProperty());
    }
  }

  @Test
  public void testValidateMissingBroker() {
    GoldenGateKafkaConfig config = new GoldenGateKafkaConfig(
      VALID_REF,
      null,
      VALID_TOPIC,
      VALID_DEFAULT_INITIAL_OFFSET,
      VALID_MAX_RATE_PER_PARTITION
    );

    try {
      config.validate();
      Assert.fail(String.format("Expected to throw %s", InvalidConfigPropertyException.class.getName()));
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(GoldenGateKafkaConfig.BROKER, e.getProperty());
    }
  }

  @Test
  public void testValidateEmptyBroker() {
    GoldenGateKafkaConfig config = new GoldenGateKafkaConfig(
      VALID_REF,
      "",
      VALID_TOPIC,
      VALID_DEFAULT_INITIAL_OFFSET,
      VALID_MAX_RATE_PER_PARTITION
    );

    try {
      config.validate();
      Assert.fail(String.format("Expected to throw %s", InvalidConfigPropertyException.class.getName()));
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(GoldenGateKafkaConfig.BROKER, e.getProperty());
    }
  }

  @Test
  public void testValidateWronglyFormattedBroker() {
    GoldenGateKafkaConfig config = new GoldenGateKafkaConfig(
      VALID_REF,
      "localhost",
      VALID_TOPIC,
      VALID_DEFAULT_INITIAL_OFFSET,
      VALID_MAX_RATE_PER_PARTITION
    );

    try {
      config.validate();
      Assert.fail(String.format("Expected to throw %s", InvalidConfigPropertyException.class.getName()));
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(GoldenGateKafkaConfig.BROKER, e.getProperty());
    }
  }

  @Test
  public void testValidateMissingTopic() {
    GoldenGateKafkaConfig config = new GoldenGateKafkaConfig(
      VALID_REF,
      VALID_BROKER,
      null,
      VALID_DEFAULT_INITIAL_OFFSET,
      VALID_MAX_RATE_PER_PARTITION
    );

    try {
      config.validate();
      Assert.fail(String.format("Expected to throw %s", InvalidConfigPropertyException.class.getName()));
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(GoldenGateKafkaConfig.TOPIC, e.getProperty());
    }
  }

  @Test
  public void testValidateEmptyTopic() {
    GoldenGateKafkaConfig config = new GoldenGateKafkaConfig(
      VALID_REF,
      VALID_BROKER,
      "",
      VALID_DEFAULT_INITIAL_OFFSET,
      VALID_MAX_RATE_PER_PARTITION
    );

    try {
      config.validate();
      Assert.fail(String.format("Expected to throw %s", InvalidConfigPropertyException.class.getName()));
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(GoldenGateKafkaConfig.TOPIC, e.getProperty());
    }
  }

  @Test
  public void testValidateDefaultInitialOffset() {
    GoldenGateKafkaConfig config = new GoldenGateKafkaConfig(
      VALID_REF,
      VALID_BROKER,
      VALID_TOPIC,
      -3L,
      VALID_MAX_RATE_PER_PARTITION
    );

    try {
      config.validate();
      Assert.fail(String.format("Expected to throw %s", InvalidConfigPropertyException.class.getName()));
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(GoldenGateKafkaConfig.DEFAULT_INITIAL_OFFSET, e.getProperty());
    }
  }

  @Test
  public void testValidateMaxRatePerPartition() {
    GoldenGateKafkaConfig config = new GoldenGateKafkaConfig(
      VALID_REF,
      VALID_BROKER,
      VALID_TOPIC,
      VALID_DEFAULT_INITIAL_OFFSET,
      -1
    );

    try {
      config.validate();
      Assert.fail(String.format("Expected to throw %s", InvalidConfigPropertyException.class.getName()));
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(GoldenGateKafkaConfig.MAX_RATE_PER_PARTITION, e.getProperty());
    }
  }
}
