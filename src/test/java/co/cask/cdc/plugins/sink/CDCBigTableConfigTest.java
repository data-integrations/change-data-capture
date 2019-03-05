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

package co.cask.cdc.plugins.sink;

import co.cask.cdap.etl.api.validation.InvalidConfigPropertyException;
import co.cask.hydrator.common.Constants;
import com.google.bigtable.repackaged.com.google.cloud.ServiceOptions;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.function.Consumer;

public class CDCBigTableConfigTest {
  private static final String VALID_REF = "test-ref";
  private static final String VALID_PROJECT = "test-project";
  private static final String VALID_INSTANCE = "test-instance";
  private static final String VALID_ACCOUNT_FILE_PATH
    = CDCBigTableConfigTest.class.getResource("/credentials.json").getPath();

  @Test
  public void testValidateValidConfig() {
    CDCBigTableConfig config = new CDCBigTableConfig(
      VALID_REF,
      VALID_INSTANCE,
      VALID_PROJECT,
      VALID_ACCOUNT_FILE_PATH
    );

    config.validate();
  }

  @Test
  public void testValidateReference() {
    CDCBigTableConfig config = new CDCBigTableConfig(
      "",
      VALID_INSTANCE,
      VALID_PROJECT,
      VALID_ACCOUNT_FILE_PATH
    );

    Assertions.assertThatThrownBy(config::validate)
      .isInstanceOfSatisfying(InvalidConfigPropertyException.class, withProperty(Constants.Reference.REFERENCE_NAME));
  }

  @Test
  public void testValidateMissingCredentialsFile() {
    CDCBigTableConfig config = new CDCBigTableConfig(
      VALID_REF,
      VALID_INSTANCE,
      VALID_PROJECT,
      "/tmp/non_existing_file"
    );

    Assertions.assertThatThrownBy(config::validate)
      .isInstanceOfSatisfying(InvalidConfigPropertyException.class,
                              withProperty(CDCBigTableConfig.SERVICE_ACCOUNT_FILE_PATH));
  }

  @Test
  public void testValidateMissingProjectId() {
    Assume.assumeTrue(ServiceOptions.getDefaultProjectId() == null);

    CDCBigTableConfig config = new CDCBigTableConfig(
      VALID_REF,
      VALID_INSTANCE,
      null,
      VALID_ACCOUNT_FILE_PATH
    );

    Assertions.assertThatThrownBy(config::validate)
      .isInstanceOfSatisfying(InvalidConfigPropertyException.class, withProperty(CDCBigTableConfig.PROJECT));
  }

  @Test
  public void testValidateMissingInstanceId() {
    CDCBigTableConfig config = new CDCBigTableConfig(
      VALID_REF,
      null,
      VALID_PROJECT,
      VALID_ACCOUNT_FILE_PATH
    );

    Assertions.assertThatThrownBy(config::validate)
      .isInstanceOfSatisfying(InvalidConfigPropertyException.class, withProperty(CDCBigTableConfig.INSTANCE));
  }

  @Test
  public void testResolveProjectId() {
    CDCBigTableConfig config = new CDCBigTableConfig(
      VALID_REF,
      VALID_INSTANCE,
      null,
      VALID_ACCOUNT_FILE_PATH
    );

    Assertions.assertThat(config.resolveProject())
      .isEqualTo(ServiceOptions.getDefaultProjectId());
  }

  @Test
  public void testResolveProjectIdAutoDetect() {
    CDCBigTableConfig config = new CDCBigTableConfig(
      VALID_REF,
      VALID_INSTANCE,
      CDCBigTableConfig.AUTO_DETECT,
      VALID_ACCOUNT_FILE_PATH
    );

    Assertions.assertThat(config.resolveProject())
      .isEqualTo(ServiceOptions.getDefaultProjectId());
  }

  @Test
  public void testServiceAccountFilePath() {
    CDCBigTableConfig config = new CDCBigTableConfig(
      VALID_REF,
      VALID_INSTANCE,
      VALID_PROJECT,
      null
    );

    Assertions.assertThat(config.resolveServiceAccountFilePath())
      .isNull();
  }

  @Test
  public void testServiceAccountFilePathAutoDetect() {
    CDCBigTableConfig config = new CDCBigTableConfig(
      VALID_REF,
      VALID_INSTANCE,
      VALID_PROJECT,
      CDCBigTableConfig.AUTO_DETECT
    );

    Assertions.assertThat(config.resolveServiceAccountFilePath())
      .isNull();
  }

  private static Consumer<InvalidConfigPropertyException> withProperty(String property) {
    return e -> Assert.assertEquals(property, e.getProperty());
  }
}
