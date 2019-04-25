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

import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.ReferencePluginConfig;

/**
 * {@link ReferencePluginConfig} with reference name validation.
 */
public class CDCReferencePluginConfig extends ReferencePluginConfig {
  public CDCReferencePluginConfig(String referenceName) {
    super(referenceName);
  }

  public void validate() {
    if (!containsMacro(Constants.Reference.REFERENCE_NAME)) {
      try {
        IdUtils.validateId(referenceName);
      } catch (IllegalArgumentException e) {
        throw new InvalidConfigPropertyException(e.getMessage(), Constants.Reference.REFERENCE_NAME);
      }
    }
  }
}
