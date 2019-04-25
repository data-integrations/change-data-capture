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

package io.cdap.plugin.cdc.integration;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import org.assertj.core.presentation.StandardRepresentation;

import java.io.IOException;

public class StructuredRecordRepresentation extends StandardRepresentation {
  @Override
  public String toStringOf(Object object) {
    try {
      if (object instanceof StructuredRecord) {
        return StructuredRecordStringConverter.toJsonString((StructuredRecord) object);
      }
      return super.toStringOf(object);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
