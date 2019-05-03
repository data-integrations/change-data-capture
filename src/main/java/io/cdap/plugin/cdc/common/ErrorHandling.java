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

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Indicates error handling strategy which will be used during reading Salesforce records.
 */
public enum ErrorHandling {

  SKIP("Skip on error"),
  STOP("Stop on error");

  private final String value;

  ErrorHandling(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  /**
   * Converts error handling string value into {@link ErrorHandling} enum.
   *
   * @param stringValue error handling string value
   * @return error handling type in optional container
   */
  public static Optional<ErrorHandling> fromValue(String stringValue) {
    return Stream.of(values())
      .filter(keyType -> keyType.value.equalsIgnoreCase(stringValue))
      .findAny();
  }
}
