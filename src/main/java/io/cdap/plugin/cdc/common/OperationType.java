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

/**
 * Represents change operation type.
 */
public enum OperationType {
  INSERT, UPDATE, DELETE;

  public static OperationType fromShortName(String name) {
    switch (name.toUpperCase()) {
      case "I":
        return INSERT;
      case "U":
        return UPDATE;
      case "D":
        return DELETE;
      default:
        throw new IllegalArgumentException(String.format("Unknown change operation '%s'", name));
    }
  }
}
