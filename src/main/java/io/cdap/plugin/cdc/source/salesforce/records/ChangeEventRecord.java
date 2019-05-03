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

package io.cdap.plugin.cdc.source.salesforce.records;

import io.cdap.plugin.cdc.common.OperationType;
import org.json.JSONObject;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Contains information about change event
 */
public class ChangeEventRecord {
  private final List<String> ids;
  private final String entityName;
  private final OperationType operationType;
  private final String transactionKey;
  private final boolean transactionEnd;
  private final boolean wildcard;

  public ChangeEventRecord(List<String> ids, String entityName, OperationType operationType, String transactionKey,
                           boolean transactionEnd) {
    this.ids = ids;
    this.entityName = entityName;
    this.operationType = operationType;
    this.transactionKey = transactionKey;
    this.transactionEnd = transactionEnd;
    wildcard = parseWildcard(ids);
  }

  /**
   * Parses json to change event record
   *
   * @param json JSON with change event
   * @return parsed change event record
   */
  public static ChangeEventRecord fromJSON(String json) {
    JSONObject message = new JSONObject(json);
    return new ChangeEventRecord(
      parseIds(message),
      parseEntityName(message),
      parseChangeOperation(message),
      parseTransactionKey(message),
      parseTransactionEnd(message)
    );
  }

  public List<String> getIds() {
    return ids;
  }

  public String getEntityName() {
    return entityName;
  }

  public OperationType getOperationType() {
    return operationType;
  }

  public String getTransactionKey() {
    return transactionKey;
  }

  public boolean isTransactionEnd() {
    return transactionEnd;
  }

  public boolean isWildcard() {
    return wildcard;
  }

  private static String parseEntityName(JSONObject message) {
    return message
      .getJSONObject("data")
      .getJSONObject("payload")
      .getJSONObject("ChangeEventHeader")
      .getString("entityName");
  }

  private static List<String> parseIds(JSONObject message) {
    return message
      .getJSONObject("data")
      .getJSONObject("payload")
      .getJSONObject("ChangeEventHeader")
      .getJSONArray("recordIds").toList()
      .stream()
      .map(Object::toString)
      .collect(Collectors.toList());
  }

  private static boolean parseWildcard(List<String> ids) {
    return ids.isEmpty() || ids.size() == 1 && ids.get(0).charAt(3) == '*';
  }

  private static OperationType parseChangeOperation(JSONObject message) {
    String operation = message
      .getJSONObject("data")
      .getJSONObject("payload")
      .getJSONObject("ChangeEventHeader")
      .getString("changeType");
    switch (operation) {
      case "CREATE":
      case "GAP_CREATE":
      case "UNDELETE":
      case "GAP_UNDELETE":
        return OperationType.INSERT;
      case "UPDATE":
      case "GAP_UPDATE":
      case "GAP_OVERFLOW":
        return OperationType.UPDATE;
      case "DELETE":
      case "GAP_DELETE":
        return OperationType.DELETE;
    }
    throw new IllegalArgumentException(String.format("Unknown change operation '%s'", operation));
  }

  private static String parseTransactionKey(JSONObject message) {
    return message
      .getJSONObject("data")
      .getJSONObject("payload")
      .getJSONObject("ChangeEventHeader")
      .getString("transactionKey");
  }

  private static boolean parseTransactionEnd(JSONObject message) {
    return message
      .getJSONObject("data")
      .getJSONObject("payload")
      .getJSONObject("ChangeEventHeader")
      .getBoolean("isTransactionEnd");
  }
}
