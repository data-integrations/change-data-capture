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

import java.util.Arrays;

/**
 * Contains information about change event. Should be used instead of {@link com.sforce.soap.partner.ChangeEventHeader}
 * because GSON does not support setters.
 */
public class ChangeEventHeader {
  private String[] recordIds;
  private String entityName;
  private ChangeEventType changeType;
  private String transactionKey;
  private boolean isTransactionEnd;

  public String[] getRecordIds() {
    return recordIds;
  }

  public void setRecordIds(String[] recordIds) {
    this.recordIds = recordIds.clone();
  }

  public String getEntityName() {
    return entityName;
  }

  public void setEntityName(String entityName) {
    this.entityName = entityName;
  }

  public ChangeEventType getChangeType() {
    return changeType;
  }

  public void setChangeType(ChangeEventType changeType) {
    this.changeType = changeType;
  }

  public String getTransactionKey() {
    return transactionKey;
  }

  public void setTransactionKey(String transactionKey) {
    this.transactionKey = transactionKey;
  }

  public boolean isTransactionEnd() {
    return isTransactionEnd;
  }

  public void setTransactionEnd(boolean transactionEnd) {
    isTransactionEnd = transactionEnd;
  }

  @Override
  public String toString() {
    return "ChangeEventHeader{" +
      "recordIds=" + Arrays.toString(recordIds) +
      ", entityName='" + entityName + '\'' +
      ", changeType=" + changeType +
      ", transactionKey='" + transactionKey + '\'' +
      ", isTransactionEnd=" + isTransactionEnd +
      '}';
  }
}
