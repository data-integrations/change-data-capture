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

package io.cdap.plugin.cdc.source.salesforce;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cdc.common.ErrorHandling;
import io.cdap.plugin.cdc.common.OperationType;
import io.cdap.plugin.cdc.source.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.cdc.source.salesforce.records.ChangeEventRecord;
import io.cdap.plugin.cdc.source.salesforce.records.SalesforceRecord;
import io.cdap.plugin.cdc.source.salesforce.sobject.SObjectDescriptor;
import io.cdap.plugin.cdc.source.salesforce.sobject.SObjectsDescribeResult;
import io.cdap.plugin.cdc.source.salesforce.util.SalesforceConnectionUtil;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Implementation of Spark receiver to receive Salesforce change events
 */
public class SalesforceReceiver extends Receiver<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceReceiver.class);
  private static final String RECEIVER_THREAD_NAME = "salesforce_streaming_api_listener";
  // every x seconds thread wakes up and checks if stream is not yet stopped
  private static final long GET_MESSAGE_TIMEOUT_SECONDS = 2;

  private final AuthenticatorCredentials credentials;
  private final List<String> objectsForTracking;
  private final ErrorHandling errorHandling;

  private SalesforceEventTopicListener eventTopicListener;


  private Map<String, Schema> schemas = new HashMap<>();
  private Map<String, List<ChangeEventRecord>> events = new HashMap<>();

  SalesforceReceiver(AuthenticatorCredentials credentials, List<String> objectsForTracking,
                     ErrorHandling errorHandling) {
    super(StorageLevel.MEMORY_AND_DISK_2());
    this.credentials = credentials;
    this.objectsForTracking = objectsForTracking;
    this.errorHandling = errorHandling;
  }

  @Override
  public void onStart() {
    eventTopicListener = new SalesforceEventTopicListener(credentials, objectsForTracking);
    eventTopicListener.start();

    ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
      .setNameFormat(RECEIVER_THREAD_NAME + "-%d")
      .build();

    Executors.newSingleThreadExecutor(namedThreadFactory).submit(this::receive);
  }

  @Override
  public void onStop() {
    // There is nothing we can do here as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  private void receive() {
    PartnerConnection connection;
    try {
      connection = SalesforceConnectionUtil.getPartnerConnection(credentials);
    } catch (ConnectionException e) {
      throw new RuntimeException("Failed to connect to Salesforce", e);
    }

    while (!isStopped()) {
      try {
        String message = eventTopicListener.getMessage(GET_MESSAGE_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        if (message != null) {
          ChangeEventRecord record = ChangeEventRecord.fromJSON(message);

          List<ChangeEventRecord> eventsList = events.getOrDefault(record.getTransactionKey(), Collections.emptyList());
          eventsList.add(record);

          if (record.isTransactionEnd()) {
            processEvents(eventsList, connection);
            events.remove(record.getTransactionKey());
          } else {
            events.put(record.getTransactionKey(), eventsList);
          }
        }
      } catch (Exception e) {
        switch (errorHandling) {
          case SKIP:
            LOG.warn("Failed to process message, skipping it.", e);
            break;
          case STOP:
            throw new RuntimeException("Failed to process message", e);
          default:
            throw new UnexpectedFormatException(String.format("Unknown error handling strategy '%s'", errorHandling));
        }
      }
    }
  }

  private void processEvents(List<ChangeEventRecord> events, PartnerConnection connection) throws ConnectionException {
    for (ChangeEventRecord event : events) {
      SObjectDescriptor descriptor = SObjectDescriptor.fromName(event.getEntityName(), connection);
      SObjectsDescribeResult describeResult = new SObjectsDescribeResult(connection, descriptor.getAllParentObjects());

      Schema schema = SalesforceRecord.getSchema(descriptor, describeResult);
      updateSchemaIfNecessary(event.getEntityName(), schema);

      if (event.getOperationType() != OperationType.DELETE) {
        sendUpdateRecords(event, descriptor, schema, connection);
      } else {
        sendDeleteRecords(event.getIds(), event.getEntityName(), schema);
      }
    }
  }

  private void updateSchemaIfNecessary(String entityName, Schema schema) {
    Schema previousSchema = schemas.get(entityName);

    if (!schema.equals(previousSchema)) {
      StructuredRecord ddlRecord = SalesforceRecord.buildDDLStructuredRecord(entityName, schema);
      schemas.put(entityName, schema);

      LOG.debug("Sending ddl message for '{}'", entityName);
      store(ddlRecord);
    }
  }

  private void sendUpdateRecords(ChangeEventRecord event, SObjectDescriptor descriptor, Schema schema,
                                 PartnerConnection connection) throws ConnectionException {
    String query = getQuery(event, descriptor.getFieldsNames());
    QueryResult queryResult = connection.query(query);

    if (queryResult != null) {
      if (queryResult.getRecords().length < event.getIds().size() && !event.isWildcard()) {
        List<String> idsForDelete = findIdsMismatch(queryResult.getRecords(), event.getIds());
        sendDeleteRecords(idsForDelete, event.getEntityName(), schema);
      }

      for (SObject sObject : queryResult.getRecords()) {
        StructuredRecord dmlRecord = SalesforceRecord
          .buildDMLStructuredRecord(sObject.getId(), event.getEntityName(), schema, event.getOperationType(), sObject);

        LOG.debug("Sending dml message for '{}:{}'", event.getEntityName(), sObject.getId());
        store(dmlRecord);
      }
    }
  }

  private List<String> findIdsMismatch(SObject[] sObjectArray, List<String> ids) {
    Set<String> idsFromQuery = Arrays.stream(sObjectArray)
      .map(SObject::getId)
      .collect(Collectors.toSet());

    return ids.stream()
      .filter(id -> !idsFromQuery.contains(id))
      .collect(Collectors.toList());
  }

  private void sendDeleteRecords(List<String> ids, String entityName, Schema schema) {
    for (String id : ids) {
      StructuredRecord dmlRecord = SalesforceRecord
        .buildDMLStructuredRecord(id, entityName, schema, OperationType.DELETE, null);

      LOG.debug("Sending dml message for {}:{}", entityName, id);
      store(dmlRecord);
    }
  }

  private String getQuery(ChangeEventRecord event, List<String> fields) {
    String query = String.format("select %s from %s", String.join(",", fields), event.getEntityName());
    if (event.isWildcard()) {
      return query;
    } else {
      String ids = event
        .getIds()
        .stream()
        .map(id -> String.format("'%s'", id))
        .collect(Collectors.joining(","));
      return String.format("%s where id in (%s)", query, ids);
    }
  }
}
