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
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.cdc.common.ErrorHandling;
import io.cdap.plugin.cdc.common.OperationType;
import io.cdap.plugin.cdc.source.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.cdc.source.salesforce.records.ChangeEventHeader;
import io.cdap.plugin.cdc.source.salesforce.records.SalesforceRecord;
import io.cdap.plugin.cdc.source.salesforce.sobject.SObjectDescriptor;
import io.cdap.plugin.cdc.source.salesforce.sobject.SObjectsDescribeResult;
import io.cdap.plugin.cdc.source.salesforce.util.SalesforceConnectionUtil;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implementation of Spark receiver to receive Salesforce change events from EventTopic using Bayeux Client.
 * Subscribes to all events if objectsForTracking is empty, otherwise subscribes to all topics in list.
 * Produces DML structured records depending on change event type. Also produces DDL record if change event entity type
 * is processed for the first time.
 */
public class SalesforceReceiver extends Receiver<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceReceiver.class);
  private static final String RECEIVER_THREAD_NAME = "salesforce_streaming_api_listener";
  // every x seconds thread wakes up and checks if stream is not yet stopped
  private static final long GET_MESSAGE_TIMEOUT_SECONDS = 2;
  private static final Gson GSON = new Gson();

  private final AuthenticatorCredentials credentials;
  private final List<String> objectsForTracking;
  private final ErrorHandling errorHandling;
  private final Map<String, Schema> schemas = new HashMap<>();
  private final Map<String, List<ChangeEventHeader>> events = new HashMap<>();
  private SalesforceEventTopicListener eventTopicListener;
  private static final JsonParser JSON_PARSER = new JsonParser();

  SalesforceReceiver(AuthenticatorCredentials credentials, List<String> objectsForTracking,
                     ErrorHandling errorHandling) {
    super(StorageLevel.MEMORY_AND_DISK_2());
    this.credentials = credentials;
    this.objectsForTracking = new ArrayList<>(objectsForTracking);
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
          // whole message class is not needed because we are interested only in change event payload
          JsonObject headerElement = JSON_PARSER.parse(message)
            .getAsJsonObject()
            .getAsJsonObject("data")
            .getAsJsonObject("payload")
            .getAsJsonObject("ChangeEventHeader");
          ChangeEventHeader event = GSON.fromJson(headerElement, ChangeEventHeader.class);

          List<ChangeEventHeader> eventsList = events.getOrDefault(event.getTransactionKey(), new ArrayList<>());
          eventsList.add(event);

          if (event.isTransactionEnd()) {
            processEvents(eventsList, connection);
            events.remove(event.getTransactionKey());
          } else {
            events.put(event.getTransactionKey(), eventsList);
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
            throw new IllegalStateException(String.format("Unknown error handling strategy '%s'", errorHandling));
        }
      }
    }
    eventTopicListener.stop();
  }

  private void processEvents(List<ChangeEventHeader> events, PartnerConnection connection) throws ConnectionException {
    for (ChangeEventHeader event : events) {
      SObjectDescriptor descriptor = SObjectDescriptor.fromName(event.getEntityName(), connection);
      SObjectsDescribeResult describeResult = new SObjectsDescribeResult(connection, descriptor.getAllParentObjects());

      Schema schema = SalesforceRecord.getSchema(descriptor, describeResult);
      updateSchemaIfNecessary(event.getEntityName(), schema);

      if (getOperationType(event) != OperationType.DELETE) {
        sendUpdateRecords(event, descriptor, schema, connection);
      } else {
        sendDeleteRecords(Arrays.asList(event.getRecordIds()), event.getEntityName(), schema);
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

  private void sendUpdateRecords(ChangeEventHeader event, SObjectDescriptor descriptor, Schema schema,
                                 PartnerConnection connection) throws ConnectionException {
    String query = getQuery(event, descriptor.getFieldsNames());
    QueryResult queryResult = connection.query(query);

    if (queryResult != null) {
      if (queryResult.getRecords().length < event.getRecordIds().length && !isWildcardEvent(event)) {
        List<String> idsForDelete = findIdsMismatch(queryResult.getRecords(), event.getRecordIds());
        sendDeleteRecords(idsForDelete, event.getEntityName(), schema);
      }

      for (SObject sObject : queryResult.getRecords()) {
        StructuredRecord dmlRecord = SalesforceRecord
          .buildDMLStructuredRecord(sObject.getId(), event.getEntityName(), schema, getOperationType(event), sObject);

        LOG.debug("Sending dml message for '{}:{}'", event.getEntityName(), sObject.getId());
        store(dmlRecord);
      }
    }
  }

  private List<String> findIdsMismatch(SObject[] sObjectArray, String[] ids) {
    Set<String> idsFromQuery = Arrays.stream(sObjectArray)
      .map(SObject::getId)
      .collect(Collectors.toSet());

    return Stream.of(ids)
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

  private String getQuery(ChangeEventHeader event, List<String> fields) {
    String query = String.format("select %s from %s", String.join(",", fields), event.getEntityName());
    if (isWildcardEvent(event)) {
      return query;
    } else {
      String ids = Stream.of(event.getRecordIds())
        .map(id -> String.format("'%s'", id))
        .collect(Collectors.joining(","));
      return String.format("%s where id in (%s)", query, ids);
    }
  }

  private static boolean isWildcardEvent(ChangeEventHeader event) {
    String[] ids = event.getRecordIds();
    return ids.length == 0 || ids.length == 1 && ids[0].charAt(3) == '*';
  }

  private static OperationType getOperationType(ChangeEventHeader event) {
    switch (event.getChangeType()) {
      case CREATE:
      case GAP_CREATE:
      case UNDELETE:
      case GAP_UNDELETE:
        return OperationType.INSERT;
      case UPDATE:
      case GAP_UPDATE:
      case GAP_OVERFLOW:
        return OperationType.UPDATE;
      case DELETE:
      case GAP_DELETE:
        return OperationType.DELETE;
    }
    throw new IllegalArgumentException(String.format("Unknown change operation '%s'", event.getChangeType()));
  }
}
