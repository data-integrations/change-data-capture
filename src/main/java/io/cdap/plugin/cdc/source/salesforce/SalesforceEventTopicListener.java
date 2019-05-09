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

import io.cdap.plugin.cdc.source.salesforce.authenticator.AuthResponse;
import io.cdap.plugin.cdc.source.salesforce.authenticator.Authenticator;
import io.cdap.plugin.cdc.source.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.cdc.source.salesforce.util.SalesforceConstants;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * Listens to a specific Salesforce eventTopics and adds messages to the blocking queue,
 * which can be read by a user of the class.
 */
public class SalesforceEventTopicListener {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceEventTopicListener.class);

  private static final String DEFAULT_EVENT_ENDPOINT = "/cometd/" + SalesforceConstants.API_VERSION;
  /**
   * Timeout of 110 seconds is enforced by Salesforce Streaming API and is not configurable.
   * So we enforce the same on client.
   */
  private static final int CONNECTION_TIMEOUT = 110;
  private static final long HANDSHAKE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(110);

  private static final int HANDSHAKE_CHECK_INTERVAL_MS = 1000;

  private static final String BASE_EVENT_TOPIC = "/data/ChangeEvents";
  private static final String EVENT_TOPIC_PATTERN = "/data/%sChangeEvent";

  // store message string not JSONObject, since it's not serializable for later Spark usage
  private final BlockingQueue<String> messagesQueue = new LinkedBlockingQueue<>();

  private final AuthenticatorCredentials credentials;
  private final List<String> objectsForTracking;
  private BayeuxClient bayeuxClient;

  public SalesforceEventTopicListener(AuthenticatorCredentials credentials, List<String> objectsForTracking) {
    this.credentials = credentials;
    this.objectsForTracking = new ArrayList<>(objectsForTracking);
  }

  /**
   * Start the Bayeux Client which listens to the Salesforce EventTopic and saves received messages
   * to the queue.
   */
  public void start() {
    try {
      bayeuxClient = getClient(credentials);
      waitForHandshake(bayeuxClient);
      LOG.debug("Client handshake done");

      ClientSessionChannel.MessageListener messageListener = (channel, message) -> messagesQueue.add(message.getJSON());
      if (objectsForTracking.isEmpty()) {
        LOG.debug("Subscribe on '{}'", BASE_EVENT_TOPIC);
        bayeuxClient.getChannel(BASE_EVENT_TOPIC)
          .subscribe(messageListener);
      } else {
        for (String objectName : objectsForTracking) {
          String topic = getObjectTopic(objectName);
          LOG.debug("Subscribe on '{}'", topic);
          bayeuxClient.getChannel(topic)
            .subscribe(messageListener);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Could not start client", e);
    }
  }

  /**
   * Stop listening to the Salesforce EventTopic.
   */
  public void stop() {
    if (bayeuxClient != null) {
      bayeuxClient.disconnect(100);
    }
  }

  /**
   * Retrieves message from the messages queue, waiting up to the
   * specified wait time if necessary for an element to become available.
   *
   * @param timeout how long to wait before giving up
   * @param unit    timeunit of timeout
   * @return the message, or {@code null} if the specified
   * waiting time elapses before an element is available
   * @throws InterruptedException blocking call is interrupted
   */
  public String getMessage(long timeout, TimeUnit unit) throws InterruptedException {
    return messagesQueue.poll(timeout, unit);
  }

  private String getObjectTopic(String objectName) {
    String name = objectName.endsWith("__c") ? objectName.substring(0, objectName.length() - 1) : objectName;
    return format(EVENT_TOPIC_PATTERN, name);
  }

  private BayeuxClient getClient(AuthenticatorCredentials credentials) throws Exception {
    AuthResponse authResponse = Authenticator.oauthLogin(credentials);
    String acessToken = authResponse.getAccessToken();
    String instanceUrl = authResponse.getInstanceUrl();

    SslContextFactory sslContextFactory = new SslContextFactory();

    // Set up a Jetty HTTP client to use with CometD
    HttpClient httpClient = new HttpClient(sslContextFactory);
    httpClient.setConnectTimeout(CONNECTION_TIMEOUT);
    httpClient.start();

    Map<String, Object> options = new HashMap<>();
    // Adds the OAuth header in LongPollingTransport
    LongPollingTransport transport = new LongPollingTransport(options, httpClient) {
      @Override
      protected void customize(Request exchange) {
        super.customize(exchange);
        exchange.header("Authorization", "OAuth " + acessToken);
      }
    };

    // Now set up the Bayeux client itself
    BayeuxClient client = new BayeuxClient(instanceUrl + DEFAULT_EVENT_ENDPOINT, transport);
    client.handshake();

    return client;
  }


  private void waitForHandshake(BayeuxClient client) {
    try {
      Awaitility.await()
        .atMost(HANDSHAKE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .pollInterval(HANDSHAKE_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS)
        .until(client::isHandshook);
    } catch (ConditionTimeoutException e) {
      throw new IllegalStateException("Client could not handshake with Salesforce server", e);
    }
  }
}
