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

package io.cdap.plugin.cdc.source.salesforce.authenticator;

import com.google.gson.Gson;
import com.sforce.ws.ConnectorConfig;
import io.cdap.plugin.cdc.source.salesforce.util.SalesforceConstants;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * Authentication to Salesforce via oauth2
 */
public class Authenticator {
  private static final Gson GSON = new Gson();

  /**
   * Authenticates via oauth2 to salesforce and returns a connectorConfig
   * which can be used by salesforce libraries to make a connection.
   *
   * @param credentials information to log in
   * @return ConnectorConfig which can be used to create BulkConnection and PartnerConnection
   */
  public static ConnectorConfig createConnectorConfig(AuthenticatorCredentials credentials) {
    try {
      AuthResponse authResponse = oauthLogin(credentials);
      ConnectorConfig connectorConfig = new ConnectorConfig();
      connectorConfig.setSessionId(authResponse.getAccessToken());
      String apiVersion = SalesforceConstants.API_VERSION;
      String restEndpoint = String.format("%s/services/async/%s", authResponse.getInstanceUrl(), apiVersion);
      String serviceEndPoint = String.format("%s/services/Soap/u/%s", authResponse.getInstanceUrl(), apiVersion);
      connectorConfig.setRestEndpoint(restEndpoint);
      connectorConfig.setServiceEndpoint(serviceEndPoint);
      // This should only be false when doing debugging.
      connectorConfig.setCompression(true);
      // Set this to true to see HTTP requests and responses on stdout
      connectorConfig.setTraceMessage(false);
      return connectorConfig;
    } catch (Exception e) {
      throw new RuntimeException("Connection to Salesforce with plugin configurations failed", e);
    }
  }

  /**
   * Authenticate via oauth2 to salesforce and return response to auth request.
   *
   * @param credentials information to log in
   * @return AuthResponse response to http request
   */
  public static AuthResponse oauthLogin(AuthenticatorCredentials credentials) throws Exception {
    SslContextFactory sslContextFactory = new SslContextFactory();
    HttpClient httpClient = new HttpClient(sslContextFactory);
    try {
      httpClient.start();
      String response = httpClient.POST(credentials.getLoginUrl()).param("grant_type", "password")
        .param("client_id", credentials.getClientId())
        .param("client_secret", credentials.getClientSecret())
        .param("username", credentials.getUsername())
        .param("password", credentials.getPassword()).send().getContentAsString();
      return GSON.fromJson(response, AuthResponse.class);
    } finally {
      httpClient.stop();
    }
  }
}
