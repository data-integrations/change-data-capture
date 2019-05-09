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

package io.cdap.plugin.cdc.source.salesforce.util;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import io.cdap.plugin.cdc.source.salesforce.authenticator.Authenticator;
import io.cdap.plugin.cdc.source.salesforce.authenticator.AuthenticatorCredentials;

/**
 * Utility class which provides methods to establish connection with Salesforce.
 */
public class SalesforceConnectionUtil {

  /**
   * Based on given Salesforce credentials, attempt to establish {@link PartnerConnection}.
   * This is mainly used to obtain sObject describe results.
   *
   * @param credentials Salesforce credentials
   * @return partner connection instance
   * @throws ConnectionException in case error when establishing connection
   */
  public static PartnerConnection getPartnerConnection(AuthenticatorCredentials credentials)
    throws ConnectionException {
    ConnectorConfig connectorConfig = Authenticator.createConnectorConfig(credentials);
    return Connector.newConnection(connectorConfig);
  }

  /**
   * Creates {@link AuthenticatorCredentials} instance based on given parameters.
   *
   * @param username     Salesforce username
   * @param password     Salesforce password
   * @param clientId     Salesforce client id
   * @param clientSecret Salesforce client secret
   * @param loginUrl     Salesforce authentication url
   * @return authenticator credentials
   */
  public static AuthenticatorCredentials getAuthenticatorCredentials(String username, String password, String clientId,
                                                                     String clientSecret, String loginUrl) {
    return new AuthenticatorCredentials(username, password, clientId, clientSecret, loginUrl);
  }
}
