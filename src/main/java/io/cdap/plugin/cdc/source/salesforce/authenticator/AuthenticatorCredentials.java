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

import java.io.Serializable;
import java.util.Objects;

/**
 * Stores information to connect to salesforce via oauth2
 */
public class AuthenticatorCredentials implements Serializable {
  private final String username;
  private final String password;
  private final String clientId;
  private final String clientSecret;
  private final String loginUrl;

  public AuthenticatorCredentials(String username, String password,
                                  String clientId, String clientSecret, String loginUrl) {
    this.username = username;
    this.password = password;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.loginUrl = loginUrl;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getClientId() {
    return clientId;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public String getLoginUrl() {
    return loginUrl;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AuthenticatorCredentials that = (AuthenticatorCredentials) o;

    return Objects.equals(username, that.username) &&
      Objects.equals(password, that.password) &&
      Objects.equals(clientId, that.clientId) &&
      Objects.equals(clientSecret, that.clientSecret) &&
      Objects.equals(loginUrl, that.loginUrl);
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, password, clientId, clientSecret, loginUrl);
  }
}
