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

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

/**
 * Oauth2 response from salesforce server
 */
public class AuthResponse {
  @SerializedName("access_token")
  private final String accessToken;
  @SerializedName("instance_url")
  private final String instanceUrl;
  private final String id;
  @SerializedName("token_type")
  private final String tokenType;
  @SerializedName("issued_at")
  private final String issuedAt;
  private final String signature;

  public AuthResponse(String accessToken, String instanceUrl, String id, String tokenType,
                      String issuedAt, String signature) {
    this.accessToken = accessToken;
    this.instanceUrl = instanceUrl;
    this.id = id;
    this.tokenType = tokenType;
    this.issuedAt = issuedAt;
    this.signature = signature;
  }

  public String getAccessToken() {
    return accessToken;
  }

  public String getInstanceUrl() {
    return instanceUrl;
  }

  public String getId() {
    return id;
  }

  public String getTokenType() {
    return tokenType;
  }

  public String getIssuedAt() {
    return issuedAt;
  }

  public String getSignature() {
    return signature;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AuthResponse that = (AuthResponse) o;

    return (Objects.equals(accessToken, that.accessToken) &&
      Objects.equals(instanceUrl, that.instanceUrl) &&
      Objects.equals(id, that.id) &&
      Objects.equals(tokenType, that.tokenType) &&
      Objects.equals(issuedAt, that.issuedAt) &&
      Objects.equals(signature, that.signature));
  }

  @Override
  public int hashCode() {
    return Objects.hash(accessToken, instanceUrl, id, tokenType, issuedAt, signature);
  }
}
