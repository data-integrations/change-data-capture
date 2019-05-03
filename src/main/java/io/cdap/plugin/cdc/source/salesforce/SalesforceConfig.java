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

import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.cdc.common.CDCReferencePluginConfig;
import io.cdap.plugin.cdc.common.ErrorHandling;
import io.cdap.plugin.cdc.source.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.cdc.source.salesforce.util.SalesforceConnectionUtil;
import io.cdap.plugin.cdc.source.salesforce.util.SalesforceConstants;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Defines the {@link PluginConfig} for the {@link CDCSalesforce}.
 */
public class SalesforceConfig extends CDCReferencePluginConfig {
  private static final String OBJECTS_SEPARATOR = ",";

  @Name(SalesforceConstants.PROPERTY_CLIENT_ID)
  @Description("Salesforce connected app's client ID")
  @Macro
  private String clientId;

  @Name(SalesforceConstants.PROPERTY_CLIENT_SECRET)
  @Description("Salesforce connected app's client secret key")
  @Macro
  private String clientSecret;

  @Name(SalesforceConstants.PROPERTY_USERNAME)
  @Description("Salesforce username")
  @Macro
  private String username;

  @Name(SalesforceConstants.PROPERTY_PASSWORD)
  @Description("Salesforce password")
  @Macro
  private String password;

  @Name(SalesforceConstants.PROPERTY_LOGIN_URL)
  @Description("Endpoint to authenticate to")
  @Macro
  private String loginUrl;

  @Name(SalesforceConstants.PROPERTY_OBJECTS)
  @Description("Objects for tracking")
  @Macro
  @Nullable
  private String objects;

  @Name(SalesforceConstants.PROPERTY_ERROR_HANDLING)
  @Description("Strategy used to handle erroneous records. Acceptable values are Skip on error, Stop on error.\n" +
    "Skip on error - ignores erroneous record.\n" +
    "Stop on error - fails pipeline due to erroneous record.")
  @Macro
  private String errorHandling;

  public SalesforceConfig() {
    super("");
  }

  public SalesforceConfig(String referenceName, String clientId, String clientSecret,
                          String username, String password, String loginUrl, String objects, String errorHandling) {
    super(referenceName);
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.username = username;
    this.password = password;
    this.loginUrl = loginUrl;
    this.objects = objects;
    this.errorHandling = errorHandling;
  }

  public String getClientId() {
    return clientId;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getLoginUrl() {
    return loginUrl;
  }

  public List<String> getObjects() {
    if (objects == null || objects.isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.asList(objects.split(OBJECTS_SEPARATOR));
  }

  public ErrorHandling getErrorHandling() {
    return ErrorHandling.fromValue(errorHandling)
      .orElseThrow(() -> new InvalidConfigPropertyException("Unsupported error handling value: " + errorHandling,
                                                            SalesforceConstants.PROPERTY_ERROR_HANDLING));
  }

  @Override
  public void validate() {
    validateConnection();
    validateErrorHandling();
  }

  public AuthenticatorCredentials getAuthenticatorCredentials() {
    return SalesforceConnectionUtil.getAuthenticatorCredentials(username, password, clientId, clientSecret, loginUrl);
  }

  private void validateConnection() {
    if (containsMacro(SalesforceConstants.PROPERTY_CLIENT_ID)
      || containsMacro(SalesforceConstants.PROPERTY_CLIENT_SECRET)
      || containsMacro(SalesforceConstants.PROPERTY_USERNAME)
      || containsMacro(SalesforceConstants.PROPERTY_PASSWORD)
      || containsMacro(SalesforceConstants.PROPERTY_LOGIN_URL)) {
      return;
    }

    try {
      SalesforceConnectionUtil.getPartnerConnection(getAuthenticatorCredentials());
    } catch (ConnectionException e) {
      throw new InvalidStageException("Cannot connect to Salesforce API with credentials specified", e);
    }
  }

  private void validateErrorHandling() {
    if (containsMacro(SalesforceConstants.PROPERTY_ERROR_HANDLING)) {
      return;
    }

    getErrorHandling();
  }
}
