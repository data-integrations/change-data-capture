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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.plugin.cdc.common.Schemas;
import io.cdap.plugin.common.Constants;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming source for reading from Salesforce CDC plugin.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("CDCSalesforce")
@Description("CDC Salesforce Streaming Source")
public class CDCSalesforce extends StreamingSource<StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(CDCSalesforce.class);
  private final SalesforceConfig config;

  public CDCSalesforce(SalesforceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    LOG.info("Creating connection with url '{}', username '{}', clientId '{}'",
             config.getLoginUrl(), config.getUsername(), config.getClientId());
    config.validate();

    pipelineConfigurer.createDataset(config.referenceName, Constants.EXTERNAL_DATASET_TYPE,
                                     DatasetProperties.EMPTY);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(Schemas.CHANGE_SCHEMA);
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) {
    config.validate();

    SalesforceReceiver salesforceReceiver
      = new SalesforceReceiver(config.getAuthenticatorCredentials(), config.getObjects(), config.getErrorHandling());
    return context.getSparkStreamingContext()
      .receiverStream(salesforceReceiver)
      .map(Schemas::toCDCRecord);
  }
}
