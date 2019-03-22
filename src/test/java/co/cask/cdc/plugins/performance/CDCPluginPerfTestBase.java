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

package co.cask.cdc.plugins.performance;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.datastreams.DataStreamsSparkLauncher;
import co.cask.cdap.etl.proto.v2.DataStreamsConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.IntegrationTestBase;
import co.cask.cdap.test.SparkManager;

import java.io.IOException;

public abstract class CDCPluginPerfTestBase extends IntegrationTestBase {
  protected SparkManager deployETL(ETLPlugin sourcePlugin, ETLPlugin sinkPlugin, String appName) throws Exception {
    ETLStage source = new ETLStage("source", sourcePlugin);
    ETLStage sink = new ETLStage("sink", sinkPlugin);
    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .setBatchInterval("1s")
      .build();

    AppRequest<DataStreamsConfig> appRequest = getStreamingAppRequest(etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    ApplicationManager applicationManager = deployApplication(appId, appRequest);
    return applicationManager.getSparkManager(DataStreamsSparkLauncher.NAME);
  }

  private AppRequest<DataStreamsConfig> getStreamingAppRequest(DataStreamsConfig config)
    throws IOException, UnauthenticatedException {
    String version = getMetaClient().getVersion().getVersion();
    return new AppRequest<>(new ArtifactSummary("cdap-data-streams", version, ArtifactScope.SYSTEM), config);
  }
}
