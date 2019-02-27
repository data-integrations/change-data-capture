package co.cask.cdc.plugins.integration;

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.datastreams.DataStreamsApp;
import co.cask.cdap.datastreams.DataStreamsSparkLauncher;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.DataStreamsConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.etl.spark.Compat;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdc.plugins.sink.CDCBigTable;
import co.cask.cdc.plugins.sink.CDCHBase;
import co.cask.cdc.plugins.sink.CDCKudu;
import co.cask.cdc.plugins.source.oracle.GoldenGateKafka;
import co.cask.cdc.plugins.source.sqlserver.CTSQLServer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CDCPluginTestBase extends HydratorTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(CDCPluginTestBase.class);
  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-streams", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-streams", "1.0.0");

  @ClassRule
  public static final TestConfiguration CONFIG =
    new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                          Constants.AppFabric.SPARK_COMPAT, Compat.SPARK_COMPAT);

  private static int startCount;

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    LOG.info("Setting up application");

    setupStreamingArtifacts(APP_ARTIFACT_ID, DataStreamsApp.class);

    LOG.info("Setting up plugins");

    addPluginArtifact(NamespaceId.DEFAULT.artifact("cdc-plugins", "1.0.0"),
                      APP_ARTIFACT_ID,
                      GoldenGateKafka.class, CTSQLServer.class,
                      CDCBigTable.class, CDCHBase.class, CDCKudu.class);
  }

  protected SparkManager deployETL(ETLPlugin sourcePlugin, ETLPlugin sinkPlugin, String appName)
    throws Exception {
    ETLStage source = new ETLStage("source", sourcePlugin);
    ETLStage sink = new ETLStage("sink", sinkPlugin);
    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .setBatchInterval("1s")
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    ApplicationManager applicationManager = deployApplication(appId, appRequest);
    return getProgramManager(applicationManager);
  }

  private SparkManager getProgramManager(ApplicationManager appManager) {
    return appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
  }
}
