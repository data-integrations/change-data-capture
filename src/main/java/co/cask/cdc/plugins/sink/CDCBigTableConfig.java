package co.cask.cdc.plugins.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.validation.InvalidConfigPropertyException;
import co.cask.hydrator.common.IdUtils;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.google.cloud.ServiceOptions;

import java.io.File;
import javax.annotation.Nullable;

/**
 * Defines the {@link PluginConfig} for the {@link CDCBigTable}.
 */
public class CDCBigTableConfig extends ReferencePluginConfig {
  public static final String AUTO_DETECT = "auto-detect";

  public static final String INSTANCE = "instance";
  public static final String PROJECT = "project";
  public static final String SERVICE_ACCOUNT_FILE_PATH = "serviceFilePath";

  @Name(INSTANCE)
  @Description("BigTable instance id. " +
    "Uniquely identifies BigTable instance within your Google Cloud Platform project.")
  @Macro
  public final String instance;

  @Name(PROJECT)
  @Description("Google Cloud Project ID, which uniquely identifies a project. "
    + "It can be found on the Dashboard in the Google Cloud Platform Console.")
  @Macro
  @Nullable
  public final String project;

  @Name(SERVICE_ACCOUNT_FILE_PATH)
  @Description("Path on the local file system of the service account key used "
    + "for authorization. Can be set to 'auto-detect' when running on a Dataproc cluster. "
    + "When running on other clusters, the file must be present on every node in the cluster.")
  @Macro
  @Nullable
  public final String serviceAccountFilePath;

  public CDCBigTableConfig(String referenceName, String instance, @Nullable String project,
                           @Nullable String serviceAccountFilePath) {
    super(referenceName);
    this.instance = instance;
    this.project = project;
    this.serviceAccountFilePath = serviceAccountFilePath;
  }

  @Nullable
  public String resolveProject() {
    if (project == null || project.isEmpty() || AUTO_DETECT.equals(project)) {
      return ServiceOptions.getDefaultProjectId();
    }
    return project;
  }

  @Nullable
  public String resolveServiceAccountFilePath() {
    if (serviceAccountFilePath == null || serviceAccountFilePath.isEmpty()
      || AUTO_DETECT.equals(serviceAccountFilePath)) {
      return null;
    }
    return serviceAccountFilePath;
  }

  public void validate() {
    IdUtils.validateId(referenceName);
    if (!containsMacro(PROJECT) && resolveProject() == null) {
      throw new InvalidConfigPropertyException("Could not detect Google Cloud project id from the environment. " +
                                                 "Please specify a project id.", PROJECT);
    }
    String serviceAccountFilePath = resolveServiceAccountFilePath();
    if (!containsMacro(SERVICE_ACCOUNT_FILE_PATH) && serviceAccountFilePath != null) {
      File serviceAccountFile = new File(serviceAccountFilePath);
      if (!serviceAccountFile.exists()) {
        throw new InvalidConfigPropertyException(String.format("File '%s' does not exist", serviceAccountFilePath),
                                                 SERVICE_ACCOUNT_FILE_PATH);
      }
    }
  }
}
