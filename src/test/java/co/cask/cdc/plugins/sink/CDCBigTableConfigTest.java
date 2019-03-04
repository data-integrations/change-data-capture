package co.cask.cdc.plugins.sink;

import co.cask.cdap.etl.api.validation.InvalidConfigPropertyException;
import com.google.bigtable.repackaged.com.google.cloud.ServiceOptions;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;

public class CDCBigTableConfigTest {
  private static final String VALID_REF = "test-ref";
  private static final String VALID_PROJECT = "test-project";
  private static final String VALID_INSTANCE = "test-instance";
  private static final String VALID_ACCOUNT_FILE_PATH
    = CDCBigTableConfigTest.class.getResource("/credentials.json").getPath();

  @Test
  public void testValidateValidConfig() {
    CDCBigTableConfig config = new CDCBigTableConfig(
      VALID_REF,
      VALID_INSTANCE,
      VALID_PROJECT,
      VALID_ACCOUNT_FILE_PATH
    );

    Assertions.assertThatCode(config::validate)
      .doesNotThrowAnyException();
  }

  @Test
  public void testValidateReference() {
    CDCBigTableConfig config = new CDCBigTableConfig(
      "",
      VALID_INSTANCE,
      VALID_PROJECT,
      VALID_ACCOUNT_FILE_PATH
    );

    Assertions.assertThatThrownBy(config::validate)
      .isInstanceOf(InvalidConfigPropertyException.class);
  }

  @Test
  public void testValidateMissingCredentialsFile() {
    CDCBigTableConfig config = new CDCBigTableConfig(
      VALID_REF,
      VALID_INSTANCE,
      VALID_PROJECT,
      "/tmp/non_existing_file"
    );

    Assertions.assertThatThrownBy(config::validate)
      .isInstanceOf(InvalidConfigPropertyException.class);
  }

  @Test
  public void testValidateMissingProjectId() {
    Assume.assumeTrue(ServiceOptions.getDefaultProjectId() == null);

    CDCBigTableConfig config = new CDCBigTableConfig(
      VALID_REF,
      VALID_INSTANCE,
      null,
      VALID_ACCOUNT_FILE_PATH
    );

    Assertions.assertThatThrownBy(config::validate)
      .isInstanceOf(InvalidConfigPropertyException.class);
  }

  @Test
  public void testValidateMissingInstanceId() {
    CDCBigTableConfig config = new CDCBigTableConfig(
      VALID_REF,
      null,
      VALID_PROJECT,
      VALID_ACCOUNT_FILE_PATH
    );

    Assertions.assertThatThrownBy(config::validate)
      .isInstanceOf(InvalidConfigPropertyException.class);
  }

  @Test
  public void testResolveProjectId() {
    CDCBigTableConfig config = new CDCBigTableConfig(
      VALID_REF,
      VALID_INSTANCE,
      null,
      VALID_ACCOUNT_FILE_PATH
    );

    Assertions.assertThat(config.resolveProject())
      .isEqualTo(ServiceOptions.getDefaultProjectId());
  }

  @Test
  public void testResolveProjectIdAutoDetect() {
    CDCBigTableConfig config = new CDCBigTableConfig(
      VALID_REF,
      VALID_INSTANCE,
      CDCBigTableConfig.AUTO_DETECT,
      VALID_ACCOUNT_FILE_PATH
    );

    Assertions.assertThat(config.resolveProject())
      .isEqualTo(ServiceOptions.getDefaultProjectId());
  }

  @Test
  public void testServiceAccountFilePath() {
    CDCBigTableConfig config = new CDCBigTableConfig(
      VALID_REF,
      VALID_INSTANCE,
      VALID_PROJECT,
      null
    );

    Assertions.assertThat(config.resolveServiceAccountFilePath())
      .isNull();
  }

  @Test
  public void testServiceAccountFilePathAutoDetect() {
    CDCBigTableConfig config = new CDCBigTableConfig(
      VALID_REF,
      VALID_INSTANCE,
      VALID_PROJECT,
      CDCBigTableConfig.AUTO_DETECT
    );

    Assertions.assertThat(config.resolveServiceAccountFilePath())
      .isNull();
  }
}
