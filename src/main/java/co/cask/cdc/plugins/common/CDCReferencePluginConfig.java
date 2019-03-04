package co.cask.cdc.plugins.common;

import co.cask.cdap.etl.api.validation.InvalidConfigPropertyException;
import co.cask.hydrator.common.Constants;
import co.cask.hydrator.common.ReferencePluginConfig;

/**
 * {@link ReferencePluginConfig} with reference name validation.
 */
public class CDCReferencePluginConfig extends ReferencePluginConfig {
  public CDCReferencePluginConfig(String referenceName) {
    super(referenceName);
  }

  public void validate() {
    if (!containsMacro(Constants.Reference.REFERENCE_NAME) && !References.isValid(referenceName)) {
      String message = String.format("%s is not a valid id. Allowed characters are letters, numbers, " +
                                       "and _, -, ., or $.", referenceName);
      throw new InvalidConfigPropertyException(message, Constants.Reference.REFERENCE_NAME);
    }
  }
}
