package co.cask.cdc.plugins.sink;

import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.common.IdUtils;
import co.cask.hydrator.common.ReferencePluginConfig;

/**
 * Defines the {@link PluginConfig} for the {@link CDCHBase}.
 */
public class CDCHBaseConfig extends ReferencePluginConfig {
  public CDCHBaseConfig(String referenceName) {
    super(referenceName);
  }

  public void validate() {
    IdUtils.validateId(referenceName);
  }
}
