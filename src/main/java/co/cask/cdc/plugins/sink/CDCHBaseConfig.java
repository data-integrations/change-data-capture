package co.cask.cdc.plugins.sink;

import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdc.plugins.common.CDCReferencePluginConfig;

/**
 * Defines the {@link PluginConfig} for the {@link CDCHBase}.
 */
public class CDCHBaseConfig extends CDCReferencePluginConfig {
  public CDCHBaseConfig(String referenceName) {
    super(referenceName);
  }
}
