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

package io.cdap.plugin.cdc;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Extracts the DML record from the output of a cdc source for direct manipulation.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("DMLFlattener")
@Description("Flattens DML records output by a CDC source.")
public class DMLFlattener extends Transform<StructuredRecord, StructuredRecord> {
  private static final String OP_TYPE = "CDC_OP_TYPE";
  private static final String CHANGE_TRACKING_VERSION = "CHANGE_TRACKING_VERSION";
  private final Conf conf;
  private Map<Schema, Schema> schemaCache;
  private Schema configuredOutputSchema;
  private boolean addOpType = false;
  private boolean addTrackingVersion = false;

  public DMLFlattener(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    if (conf.schema != null) {
      try {
        pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.parseJson(conf.schema));
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse configured schema: " + e.getMessage(), e);
      }
    }
  }

  @Override
  public void initialize(TransformContext context) throws IOException {
    configuredOutputSchema = conf.schema == null ? null : Schema.parseJson(conf.schema);
    addOpType = configuredOutputSchema.getField(OP_TYPE) != null;
    addTrackingVersion = configuredOutputSchema.getField(CHANGE_TRACKING_VERSION) != null;
    schemaCache = new HashMap<>();
  }

  @Override
  public void transform(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord dml = record.get("dml");
    if (dml == null) {
      return;
    }

    Schema rowSchema = Schema.parseJson((String) dml.get("rows_schema"));
    Schema outputSchema = schemaCache.computeIfAbsent(rowSchema, this::createOutputSchema);

    StructuredRecord.Builder output = StructuredRecord.builder(outputSchema);
    if (addOpType) {
      output.set(OP_TYPE, dml.get("op_type").toString());
    }
    if (addTrackingVersion) {
      output.set(CHANGE_TRACKING_VERSION, dml.get("change_tracking_version"));
    }
    Map<String, Object> valueMap = dml.get("rows_values");
    if (valueMap == null) {
      valueMap = new HashMap<>();
    }
    for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
      output.set(entry.getKey(), entry.getValue());
    }
    emitter.emit(output.build());
  }

  private Schema createOutputSchema(Schema rowSchema) {
    // the transform optionally adds a OP_TYPE field and CHANGE_TRACKING_VERSION field that do not come from the
    // actual row data, but from general change tracking information.
    int numFields = rowSchema.getFields().size() + (addOpType ? 1 : 0) + (addTrackingVersion ? 1 : 0);
    List<Schema.Field> fields = new ArrayList<>(numFields);
    fields.addAll(rowSchema.getFields());
    if (addOpType) {
      fields.add(Schema.Field.of(OP_TYPE, Schema.of(Schema.Type.STRING)));
    }
    if (addTrackingVersion) {
      fields.add(Schema.Field.of(CHANGE_TRACKING_VERSION, Schema.of(Schema.Type.STRING)));
    }
    return Schema.recordOf(rowSchema + ".added", fields);
  }

  /**
   * plugin config.
   */
  public static class Conf extends PluginConfig {

    @Nullable
    @Description("The output schema of DML records. This should only be set if the source has been configured to read "
      + "from a single table whose schema will never change.")
    private String schema;
  }
}
