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

package io.cdap.plugin.cdc.transform;

import com.google.cloud.Timestamp;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import org.apache.spark.streaming.dstream.DStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(CDCStreamingToRecordTransform.NAME)
@Description("Transforms CDC record to normal record")
public class CDCStreamingToRecordTransform extends Transform<StructuredRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(CDCStreamingToRecordTransform.class);
  public static final String NAME = "CDCStreamingToRecordTransform";
  private final Config config;

  @VisibleForTesting
  public CDCStreamingToRecordTransform(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    config.validate(inputSchema);
    try {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.parseJson(config.schema));
    } catch (IOException e) {
      throw new IllegalArgumentException("Output schema cannot be parsed.", e);
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {

    for (Schema.Field field : input.getSchema().getFields()) {
      String name = field.getName();
      //LOG.info("Local field name " + field.getName());
      //LOG.info("Field type " + field.getSchema().toString());

      if (input.get(name) != null && name.equals("dml")) {

        //LOG.info("Preparing record");
        //LOG.info(input.get(name).toString());
        StructuredRecord sr = input.get(name);

        try {
          StructuredRecord out = getDMLRecord(sr);
          emitter.emit(out);
        } catch (Exception ex) {
        }

      }
    }

  }

  private StructuredRecord getDMLRecord(StructuredRecord input) throws Exception {

    Schema outputSchema = null;
    Schema outSchema = null;

    for (Schema.Field field : input.getSchema().getFields()) {
      String name = field.getName();

      if (name.equals("rows_schema")) {

        outSchema = Schema.parseJson(input.get(name).toString());

      }
    }

    List<Schema.Field> fields = new ArrayList<>(outSchema.getFields().size() + 1);
    fields.addAll(outSchema.getFields());
    fields.add(Schema.Field.of("CDC_OP_TYPE", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    fields.add(Schema.Field.of("CHANGE_TRACKING_VERSION", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    outputSchema = Schema.recordOf(outSchema.getRecordName() + ".added", fields);

    //LOG.info("OutputSchema of new record " + outputSchema.toString());

    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);

    try {

      for (Schema.Field field : input.getSchema().getFields()) {

        String name = field.getName();

        if (name.equals("rows_values")) {
          Gson gson = new Gson();
          JsonReader reader = new JsonReader(new StringReader(input.get("rows_values").toString()));
          reader.setLenient(true);
          Map<String, Object> rowsValues = new HashMap<>();
          rowsValues = gson.fromJson(reader, Map.class);


          for (Map.Entry<String, Object> entry : rowsValues.entrySet()) {

            String type = "string";

            for (Schema.Field fieldtype : outSchema.getFields()) {
              if (fieldtype.getName().equals(entry.getKey())) {

                type = fieldtype.getSchema().toString().split("\"")[1];
                if (type.equals("type")) {
                  type = fieldtype.getSchema().toString().split("\"")[3];
                  if (fieldtype.getSchema().toString().split("\"")[5].equals("logicalType")) {
                    type = fieldtype.getSchema().toString().split("\"")[7];
                  }
                }
              }
            }

            type = type.trim().toLowerCase();

            if (entry.getValue() == null || entry.getValue().toString() == null) {
              builder.set(entry.getKey(), null);
            } else if (type.equals("long")) {
              builder.set(entry.getKey(), Long.parseLong(entry.getValue().toString()));
            } else if (type.equals("double")) {
              builder.set(entry.getKey(), Double.parseDouble(entry.getValue().toString()));
            } else if (type.equals("float")) {
              builder.set(entry.getKey(), Float.parseFloat(entry.getValue().toString()));
            } else if (type.equals("boolean")) {
              builder.set(entry.getKey(), Boolean.parseBoolean(entry.getValue().toString()));
            } else if (type.equals("timestamp-micros")) {
              //why multiply by 1000? timestamp type is in micros but the value is in millis.
              builder.set(entry.getKey(), Long.parseLong(entry.getValue().toString()) * 1000);
            } else {
              builder.set(entry.getKey(), entry.getValue());
            }
          }
        }

        if (name.equals("op_type")) {
          builder.set("CDC_OP_TYPE", input.get(name).toString());
        }
        if (name.equals("change_tracking_version")) {
          builder.set("CHANGE_TRACKING_VERSION", input.get(name).toString());
        }
      }
    } catch (Exception ex) {
      LOG.info(input.get("rows_values").toString());
      LOG.error(ex.getMessage());
    }

    return builder.build();
  }

  @Override
  public void destroy() {
  }

  /**
   *
   */
  public static class Config extends PluginConfig {
    @Name("schema")
    @Description("Specifies the schema of the records outputted from this plugin.")
    private final String schema;

    public Config(String schema) {
      this.schema = schema;
    }

    private void validate(Schema inputSchema) throws IllegalArgumentException {

    }
  }


}

