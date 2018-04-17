/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdc.plugins.common;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Helper class to convert Avro types into CDAP types
 */
public final class AvroConverter {

  private static final Function<org.apache.avro.Schema, Schema> SCHEMA_CONVERTER =
    new Function<org.apache.avro.Schema, Schema>() {
      @Override
      public Schema apply(org.apache.avro.Schema input) {
        return fromAvroSchema(input);
      }
    };

  private static final Function<org.apache.avro.Schema.Field, Schema.Field> FIELD_CONVERTER =
    new Function<org.apache.avro.Schema.Field, Schema.Field>() {
      @Override
      public Schema.Field apply(org.apache.avro.Schema.Field input) {
        return Schema.Field.of(input.name(), SCHEMA_CONVERTER.apply(input.schema()));
      }
    };


  /**
   * Creates a CDAP {@link Schema} from an avro {@link org.apache.avro.Schema}.
   */
  public static Schema fromAvroSchema(org.apache.avro.Schema avroSchema) {
    switch (avroSchema.getType()) {
      case NULL:
        return Schema.of(Schema.Type.NULL);
      case BOOLEAN:
        return Schema.of(Schema.Type.BOOLEAN);
      case INT:
        return Schema.of(Schema.Type.INT);
      case LONG:
        return Schema.of(Schema.Type.LONG);
      case FLOAT:
        return Schema.of(Schema.Type.FLOAT);
      case DOUBLE:
        return Schema.of(Schema.Type.DOUBLE);
      case STRING:
        return Schema.of(Schema.Type.STRING);
      case BYTES:
        return Schema.of(Schema.Type.BYTES);
      case FIXED:
        return Schema.of(Schema.Type.BYTES);
      case ENUM:
        return Schema.enumWith(avroSchema.getEnumSymbols());
      case ARRAY:
        return Schema.arrayOf(fromAvroSchema(avroSchema.getElementType()));
      case MAP:
        return Schema.mapOf(Schema.of(Schema.Type.STRING), fromAvroSchema(avroSchema.getValueType()));
      case RECORD:
        return Schema.recordOf(avroSchema.getName(), Iterables.transform(avroSchema.getFields(), FIELD_CONVERTER));
      case UNION:
        return Schema.unionOf(Iterables.transform(avroSchema.getTypes(), SCHEMA_CONVERTER));
    }

    // This shouldn't happen.
    throw new IllegalArgumentException("Unsupported Avro schema type " + avroSchema.getType());
  }

  /**
   * Creates a {@link StructuredRecord} from a {@link GenericRecord}.
   *
   * @param record the {@link GenericRecord}
   * @param schema the {@link Schema} of the {@link StructuredRecord} to create
   * @return a new {@link StructuredRecord}
   */
  public static StructuredRecord fromAvroRecord(GenericRecord record, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    org.apache.avro.Schema avroSchema = record.getSchema();
    for (Schema.Field field : schema.getFields()) {
      String name = field.getName();
      builder.set(name, fromAvroValue(field.getSchema(), avroSchema.getField(name).schema(), record.get(name)));
    }
    return builder.build();
  }

  @Nullable
  private static Object fromAvroValue(Schema schema, org.apache.avro.Schema avroSchema, Object value) {
    switch (schema.getType()) {
      case NULL:
        Preconditions.checkArgument(avroSchema.getType() == org.apache.avro.Schema.Type.NULL);
        return null;
      case BOOLEAN:
        Preconditions.checkArgument(avroSchema.getType() == org.apache.avro.Schema.Type.BOOLEAN);
        return value;
      case INT:
        Preconditions.checkArgument(avroSchema.getType() == org.apache.avro.Schema.Type.INT);
        return value;
      case LONG:
        Preconditions.checkArgument(avroSchema.getType() == org.apache.avro.Schema.Type.LONG);
        return value;
      case FLOAT:
        Preconditions.checkArgument(avroSchema.getType() == org.apache.avro.Schema.Type.FLOAT);
        return value;
      case DOUBLE:
        Preconditions.checkArgument(avroSchema.getType() == org.apache.avro.Schema.Type.DOUBLE);
        return value;
      case BYTES:
        Preconditions.checkArgument(avroSchema.getType() == org.apache.avro.Schema.Type.BYTES);
        return value;
      case STRING:
        Preconditions.checkArgument(avroSchema.getType() == org.apache.avro.Schema.Type.STRING);
        return value.toString();
      case ENUM:
        Preconditions.checkArgument(avroSchema.getType() == org.apache.avro.Schema.Type.ENUM);
        return value.toString();
      case ARRAY:
        Preconditions.checkArgument(avroSchema.getType() == org.apache.avro.Schema.Type.ARRAY);
        return convertAvroArray(schema.getComponentSchema(), avroSchema.getElementType(), (GenericArray) value);
      case MAP:
        Preconditions.checkArgument(avroSchema.getType() == org.apache.avro.Schema.Type.MAP);
        Preconditions.checkArgument(schema.getMapSchema().getKey().getType() == Schema.Type.STRING);
        return convertAvroMap(schema.getMapSchema().getValue(), avroSchema.getValueType(), (Map<String, Object>) value);
      case RECORD:
        Preconditions.checkArgument(avroSchema.getType() == org.apache.avro.Schema.Type.RECORD);
        return fromAvroRecord((GenericRecord) value, schema);
      case UNION:
        Preconditions.checkArgument(avroSchema.getType() == org.apache.avro.Schema.Type.UNION);
        return convertUnion(schema, avroSchema, value);
    }
    throw new IllegalArgumentException("Unsupported schema type " + schema.getType());
  }

  private static Collection<?> convertAvroArray(Schema elementSchema,
                                                org.apache.avro.Schema avroElementSchema, GenericArray<?> array) {
    List<Object> result = new ArrayList<>(array.size());
    for (Object obj : array) {
      result.add(fromAvroValue(elementSchema, avroElementSchema, obj));
    }
    return result;
  }

  private static Map<String, ?> convertAvroMap(Schema valueSchema,
                                               org.apache.avro.Schema avroValueSchema, Map<String, Object> map) {
    Map<String, Object> result = new HashMap<>(map.size());
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      map.put(entry.getKey(), fromAvroValue(valueSchema, avroValueSchema, entry.getValue()));
    }
    return result;
  }

  private static Object convertUnion(Schema unionSchema,
                                     org.apache.avro.Schema avroUnionSchema, Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof GenericRecord) {
      return fromAvroValue(findByType(unionSchema, Schema.Type.RECORD),
                           findByType(avroUnionSchema, org.apache.avro.Schema.Type.RECORD), value);
    }
    if (value instanceof GenericEnumSymbol) {
      return fromAvroValue(findByType(unionSchema, Schema.Type.RECORD),
                           findByType(avroUnionSchema, org.apache.avro.Schema.Type.RECORD), value);
    }
    if (value instanceof GenericArray) {
      return fromAvroValue(findByType(unionSchema, Schema.Type.ARRAY),
                           findByType(avroUnionSchema, org.apache.avro.Schema.Type.ARRAY), value);
    }
    if (value instanceof Map) {
      return fromAvroValue(findByType(unionSchema, Schema.Type.MAP),
                           findByType(avroUnionSchema, org.apache.avro.Schema.Type.MAP), value);
    }
    if (value instanceof GenericFixed) {
      return fromAvroValue(findByType(unionSchema, Schema.Type.BYTES),
                           findByType(avroUnionSchema, org.apache.avro.Schema.Type.FIXED), value);
    }
    if (value instanceof CharSequence) {
      return fromAvroValue(findByType(unionSchema, Schema.Type.STRING),
                           findByType(avroUnionSchema, org.apache.avro.Schema.Type.STRING), value);
    }
    if (value instanceof ByteBuffer) {
      return fromAvroValue(findByType(unionSchema, Schema.Type.BYTES),
                           findByType(avroUnionSchema, org.apache.avro.Schema.Type.BYTES), value);
    }
    if (value instanceof Integer) {
      return fromAvroValue(findByType(unionSchema, Schema.Type.INT),
                           findByType(avroUnionSchema, org.apache.avro.Schema.Type.INT), value);
    }
    if (value instanceof Long) {
      return fromAvroValue(findByType(unionSchema, Schema.Type.LONG),
                           findByType(avroUnionSchema, org.apache.avro.Schema.Type.LONG), value);
    }
    if (value instanceof Float) {
      return fromAvroValue(findByType(unionSchema, Schema.Type.FLOAT),
                           findByType(avroUnionSchema, org.apache.avro.Schema.Type.FLOAT), value);
    }
    if (value instanceof Double) {
      return fromAvroValue(findByType(unionSchema, Schema.Type.DOUBLE),
                           findByType(avroUnionSchema, org.apache.avro.Schema.Type.DOUBLE), value);
    }
    if (value instanceof Boolean) {
      return fromAvroValue(findByType(unionSchema, Schema.Type.BOOLEAN),
                           findByType(avroUnionSchema, org.apache.avro.Schema.Type.BOOLEAN), value);
    }

    throw new IllegalArgumentException("Unsupported data type " + value.getClass());
  }

  private static Schema findByType(Schema unionSchema, Schema.Type type) {
    for (Schema schema : unionSchema.getUnionSchemas()) {
      if (schema.getType() == type) {
        return schema;
      }
    }
    throw new IllegalArgumentException("Failed to find schema of type " + type + " in union schema");
  }

  private static org.apache.avro.Schema findByType(org.apache.avro.Schema unionSchema,
                                                   org.apache.avro.Schema.Type type) {
    for (org.apache.avro.Schema schema : unionSchema.getTypes()) {
      if (schema.getType() == type) {
        return schema;
      }
    }
    throw new IllegalArgumentException("Failed to find avro schema of type " + type + " in union schema");
  }

  private AvroConverter() {
  }
}
