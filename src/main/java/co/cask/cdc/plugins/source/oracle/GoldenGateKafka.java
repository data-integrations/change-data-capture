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

package co.cask.cdc.plugins.source.oracle;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdc.plugins.common.Schemes;
import co.cask.hydrator.common.Constants;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import org.apache.avro.SchemaNormalization;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Streaming source for reading from Golden Gate Kafka topic.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("CDCDatabase")
@Description("Streaming source for reading through Golden Gate Kafka topic")
public class GoldenGateKafka extends StreamingSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(GoldenGateKafka.class);
  private static final Schema GENERIC_WRAPPER_SCHEMA_MESSAGE
    = Schema.recordOf("GenericWrapperSchema", Schema.Field.of("message", Schema.of(Schema.Type.BYTES)));
  private static final Schema DDL_SCHEMA_MESSAGE
    = Schema.recordOf("DDLRecord", Schema.Field.of("message", Schema.of(Schema.Type.BYTES)));
  private static final Schema TRANSFORMED_MESSAGE
    = Schema.recordOf("Message", Schema.Field.of("message", Schema.of(Schema.Type.BYTES)));

  private static final Schema STATE_SCHEMA
    = Schema.recordOf("state",
                      Schema.Field.of("data",
                                      Schema.mapOf(Schema.of(Schema.Type.LONG),
                                                   Schema.of(Schema.Type.STRING))));

  private static final Schema DML_SCHEMA = Schema.recordOf("DMLRecord",
                                                           Schema.Field.of("message", Schema.of(Schema.Type.BYTES)),
                                                           Schema.Field.of("staterecord", STATE_SCHEMA));

  private static final Normalizer NORMALIZER = new Normalizer();

  private final GoldenGateKafkaConfig conf;


  public GoldenGateKafka(GoldenGateKafkaConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    conf.validate();
    pipelineConfigurer.createDataset(conf.referenceName, Constants.EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(Schemes.CHANGE_SCHEMA);

    // Make sure that Golden Gate kafka topic only have single partition
    SimpleConsumer consumer = new SimpleConsumer(conf.getHost(), conf.getPort(), 20 * 1000, 128 * 1024,
                                                 "partitionLookup");
    try {
      getPartitionId(consumer);
    } finally {
      consumer.close();
    }

    if (conf.getMaxRatePerPartition() > 0) {
      Map<String, String> pipelineProperties = new HashMap<>();
      pipelineProperties.put("spark.streaming.kafka.maxRatePerPartition", conf.getMaxRatePerPartition().toString());
      pipelineConfigurer.setPipelineProperties(pipelineProperties);
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    context.registerLineage(conf.referenceName);

    SimpleConsumer consumer = new SimpleConsumer(conf.getHost(), conf.getPort(), 20 * 1000, 128 * 1024,
                                                 "partitionLookup");
    Map<TopicAndPartition, Long> offsets;
    try {
      offsets = loadOffsets(consumer);
    } finally {
      consumer.close();
    }

    LOG.info("Using initial offsets {}", offsets);
    Map<String, String> kafkaParams = new HashMap<>();
    kafkaParams.put("metadata.broker.list", conf.getBroker());
    JavaInputDStream<StructuredRecord> directStream = KafkaUtils.createDirectStream(
      context.getSparkStreamingContext(), byte[].class, byte[].class, DefaultDecoder.class, DefaultDecoder.class,
      StructuredRecord.class, kafkaParams, offsets, in -> kafkaMessageToRecord(in));
    return directStream
      .mapToPair(record -> new Tuple2<>("", record))
      .mapWithState(StateSpec.function(schemaStateFunction()))
      .flatMap(record -> NORMALIZER.transform(record).iterator())
      .map(changeRecord -> StructuredRecord.builder(Schemes.CHANGE_SCHEMA)
        .set(Schemes.CHANGE_FIELD, changeRecord)
        .build());
  }

  private Map<TopicAndPartition, Long> loadOffsets(SimpleConsumer consumer) {
    // KafkaUtils doesn't understand -1 and -2 as latest offset and smallest offset.
    // so we have to replace them with the actual smallest and latest
    String topicName = conf.getTopic();
    int partitionId = getPartitionId(consumer);
    long initialOffset = conf.getDefaultInitialOffset();

    TopicAndPartition topicAndPartition = new TopicAndPartition(topicName, partitionId);

    Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetsToRequest = new HashMap<>();
    if (initialOffset == OffsetRequest.EarliestTime() || initialOffset == OffsetRequest.LatestTime()) {
      offsetsToRequest.put(topicAndPartition, new PartitionOffsetRequestInfo(initialOffset, 1));
    }

    kafka.javaapi.OffsetRequest offsetRequest =
      new kafka.javaapi.OffsetRequest(offsetsToRequest, OffsetRequest.CurrentVersion(), "offsetLookup");
    OffsetResponse response = consumer.getOffsetsBefore(offsetRequest);

    if (response.errorCode(topicName, partitionId) != 0) {
      throw new IllegalStateException(String.format(
        "Could not find offset for topic '%s' and partition '%s'. Please check all brokers were included in the " +
          "broker list.", topicName, partitionId));
    }

    Map<TopicAndPartition, Long> offsets = new HashMap<>();
    offsets.put(topicAndPartition, response.offsets(topicName, partitionId)[0]);
    return offsets;
  }

  private StructuredRecord kafkaMessageToRecord(MessageAndMetadata<byte[], byte[]> messageAndMetadata) {
    return StructuredRecord.builder(TRANSFORMED_MESSAGE)
      .set("message", messageAndMetadata.message())
      .build();
  }

  private int getPartitionId(SimpleConsumer consumer) {
    Set<Integer> partitions = new HashSet<>();
    TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(conf.getTopic()));
    TopicMetadataResponse response = consumer.send(topicMetadataRequest);

    for (TopicMetadata topicMetadata : response.topicsMetadata()) {
      for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
        partitions.add(partitionMetadata.partitionId());
      }
    }

    if (partitions.size() > 1) {
      throw new IllegalArgumentException(
        String.format("Topic '%s' should only have one partition. Found '%s' partitions.",
                      conf.getTopic(), partitions.size()));
    }
    return partitions.iterator().next();
  }

  private static Function3<String, Optional<StructuredRecord>, State<Map<Long, String>>, StructuredRecord>
  schemaStateFunction() {
    return (key, value, state) -> {
      StructuredRecord input = value.get();
      Object message = input.get("message");

      byte[] messageBytes = BinaryMessages.getBytesFromBinaryMessage(message);
      String messageBody = new String(messageBytes, StandardCharsets.UTF_8);

      if (messageBody.contains("generic_wrapper") && messageBody.contains("oracle.goldengate")) {
        StructuredRecord.Builder builder = StructuredRecord.builder(GENERIC_WRAPPER_SCHEMA_MESSAGE);
        builder.set("message", message);
        return builder.build();
      }

      if (messageBody.contains("\"type\" : \"record\"")) {
        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(messageBody);
        long schemaFingerPrint = SchemaNormalization.parsingFingerprint64(avroSchema);
        Map<Long, String> newState;
        if (state.exists()) {
          newState = state.get();
        } else {
          newState = new HashMap<>();
        }
        newState.put(schemaFingerPrint, messageBody);
        state.update(newState);
        LOG.debug("Schema mapping updated to {}", state.get());

        StructuredRecord.Builder builder = StructuredRecord.builder(DDL_SCHEMA_MESSAGE);
        builder.set("message", message);
        return builder.build();
      }

      StructuredRecord.Builder stateBuilder = StructuredRecord.builder(STATE_SCHEMA);
      stateBuilder.set("data", state.get());

      StructuredRecord.Builder builder = StructuredRecord.builder(DML_SCHEMA);
      builder.set("message", message);
      builder.set("staterecord", stateBuilder.build());
      return builder.build();
    };
  }
}
