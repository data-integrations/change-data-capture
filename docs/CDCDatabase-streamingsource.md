# CDC Golden Gate Kafka Streaming Source

Description
-----------
This plugin reads Change Data Capture (CDC) events from a Golden Gate Kafka topic.

All CDC source plugins are normally used in conjunction with CDC sink plugins. 
CDC source produces messages in CDC format.  

Useful links:
* [Goldengate site](https://www.oracle.com/middleware/technologies/goldengate.html)
* [Installing Oracle GoldenGate](https://docs.oracle.com/goldengate/1212/gg-winux/GIORA/install.htm#GIORA162).
* [Using Oracle GoldenGate for Oracle Database](https://www.oracle.com/pls/topic/lookup?ctx=en/middleware/goldengate/core/18.1&id=GGODB-GUID-110CD372-2F7E-4262-B8D2-DC0A80422806).
* [Using Oracle GoldenGate for BigData](https://docs.oracle.com/goldengate/bd123210/gg-bd/GADBD/introduction-oracle-goldengate-big-data.htm#GADBD114).

Properties
----------
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Kafka Broker**: Kafka broker specified in host:port form. For example, example.com:9092.

**Kafka Topic**: Name of the topic to which Golden Gate publishes the DDL and DML changes.

**Default Initial Offset**: The default initial offset to read from. 
An offset of -2 means the smallest offset (the beginning of the topic). 
An offset of -1 means the latest offset (the end of the topic). 
Defaults to -1. Offsets are inclusive. 
If an offset of 5 is used, the message at offset 5 will be read.

**Max Rate Per Partition**: Max number of records to read per second per partition. 0 means there is no limit.
 Defaults to 1000.

Required GoldenGate Settings
----------
* GoldenGate should push data using Kafka handler
* Generic Wrapper Functionality should be enabled ("gg.handler.kafkahandler.format.wrapMessageInGenericAvroMessage"). 
* Schema topic ("gg.handler.kafkahandler.schemaTopicName") should be equal to DML changes topic. 
* Handler should send events in "OP" mode ("gg.handler.kafkahandler.mode"). 
* Handler should send events in "avro_op" format ("gg.handler.kafkahandler.format"). 