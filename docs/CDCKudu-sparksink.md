# Change Tracking SQL Server Streaming Source

Description
-----------
CDAP Realtime Plugin for Change Data Capture (CDC) in Kudu.

All CDC sink plugins are normally used in conjunction with CDC source plugins. 
CDC sink expects messages in CDC format as an input.  

Properties
----------
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Master Addresses**: Comma separated list of hostname:port of Apache Kudu Masters.

**Replicas**: Specifies the number of buckets to split the table into.

**Seed**: Seed to randomize the mapping of rows to hash buckets.

**Compression Algorithm**: Compression algorithm to be applied on the columns.

**Encoding Type**: Specifies the encoding to be applied on the schema.

**User Operations Timeout**: Timeout for Kudu operations in milliseconds.

**Administration Operations Timeout**: Administration operation timeout.

**Number of copies**: Specifies the number of replicas for the Kudu tables.

**Rows Buffer**: Number of rows that are buffered before flushing to the tablet server.

**Boss Threads**: Specifies the number of boss threads to be used by the client.