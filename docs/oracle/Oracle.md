Change Data Capture(CDC) for Oracle
===================================

Oracle GoldenGate is a realtime Change Data Capture system which allows streaming
changes from the Oracle database source to a target system. We configure the Oracle GoldenGate
to stream all changes made to a table, or a set of tables to a Kafka. These changes are then
read by CDAP realtime data pipeline making them available in the Hadoop ecosystem, such as Kudu,
HBase etc. Once the data is in Hadoop, computations can be performed on the data with low cost.

This document explains the steps required to setup the GoldenGate processes with the Oracle database and
configurations of the CDAP plugins which are used to process the changes from GoldenGate.

* [Setting up GoldenGate](GoldenGateSetup.md)
* [Microsoft SQL Server](docs/sqlserver/SQLServer.md)

It uses log-based technology to stream all changes to a database from source, to target – which may be another database of the same type, or a different one. It is commonly used for data integration, as well as replication of data for availability purposes.

In the context of Kafka, Oracle GoldenGate provides a way of streaming all changes made to a table,
or set of tables, and making them available to other processes in our data pipeline. These processes could include microservices relying on an up-to-date feed of data from a particular table, as well as persisting a replica copy of the data from the source system into a common datastore for analysis alongside data from other systems.