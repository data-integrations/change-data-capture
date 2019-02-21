# CDC SQL Server Streaming Source

Description
-----------
This plugin reads Change Data Capture (CDC) events from a Golden Gate Kafka topic.

All CDC source plugins are normally used in conjunction with CDC sink plugins. 
CDC source produces messages in CDC format.  

Useful links:
* [Goldengate site](https://www.oracle.com/middleware/technologies/goldengate.html)
* [Setup tutorial](https://docs.oracle.com/en/middleware/goldengate/core/18.1/oracle-db/replicating-data-oracle-autonomous-data-warehouse-cloud.html)

Properties
----------
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Hostname**: Hostname of the SQL Server from which the data needs to be offloaded. 
Ex: mysqlserver.net or 12.123.12.123.

**Port**: SQL Server Port.

**Username**:  Username to use to connect to the specified database. Required for databases that need authentication. 
Optional for databases that do not require authentication.

**Password**:  Password to use to connect to the specified database. Required for databases that need authentication.
Optional for databases that do not require authentication.

**Database name**:  SQL Server database name which needs to be tracked. 
Note: Change Tracking must be enabled on the database for the source to read the chage data.