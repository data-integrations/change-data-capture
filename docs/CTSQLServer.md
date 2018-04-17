Change Tracking SQL Server Streaming Source
===========================================

CDAP Realtime Plugin for EDW offloading from SQL Server through Change Tracking.

Plugin Configuration
---------------------
| Config | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Reference Name** | **Y** | N/A | A unique name which will be used to identify this source for lineage and annotating metadata.|
| **Hostname** | **Y** | N/A | Hostname of the SQL Server from which the data needs to be offloaded. Ex: mysqlserver.net or 12.123.12.123  |
| **Port** | **Y** | 1433 | SQL Server Port |
| **Username** | N | N/A | Username to use to connect to the specified database. Required for databases that need authentication. Optional for databases that do not require authentication. |
| **Password** | N | N/A | Password to use to connect to the specified database. Required for databases that need authentication. Optional for databases that do not require authentication. |
| **Database name** | **Y** | N/A | SQL Server database name which needs to be tracked. Note: Change Tracking must be enabled on the database for the source to read the chage data.|

SQL Server Change Tracking
--------------------------
Change Tracking allows to identify the rows which have changed. Change Tracking SQL Server Streaming Source leverage 
this to retrieve just the minimum information to keep a SQL server database in sync with a downstream sink. You can 
read more about SQL Server Change Tracking 
[here](https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-tracking-sql-server)

### Change Tracking and Change Data Capture
SQL Server also allow capturing the changed data through Change Data Capture. Change Data Capture provides historical 
information about the changes. This plugin uses Change Tracking over Change Data Capture because of the following 
reasons:

1. Historical Information: From a pipeline whose purose is to offload data from a database and/or to keep a database 
in sync with some external storage, historical information is not critical.
2. Schema Changes: Change Data Capture has very limited suport for schema changes in the table being tracked. 
New columns added to a tracked table are not automatically tracked. For more details please refer 
[Handling Changes to Source Data](https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server#handling-changes-to-source-tables)
3. Supported Editions: Change Data Capture is only avaliable in DataCenter and Enterprise editions whereas 
Change Tracking is supported in Express, Workgroup, Web Standard, Enterprise and DataCenter.
You can read more about differences between SQL Server CT and CDC 
[here](https://technet.microsoft.com/en-us/library/cc280519(v=sql.105).aspx)

### Enable Change Tracking for a Database
Before you start using the Change Tracking SQL Server Source to track changes in your database you will need to 
enable Change Tracking on the database. Change Tracking can be enabled on database by:

> ALTER DATABASE dbName SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)

Refer [Enable Change Tracking for a Database](https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-tracking-sql-server#enable-change-tracking-for-a-database) for more details.

### Enable Change Tracking for a Table
Change Tracking SQL Server Streaming Source will sync all the tables in a database which has change tracking enabled. 
Change Tracking can ge enabled for a table by:

> ALTER TABLE tableName ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)

Refer [Enable Change Tracking for a Table](https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-tracking-sql-server#enable-change-tracking-for-a-table) for more details.

