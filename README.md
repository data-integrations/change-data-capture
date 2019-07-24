[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Join CDAP community](https://cdap-users.herokuapp.com/badge.svg?t=wrangler)](https://cdap-users.herokuapp.com?t=1)

Change Data Capture (Alpha)
===========================

In databases, Change Data Capture(CDC) is used to determine and track the data that has changed so that
action can be taken using the changed data. This repository contains CDAP plugins which allows to capture
the changes from databases such as Oracle and Microsoft SQL Server and to push those changes in realtime
to sinks such as Kudu, HBase, and Database.

* [Oracle](docs/oracle/Oracle.md)
* [Microsoft SQL Server](docs/CTSQLServer.md)

# Overview

Following plugins are available in this repository. 

  * [Google Cloud BigTable Sink](docs/CDCBigTable-sparksink.md)
  * [HBase Sink](docs/CDCHBase-sparksink.md)
  * Kudu Sink 
  * [Golden Gate Kafka Source](docs/oracle/Oracle.md)
  * [SQL Server Change Tracking Streaming Source](docs/CTSQLServer.md)
  
# Development

## Run Integration Tests
It is possible to run integration tests against **local** (see [Setup Local Environment](#setup-local-environment)) 
or **remote environment**.

Run tests against **local** environment:
```bash
mvn clean test
```

Run tests against **remote** environment:
```bash
mvn clean test -DinstanceUri=<HostAndPort>
```

To use **remote environment** you may configure the following system properties:
* **test.sql-server.host** - SQL Server host. Default: localhost.
* **test.sql-server.port** - SQL Server port. Default: 1433.
* **test.sql-server.username** - SQL Server username. This user should have permissions to create databases.
 Default: SA.
* **test.sql-server.password** - SQL Server password. Default: 123Qwe123.
* **test.sql-server.namespace** - SQL Server namespace for test databases. Default: dbo.
* **test.bigtable.project** - Google Cloud Project ID. Default: lookup from local environment.
* **test.bigtable.instance** - Bigtable Instance ID. Default: null.
* **test.bigtable.serviceFilePath** - Path on the local file system of the service account key used for
  authorization. Default: lookup from local environment.
* **test.oracle-db.host** - Oracle DB host. Default: localhost.
* **test.oracle-db.port** - Oracle DB port. Default: 1521.
* **test.oracle-db.service** - Oracle DB service name. Default: XE.
* **test.oracle-db.username** - Oracle DB username. Default: trans_user.
* **test.oracle-db.password** - Oracle DB password. Default: trans_user.
* **test.oracle-db.driver.jar** - Path to Oracle Java Driver jar file. Default: null.
* **test.oracle-db.driver.class** - Oracle Java Driver class name. Default: oracle.jdbc.OracleDriver.
* **test.goldengate.broker** - Kafka broker specified in host:port form. Default: localhost:9092.
* **test.goldengate.topic** - Name of the topic to which Golden Gate publishes the DDL and DML changes. 
Default: oggtopic.
  
**NOTE:** Bigtable Sink tests will be skipped without provided properties.
**NOTE:** Golden Gate Kafka Source tests will be skipped without provided properties.

## Run Performance Tests
It is possible to run performance tests against **local** (see [Setup Local Environment](#setup-local-environment)) 
or **remote environment**.

Run tests against **local** environment:
```bash
mvn clean test -P perf-tests
```

Run tests against **remote** environment:
```bash
mvn clean test -P perf-tests -DinstanceUri=<HostAndPort>
```

Common system properties for tests:
* **ptest.test-data.load** - Prepare and load test data to source storage. Default: true.
* **ptest.test-data.inserts** - Number of records to prepare. Default: 5000.
* **ptest.target-table-created-timeout.seconds** - Timeout for table creation in sink storage. Default: 300.
* **ptest.data-transferred-timeout.seconds** - Timeout for data transfer to target storage. Default: 600.

To use **remote environment** you may configure the following system properties:
* **ptest.sql-server.host** - SQL Server host. Default: localhost.
* **ptest.sql-server.port** - SQL Server port. Default: 1433.
* **ptest.sql-server.username** - SQL Server username. This user should have permissions to create databases.
 Default: SA.
* **ptest.sql-server.password** - SQL Server password. Default: 123Qwe123.
* **ptest.bigtable.project** - Google Cloud Project ID. Default: lookup from local environment.
* **ptest.bigtable.instance** - Bigtable Instance ID. Default: null.
* **ptest.bigtable.serviceFilePath** - Path on the local file system of the service account key used for
  authorization. Default: lookup from local environment.
  
**NOTE:** Bigtable Sink tests will be skipped without provided properties.

## Setup Local Environment
To start local environment you should:
* [Install Docker Compose](https://docs.docker.com/compose/install/)
* Build local docker images
  * [Build Oracle DB docker image](https://github.com/oracle/docker-images/tree/master/OracleDatabase/SingleInstance)
  * [Build Oracle GoldenGate docker image](https://github.com/oracle/docker-images/tree/master/OracleGoldenGate)
* Start environment by running commands:
  ```bash
  cd docker-compose/cdc-env/
  docker-compose up -d
  ```
* Configure GoldenGate for Oracle:
  * Start ggsci:
    ```bash
    docker-compose exec --user oracle goldengate_oracle ggsci
    ```
  * Configure user credentials:
    ```bash
    ADD credentialstore  
    alter credentialstore add user gg_extract@oracledb:1521/xe password gg_extract alias oggadmin 
    ```
  * Change source schema configuration:
    ```bash
    DBLOGIN USERIDALIAS oggadmin
    add schematrandata trans_user ALLCOLS
    ```
  * Define the Extract and start it 
  (all EXTRACT params are defined in docker-compose/cdc-env/GoldenGate/dirprm/ext1.prm):
    ```bash
    ADD EXTRACT ext1, TRANLOG, BEGIN NOW
    ADD EXTTRAIL /u01/app/ogg/dirdat/in, EXTRACT ext1
    START ext1
    ```
  * Check its status:
    ```bash
    INFO ext1
    ```
* Configure GoldenGate for BigData:
  * Start ggsci:
    ```bash
    docker-compose exec --user oracle goldengate_bigdata ggsci
    ```
  * Define the Replicat and start it
  (all REPLICAT params are defined in docker-compose/cdc-env/GoldenGate-Bigdata/dirprm/rconf.prm):
    ```bash
    ADD REPLICAT rconf, EXTTRAIL /u01/app/ogg/dirdat/in
    START rconf
    ```
  * Check its status:
    ```bash
    INFO RCONF
    ```
NOTE: More info about *.prm files - https://docs.oracle.com/goldengate/1212/gg-winux/GWURF/gg_parameters.htm#GWURF394
 
# Contact

## Mailing Lists

CDAP User Group and Development Discussions:

* [cdap-user@googlegroups.com](https://groups.google.com/d/forum/cdap-user)

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from
users, release announcements, and any other discussions that we think will be helpful
to the users.

# License and Trademarks

Copyright © 2016-2019 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.




