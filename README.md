[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Join CDAP community](https://cdap-users.herokuapp.com/badge.svg?t=wrangler)](https://cdap-users.herokuapp.com?t=1)

Change Data Capture
===================

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
  
**NOTE:** Bigtable Sink tests will be skipped without provided properties.

## Setup Local Environment
To start local environment you should:
* [Install Docker Compose](https://docs.docker.com/compose/install/)
* Run commands:
  ```bash
  cd docker-compose/cdc-env/
  docker-compose up -d
  ```
 
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




