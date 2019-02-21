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




