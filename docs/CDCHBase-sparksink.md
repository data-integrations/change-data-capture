CDC HBase Sink
==========

CDAP Plugin for Change Data Capture (CDC) in HBase using the Spark Framework. Plugin can be configured only for real-time pipelines.

Usage Notes
-----------

This plugin supports table creation and table modification on an HBase server. We recommend placing a normalizer transformation plugin before this plugin. It converts inputs into standard Data Definition Language (DDL) and Data Manipulation Language (DML) records that can be parsed by this plugin.

Table Creation
--------------

When the plugin receives a DDL record, it creates a table in the target HBase database. The name of the table is specified in the DDL record. Below is a sample DDL Record that creates a table in namespace `GGTEST` with name `TESTANOTHER`.
```{
  "schema": {
    "type": "RECORD",
    "recordName": "DDLRecord",
    "fieldMap": {
      "table": {
        "name": "table",
        "schema": {
          "type": "STRING",
          "unionSchemas": []
        }
      },
      "schema": {
        "name": "schema",
        "schema": {
          "type": "STRING",
          "unionSchemas": []
        }
      }
    },
    "fields": [
      {
        "name": "table",
        "schema": {
          "type": "STRING",
          "unionSchemas": []
        }
      },
      {
        "name": "schema",
        "schema": {
          "type": "STRING",
          "unionSchemas": []
        }
      }
    ],
    "unionSchemas": []
  },
  "fields": {
    "schema": "{\"type\":\"record\",\"name\":\"columns\",\"fields\":[{\"name\":\"CID\",\"type\":[\"null\",\"long\"]},{\"name\":\"CNAME\",\"type\":[\"null\",\"string\"]}]}",
    "table": "GGTEST.TESTANOTHER"
  }
}
```

Table Modification
--------------
When the plugin receives a DML record, it modifies the corresponding table according to the operation specified in `op_type`. 

| op\_type | Operation |
| :--------------: | :--------------: |
| I | Insert |
| U | Update | 
| D | Delete |

The content of the changes is listed in the `change` field. The `primary_keys` field specifies the fields in `change` that will be used to name a row in the table. Below is a sample DML record that creates a row for `Scott` and inserts his information into the row.
```
{
  "table": "GGTEST_EMPLOYEE",
  "schema": "{\"type\":\"record\",\"name\":\"columns\",\"fields\":[{\"name\":\"EMPNO\",\"type\":[\"null\",\"long\"]},{\"name\":\"ENAME\",\"type\":[\"null\",\"string\"]},{\"name\":\"JOB\",\"type\":[\"null\",\"string\"]},{\"name\":\"MGR\",\"type\":[\"null\",\"long\"]},{\"name\":\"HIREDATE\",\"type\":[\"null\",\"string\"]},{\"name\":\"SAL\",\"type\":[\"null\",\"long\"]},{\"name\":\"COMM\",\"type\":[\"null\",\"long\"]},{\"name\":\"DEPTNO\",\"type\":[\"null\",\"long\"]},{\"name\":\"EMP_ADDRESS\",\"type\":[\"null\",\"string\"]}]}",
  "op_type": "I",
  "primary_keys": [
    "ENAME"
  ],
  "change": {
    "HIREDATE": "03-DEC-2015",
    "JOB": "Software Engineer",
    "MGR": 991,
    "SAL": 1234,
    "DEPTNO": 1,
    "EMP_ADDRESS": "San Jose",
    "ENAME": "Scott",
    "EMPNO": 1,
    "COMM": 1
  }
}
```
Plugin Configuration
---------------------

The following configurations collectively specify the address of the target HBase server.

| Config | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Reference Name** | **Yes** | N/A | Name of the plugin instance.| 
| **zookeeperClientPort** | **No** | `2181` | Specifies the port which the HBase server runs on. |
| **zookeeperParent** | **No** | `/hbase` | Specifies the HBase parent node. |
| **zookeeperQuorum** | **No** | `localhost` | The web/IP address of the HBase server. |

Note that your HBase server does not necessarily run on the address specified by the default configurations. Check before you use this plugin.
 
Build
-----
To build this plugin:

```
   mvn clean package -DskipTests
```    

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

Deployment
----------
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/plugin.jar> config-file <target/plugin.json>

For example, if your artifact is named 'CDCHBase-sink-1.0.0':

    > load artifact target/CDCHBase-sink-1.0.0.jar config-file target/CDCHBase-sink-1.0.0.json
    
## Mailing Lists

CDAP User Group and Development Discussions:

* `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.

## IRC Channel

CDAP IRC Channel: #cdap on irc.freenode.net


## License and Trademarks

Copyright © 2016-2017 Cask Data, Inc.

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
