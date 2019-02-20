# CDC HBase Sink

Description
-----------
CDAP Realtime Plugin for Change Data Capture (CDC) in HBase.

This plugin will use connection parameter from system Hadoop cluster.

All CDC sink plugins are normally used in conjunction with CDC source plugins. 
CDC sink expects messages in CDC format as an input.  

Properties
----------
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

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