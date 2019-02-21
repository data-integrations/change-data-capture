# CDC Google Cloud Bigtable Sink

Description
-----------
This plugin takes input from a CDC source and writes the changes to Cloud Bigtable.

All CDC sink plugins are normally used in conjunction with CDC source plugins. 
CDC sink expects messages in CDC format as an input.  

Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be
provided and can be set to 'auto-detect'.
Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.
The service account key can be found on the Dashboard in the Cloud Platform Console.
Make sure the account key has permission to access BigQuery and Google Cloud Storage.
The service account key file needs to be available on every node in your cluster and
must be readable by all users running the job.

Properties
----------
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Instance ID**: The Instance Id the Cloud Bigtable is in.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console. This is the project
that the BigQuery job will run in. If a temporary bucket needs to be created, the service account
must have permission in this project to create buckets.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

Usage Notes
-----------
This plugin supports table creation and table modification on a Cloud Bigtable project. 
We recommend placing a normalizer transformation plugin before this plugin. 
It converts inputs into standard Data Definition Language (DDL) and Data Manipulation Language (DML) records that 
can be parsed by this plugin.

Table Creation
--------------
When the plugin receives a DDL record, it creates a table in the target Cloud Bigtable project. The name of the table 
is specified in the DDL record. Below is a sample DDL Record that creates a table with name `TESTANOTHER`.
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
    "table": "TESTANOTHER"
  }
}
```

Table Modification
--------------
When the plugin receives a DML record, it modifies the corresponding table according to the operation specified in 
`op_type`. 

| op\_type | Operation |
| :--------------: | :--------------: |
| I | Insert |
| U | Update | 
| D | Delete |

The content of the changes is listed in the `change` field. The `primary_keys` field specifies the fields in `change` 
that will be used to name a row in the table. Below is a sample DML record that creates a row for `Scott` and inserts 
his information into the row.
```
{
  "table": "EMPLOYEE",
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