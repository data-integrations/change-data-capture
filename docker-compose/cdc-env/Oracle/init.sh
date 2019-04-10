set -ex

sqlplus / as sysdba << EOF
  alter system set enable_goldengate_replication=TRUE;
  alter database add supplemental log data;
  alter database force logging;
  alter system switch logfile;

  shutdown immediate;
  Startup mount;
  Alter database archivelog;
  alter database open;

  CREATE USER gg_extract IDENTIFIED BY gg_extract;
  GRANT CREATE SESSION, CONNECT, RESOURCE, ALTER ANY TABLE, ALTER SYSTEM, DBA, SELECT ANY TRANSACTION TO gg_extract;
  CREATE USER trans_user IDENTIFIED BY trans_user;
  GRANT CREATE SESSION, CONNECT, RESOURCE TO trans_user;
  ALTER USER trans_user QUOTA UNLIMITED ON USERS;

  exit;
EOF