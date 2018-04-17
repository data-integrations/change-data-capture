Setup the GoldenGate for Oracle Change Data Capture
===================================================

Following are the instructions to setup GoldenGate with the Oracle. Please note that the instructions might
need some tweaks depending on the environment.


Prepare database for the Change Data Capture
--------------------------------------------

Login to database as sysdba:

  sqlplus / as sysdba

Execute the following commands to enable supplemental logging on the database:

  ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
	ALTER DATABASE FORCE LOGGING;
	SHUTDOWN IMMEDIATE
	STARTUP MOUNT
	ALTER DATABASE ARCHIVELOG;
	ALTER DATABASE OPEN;
	ALTER SYSTEM SWITCH LOGFILE;
	ALTER SYSTEM SET ENABLE_GOLDENGATE_REPLICATION=TRUE SCOPE=BOTH;
	EXIT

Create Golden Gate test user(ggtest):

  create user ggtest identified by ggtest;
  grant resource, dba, connect to ggtest;

Exit sqlplus and start the Oracle listener controller if it is not running already:

  lsnrctl start
  lsnrctl status


Setting up GoldenGate processes
-------------------------------

We need two GoldenGate processes running. Standard Oracle GoldenGate process
which acts as EXTRACT. This process integrates with the Database server
(such as Oracle), read the changed data and writes to the trail file. Another
is Oracle GoldenGate for Big Data which is responsible for publishing the changes
to the big data systems such as Kafka.


# Downloading the GoldenGate software

**[Download](http://www.oracle.com/technetwork/middleware/goldengate/downloads/index.html)** the
standard Oracle GoldenGate and follow the instructions in section 2.5.2 as given
**[here](https://docs.oracle.com/goldengate/1212/gg-winux/GIORA/install.htm#GIORA162)**.

From the instructions, you’ll need to create a response file (oggcore.rsp).
We used following properties in the response file while testing:

  INSTALL_OPTION=ORA12c
  SOFTWARE_LOCATION=/u01/ogg
  START_MANAGER=true
  MANAGER_PORT=9999
  DATABASE_LOCATION=/u01/app/oracle/product/12.1.0/xe
  UNIX_GROUP_NAME=dba

Execute the `runInstaller` binary passing the name of the response file created above as argument:

  cd fbo_ggs_Linux_x64_shiphome/Disk1
  ./runInstaller -silent -nowait -responseFile /u01/oggcore.rsp

**[Download](http://www.oracle.com/technetwork/middleware/goldengate/downloads/index.html)** the
Oracle GoldenGate for Big Data 12.3.0.1.0 and unzip the downloaded file at location say `/u01/ogg-bd`.


Configuring the GoldenGate Manager
----------------------------------
Manager processes in the GoldenGate are responsible for managing the `EXTRACT` and `REPLICAT` processes.

Login to the GoldenGate command interface(ggsci):

  cd /u01/ogg
  ./ggsci

If you receive following error:

  ./ggsci: error while loading shared libraries: libnnz12.so: cannot open shared object file: No such file or directory

This means that the GoldenGate is not able to find the Oracle installation, source the oracle environment:

  . oraenv

Please enter the location of the Oracle database when prompted. The location can be typically
found in the file `/etc/oratab`.

At the GGSCI prompt create required subdirs which will store the data and configurations for the GoldenGate processes:

  create subdirs

Enter `EDIT PARAM MGR` and in the resulting vi edit session add:

  DynamicPortList 20000-20099
  PurgeOldExtracts ./dirdat/*, UseCheckPoints, MinKeepHours 2
  Autostart Extract E*
  AUTORESTART Extract *, WaitMinutes 1, Retries 3

Save and exit vi and start the mgr process:

  start mgr

Make sure manager is running:

  GGSCI (bigdatalite.localdomain) 1> info mgr
  Manager is running (IP port bigdatalite.localdomain.7811, Process ID 23018).

Configure the manager for the Oracle GoldenGate for big data:




