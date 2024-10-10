<h1 align="center">
  DBMS PERMISSIONS
</h1>
<p align="center">
  本文档用于描述 DBMS 分布式迁移服务平台所需数据源权限
</p>

-------

### ORACLE NONCDB
对于非增量 ORACLE 数据同步，DBMS 数据库分布式迁移服务平台所需的用户权限
```sql
CREATE USER dbmsadmin IDENTIFIED BY dbmsadmin123 DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP;
ALTER USER dbmsadmin QUOTA UNLIMITED ON USERS;

CREATE ROLE dbms_role_privs;

GRANT CONNECT,RESOURCE TO dbms_role_privs;
GRANT UNLIMITED TABLESPACE TO dbms_role_privs;
GRANT EXECUTE ON DBMS_PARALLEL_EXECUTE TO dbms_role_privs;
GRANT EXECUTE ON DBMS_CRYPTO TO dbms_role_privs;
GRANT SELECT ANY DICTIONARY TO dbms_role_privs;
GRANT SELECT_CATALOG_ROLE TO dbms_role_privs;
GRANT SELECT ANY TABLE TO dbms_role_privs;
GRANT EXECUTE_CATALOG_ROLE TO dbms_role_privs;
GRANT FLASHBACK ANY TABLE TO dbms_role_privs;

GRANT dbms_role_privs TO dbmsadmin;
```

对于 LOGMINER 增量 ORACLE 数据同步，DBMS 数据库分布式迁移服务平台所需的用户权限
```sql
CREATE USER dbmsadmin IDENTIFIED BY dbmsadmin123 DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP;
ALTER USER dbmsadmin QUOTA UNLIMITED ON USERS;

CREATE ROLE dbms_role_privs;

GRANT CONNECT,RESOURCE TO dbms_role_privs;
GRANT UNLIMITED TABLESPACE TO dbms_role_privs;
GRANT EXECUTE ON DBMS_PARALLEL_EXECUTE TO dbms_role_privs;
GRANT EXECUTE ON DBMS_CRYPTO TO dbms_role_privs;
GRANT SELECT ANY DICTIONARY TO dbms_role_privs;
GRANT SELECT_CATALOG_ROLE TO dbms_role_privs;
GRANT SELECT ANY TABLE TO dbms_role_privs;
GRANT EXECUTE_CATALOG_ROLE TO dbms_role_privs;
GRANT SELECT ON SYSTEM.LOGMNR_COL$ TO dbms_role_privs;
GRANT SELECT ON SYSTEM.LOGMNR_OBJ$ TO dbms_role_privs;
GRANT SELECT ON SYSTEM.LOGMNR_USER$ TO dbms_role_privs;
GRANT SELECT ON SYSTEM.LOGMNR_UID$ TO dbms_role_privs;
GRANT EXECUTE ON DBMS_LOGMNR TO dbms_role_privs;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO dbms_role_privs;
GRANT FLASHBACK ANY TABLE TO dbms_role_privs;

-- Only needs to be added when Oracle is version 12c, otherwise delete this line
GRANT LOGMINING TO dbms_role_privs;

GRANT dbms_role_privs TO dbmsadmin;
```
-------

### ORACLE CDB
对于非增量 ORACLE 数据同步，DBMS 数据库分布式迁移服务平台所需的用户权限
```sql
ALTER SESSION SET CONTAINER =CDB$ROOT;

CREATE USER c##dbmsadmin IDENTIFIED BY c##dbmsadmin123 CONTAINER = ALL;

ALTER USER c##dbmsadmin QUOTA UNLIMITED ON USERS CONTAINER = ALL;

-- Allow CDB users to access all PDBS
ALTER USER c##dbmsadmin SET CONTAINER_DATA=ALL CONTAINER =CURRENT;


CREATE ROLE c##dbms_role_privs CONTAINER =ALL;

GRANT CONNECT,RESOURCE TO c##dbms_role_privs CONTAINER =ALL;
GRANT UNLIMITED TABLESPACE TO c##dbms_role_privs CONTAINER =ALL;
GRANT EXECUTE ON DBMS_PARALLEL_EXECUTE TO c##dbms_role_privs CONTAINER =ALL;
GRANT EXECUTE ON DBMS_CRYPTO TO c##dbms_role_privs CONTAINER =ALL;
GRANT SELECT ANY DICTIONARY TO c##dbms_role_privs CONTAINER =ALL;
GRANT SELECT_CATALOG_ROLE TO c##dbms_role_privs CONTAINER =ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##dbms_role_privs CONTAINER =ALL;
GRANT SELECT ANY TABLE TO c##dbms_role_privs CONTAINER =ALL;
GRANT FLASHBACK ANY TABLE TO c##dbms_role_privs CONTAINER =ALL;

GRANT c##dbms_role_privs TO c##dbmsadmin CONTAINER =ALL;
```

对于 LOGMINER 增量 ORACLE 数据同步，DBMS 数据库分布式迁移服务平台所需的用户权限
```sql
ALTER SESSION SET CONTAINER =CDB$ROOT;

CREATE USER c##dbmsadmin IDENTIFIED BY c##dbmsadmin123 CONTAINER = ALL;

ALTER USER c##dbmsadmin QUOTA UNLIMITED ON USERS CONTAINER = ALL;

-- Allow CDB users to access all PDBS
ALTER USER c##dbmsadmin SET CONTAINER_DATA=ALL CONTAINER =CURRENT;


CREATE ROLE c##dbms_role_privs CONTAINER =ALL;

GRANT CONNECT,RESOURCE TO c##dbms_role_privs CONTAINER =ALL;
GRANT UNLIMITED TABLESPACE TO c##dbms_role_privs CONTAINER =ALL;
GRANT EXECUTE ON DBMS_PARALLEL_EXECUTE TO c##dbms_role_privs CONTAINER =ALL;
GRANT EXECUTE ON DBMS_CRYPTO TO c##dbms_role_privs CONTAINER =ALL;
GRANT SELECT ANY DICTIONARY TO c##dbms_role_privs CONTAINER =ALL;
GRANT SELECT_CATALOG_ROLE TO c##dbms_role_privs CONTAINER =ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##dbms_role_privs CONTAINER =ALL;
GRANT SELECT ANY TABLE TO c##dbms_role_privs CONTAINER =ALL;
GRANT EXECUTE ON DBMS_LOGMNR TO c##dbms_role_privs CONTAINER =ALL;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO c##dbms_role_privs CONTAINER =ALL;
GRANT SELECT ON V_$ARCHIVED_LOG TO c##dbms_role_privs CONTAINER=ALL;
GRANT SELECT ON V_$LOG TO c##dbms_role_privs CONTAINER=ALL;
GRANT SELECT ON V_$LOGFILE TO c##dbms_role_privs CONTAINER=ALL;
GRANT FLASHBACK ANY TABLE TO c##dbms_role_privs CONTAINER =ALL;

GRANT c##dbms_role_privs TO c##dbmsadmin CONTAINER =ALL;
```

**基于 ORACLE LOGMINER 机制增量数据同步的必要条件**
1. 启用 ARCH 日志模式
2. 启用数据库或表补充日志，建议设置数据库级别

Example:
```sql
-- Connect
SQLPLUS / AS DBA
ALTER SESSION SET CONTAINER = CDB$ROOT;

-- Turn on archiving [required option]
ALTER DATABASE ARCHIVELOG;
    
-- Set archive directory
ALTER SYSTEM SET LOG_ARCHIVE_DEST_1='LOCATION=/DEPLOY/ORACLE/ORADATA/ARCHIVE' SCOPE=SPFILE SID='*';
    
-- Add minimum additional log [required option]
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

/*  

    Field append log
    Choose either table level or database level
    Generally, it can only be turned on for the synchronization table [required option]. Failure to turn it on will cause synchronization problems.
*/
-- Add database additional log
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
-- Clear database additional log
ALTER DATABASE DROP SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
    
/* Table additional log */
-- switch ${SCHEMA-NAME}.${TABLE-NAME} CONTAINER
ALTER SESSION SET CONTAINER =ORCLPDB;

-- Add table additional log
ALTER TABLE MARVIN.MARVIN4 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE MARVIN.MARVIN7 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE MARVIN.MARVIN8 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- Clear database additional log
ALTER TABLE MARVIN.MARVIN4 DROP SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE MARVIN.MARVIN7 DROP SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE MARVIN.MARVIN8 DROP SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

/* Query additional log */
-- Database level additional log viewing
SELECT
    SUPPLEMENTAL_LOG_DATA_MIN MIN,
	SUPPLEMENTAL_LOG_DATA_PK PK,
	SUPPLEMENTAL_LOG_DATA_UI UI,
	SUPPLEMENTAL_LOG_DATA_FK FK,
	SUPPLEMENTAL_LOG_DATA_ALL ALLC
FROM
    V$DATABASE;

-- Table level additional log viewing
SELECT * FROM DBA_LOG_GROUPS WHERE UPPER(OWNER) = UPPER('MARVIN');
```
-------

### Postgres Compatible Database
Postgres 兼容性数据库用于数据同步，DBMS 数据库分布式迁移服务平台所需的用户权限

```sql
-- create user
CREATE USER dbmsadmin WITH PASSWORD 'dbmsadmin';

-- create role
CREATE ROLE dbms_privs_role WITH CREATEDB;

-- grant system view permission
GRANT USAGE ON SCHEMA pg_catalog TO dbms_privs_role;
GRANT SELECT ON ALL TABLES IN SCHEMA pg_catalog TO dbms_privs_role;

GRANT USAGE ON SCHEMA information_schema TO dbms_privs_role;
GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO dbms_privs_role;

-- grant database permission (connect,create schema privs)
\c ${database_name};

GRANT CONNECT,CREATE ON DATABASE ${database_name} TO dbms_privs_role;

-- grant database schema permission
GRANT USAGE ON SCHEMA ${schema_name} TO dbms_privs_role;
GRANT SELECT ON ALL TABLES IN SCHEMA ${schema_name} TO dbms_privs_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA ${schema_name} GRANT SELECT ON TABLES TO dbms_privs_role;

-- grant role to user
GRANT dbms_privs_role TO dbmsadmin;
```

### MYSQL Compatible Database
MySQL 兼容性数据库用于数据同步，DBMS 数据库分布式迁移服务平台所需的用户权限

```sql
CREATE USER dbmsadmin@%;
GRANT ALL PRIVILEGES ON *.* TO 'dbmsadmin'@'%';
FLUSH PRIVILEGES;
```
