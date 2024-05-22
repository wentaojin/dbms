<h1 align="center">
  DBMS
</h1>

<p align="center">
    The DBMS is positioned as an integrated distributed migration service platform between heterogeneous databases
</p>

<div align="center">
  <a href="https://github.com/wentaojin/dbms/actions">
		<img src="https://img.shields.io/github/actions/workflow/status/wentaojin/dbms/release.yml"/>
  </a>
  <img src="https://img.shields.io/github/license/wentaojin/dbms"/>
  <img src="https://img.shields.io/github/downloads/wentaojin/dbms/total">
  <img src="https://img.shields.io/github/issues/wentaojin/dbms">
</div>

-------
<p align="center">
    <a href="#whats-included-">What's included 🚀</a> &bull;
    <a href="#architecture-">Architecture 🌈</a> &bull;
    <a href="#quick-start-">Quick Start 🛠️</a> &bull;
    <a href="#development-">Development 🧬</a> &bull;
    <a href="#customization-">Customization 🖍️</a> &bull;
    <a href="#license-">License 📓</a> &bull;
    <a href="#acknowledgments-">Acknowledgments ⛳</a>
</p>

-------
### What's included 🚀

- The Oracle to MYSQL compatible-database migration service
    - Schema Table structure conversion, supporting schema, table, column level and default value customization
    - Schema Table structure comparison
    - Schema table object assess
    - Schema table data migrate, supporting sql, csv consistent or non-consistent migration and custom sql migration
    - Schema table data verify
- As so on...

**TODO Feature**
- The oracle number datatype column sampling or full scan, used to identify whether the number data type field value exists in both integer and decimal types
- The mysql compatible database table structure migrate to oracle database
- The mysql compatible database table structure compare with oracle database
- The postgresql database table structure migrate to mysql compatible database
- The oracle data is synchronized to mysql compatible database in real time(base the logminer)

------
### Architecture 🌈

![DBMS ARCH](/image/dbms-arch.png "DBMS ARCH")

The DBMS cluster is composed of master, worker, dbmsctl and dbms components, respectively used for：
- master functions：instance register、service discovery、api access（only leader）
- worker functions: the task runner
- dbmsctl functions: deliver with the master node and submit the task
- dbms functions: provide cluster management operations such as cluster deployment and installation, expansion and contraction, start, stop and restart.

-------
### Quick Start 🛠️
[Operation Management](doc/dbms_operation.md)

[Permission Description](doc/dbms_permissions.md)

[Oracle Migrate MYSQL Manual](doc/oracle_migrate_manual.md)

-------
### Development 🧬
When the function development or the bug fixed is completed, the cluster can be quickly started locally to verify and test.

<span style="background-color:rgb(100,200,200,0.5)">NOTE: Unless there is something special, function development needs to be developed from a task perspective, that is, one function per task.</span>


**Quickly Started Cluster**
```shell
$ make runMaster
$ make runWorker
```
**Quickly Tested**
cluster cli help
```shell
$ go run component/cli/main.go --help
CLI dbmsctl app for dbms cluster

Usage:
  dbmsctl [flags]
  dbmsctl [command]

Available Commands:
  assess      Operator cluster data assess
  compare     Operator cluster data compare
  completion  Generate the autocompletion script for the specified shell
  csv         Operator cluster csv migrate
  database    Operator cluster database
  datasource  Operator cluster datasource
  decrypt     Operator cluster decrypt data
  help        Help about any command
  sql         Operator cluster sql migrate
  stmt        Operator cluster statement migrate
  struct      Operator cluster struct migrate
  task        Operator cluster task
  verify      Operator cluster data compare

Flags:
  -h, --help            help for dbmsctl
  -s, --server string   server addr for app server
  -v, --version         version for app client

Use "dbmsctl [command] --help" for more information about a command.
```

-------
### Customization 🖍️
If you like the project and want to buy me a cola or have tech exchange, you can button sponsor or join tech group:

| QQ Group                                      |
|-----------------------------------------------|
| <img src="image/tech-exchange.jpg" height="200" width="200"/> |


-------
### License 📓

This software is free to use under the Apache License.
