<h1 align="center">
  DBMS QUICK START MANUAL
</h1>
<p align="center">
  本文档用于描述 DBMS 分布式迁移服务平台使用指引手册
</p>

------
使用 DBMS 分布式迁移服务平台前，前提需要已安装部署 DBMS 分布式迁移服务平台，集群相关部署安装以及运维管理指引参见 [DBMS 集群运维管理](dbms_operation.md)。

NOTE：本文档前提假设已部署安装 DBMS 分布式迁移服务平台


**使用指引**

1，下载 oracle client，参考官网下载地址 https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html

2，上传 oracle client 至所有 **dbms-worker** 节点所在服务器，解压目录并配置环境变量
```shell
$ export LD_LIBRARY_PATH=${oracle client 解压目录所在路径}
```

3，查看已安装 DBMS 分布式迁移服务平台集群
```shell
$ dbms list

Name      User  Version  Path                          PrivateKey
----      ----  -------  ----                          ----------
dbms-jwt  dbms  v0.0.0   /Users/marvin/.dbms/dbms-jwt  /Users/marvin/.dbms/dbms-jwt/ssh/id_rsa
```

4，选定某个 DBMS 集群查看集群信息
```shell
$ dbms display dbms-jwt

Cluster type:       DBMS
Cluster name:       dbms-jwt
Cluster version:    v0.0.0
Deploy user:        marvin
SSH type:           none
ID                    Role                   Host             Ports      OS/Arch         Status   Data Dir                                         Deploy Dir                                        
--                    ----                   ----             -----      -------         ------   --------                                         ----------                                        
192.168.209.193:2379  dbms-master            192.168.209.193  2379/2380  darwin/aarch64  Healthy  /Users/marvin/gostore/dbms/deploy/master00/data  /Users/marvin/gostore/dbms/deploy/master00        
192.168.209.193:8261  dbms-worker (patched)  192.168.209.193  8261       darwin/aarch64  FREE     -                                                /Users/marvin/gostore/dbms/deploy/dbms-worker-8261
Total nodes: 2
```

5，DBMS 集群元数据库创建（dbmsctl）

元数据库配置文件[示例](../example/database.toml)
```shell
$ dbmsctl database upsert -s ${dbms-master-ip-leader}:${dbms-master-port} -c ${database.toml}
```

元数据任务提交完成，自动创建元数据库以及对应元数据表且所有 dbms-master 以及 dbms-worker 自动连接元数据库，预计等待时间 30-60s

6，DBMS 集群数据源创建（dbmsctl）

数据源配置文件[示例](../example/datasource.toml)
```shell
$ dbmsctl datasource upsert -s ${dbms-master-ip-leader}:${dbms-master-port} -c ${datasource.toml}
```

任务数据源可一次性创建多个数据源，该任务数据源用于后续具体任务执行引用

7，DBMS 集群任务创建（dbmsctl）

对象评估任务配置[示例](../example/assess_migrate_task.toml)
```shell
$ dbmsctl assess upsert -s ${dbms-master-ip-leader}:${dbms-master-port} -c ${assess_migrate_task.toml}
```
结构迁移任务配置[示例](../example/struct_migrate_task.toml)
```shell
$ dbmsctl struct upsert -s ${dbms-master-ip-leader}:${dbms-master-port} -c ${struct_migrate_task.toml}
```
结构对比任务配置[示例](../example/struct_compare_task.toml)
```shell
$ dbmsctl compare upsert -s ${dbms-master-ip-leader}:${dbms-master-port} -c ${struct_compare_task.toml}
```
SQL 数据迁移任务配置[示例](../example/stmt_migrate_task.toml)
```shell
$ dbmsctl stmt upsert -s ${dbms-master-ip-leader}:${dbms-master-port} -c ${stmt_migrate_task.toml}
```
CSV 数据迁移任务配置[示例](../example/csv_migrate_task.toml)
```shell
$ dbmsctl csv upsert -s ${dbms-master-ip-leader}:${dbms-master-port} -c ${csv_migrate_task.toml}
```
自定义 SQL 数据迁移任务配置[示例](../example/sql_migrate_task.toml)
```shell
$ dbmsctl sql upsert -s ${dbms-master-ip-leader}:${dbms-master-port} -c ${sql_migrate_task.toml}
```
数据校验任务配置[示例](../example/data_compare_task00.toml)
```shell
$ dbmsctl verify upsert -s ${dbms-master-ip-leader}:${dbms-master-port} -c ${data_compare_task00.toml}
```

8，DBMS 任务管理操作（dbmsctl）

以某个任务配置为例：task-name = "gct_33to45"

任务启动
```shell
$ dbmsctl task start -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name}
```

任务停止
```shell
$ dbmsctl task stop -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name}
```

任务删除（未启动运行任务状态）
```shell
$ dbmsctl task delete -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name}
```

设置定时任务
```shell
$ dbmsctl task crontab -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name} --express ${参考 linux crontab 写法}
```

清理定时任务（未启动运行任务状态）
```shell
$ dbmsctl task clear -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name}
```

8，获取结构迁移兼容以及不兼容性对象信息报告
```shell
$ dbms struct gen -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name} -o ${output-dir}
```

9，获取结构对比详细信息报告
```shell
$ dbms compare gen -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name} -o ${output-dir}
```

10，获取对象评估详细信息报告
```shell
$ dbms assess gen -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name} -o ${output-dir}
```

11，获取数据校验详细信息报告
```shell
$ dbms verify gen -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name} -o ${output-dir}
```

12，获取任务运行状态日志信息

```shell
$ dbms task get -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name}
```
任务日志信息当前只返回当前查询最后一条日志（日志量过大），输出格式如下
```json
{
  "taskName": "gct_33to45",
  "taskMode": "AssessMigrate",
  "datasourceNameS": "oracle33",
  "datasourceNameT": "tidb145",
  "taskStatus": "RUNNING",
  "workerAdd": "192.168.209.193",
  "logDetail": "xxxxxxx last log detail"
}
```

13，更多 DBMS 任务管理操作

```shell
$ dbmsctl --help

CLI dbmsctl app for dbms cluster

Usage:
  dbmsctl [flags]
  dbmsctl [command]

Available Commands:
  assess      Operator cluster data assess
  compare     Operator cluster struct compare
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