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

1，下载 dbms 离线安装包，参考官网下载地址 https://github.com/wentaojin/dbms/releases

Note：MacOS ARM64 无 instantClient，采用 AMD64 instanceClient 运行

2，上传 dbms 离线安装包至所在服务器，解压目录并进行集群部署
```shell
$ tar -zxvf dbms-community-{version}-{os}-{arch}.tar.gz
$ cd dbms-community-{version}-{os}-{arch}
$ sh local_install.sh
$ dbms-cluster deploy ${cluster_name} ${cluster_version} ${topology.yaml} --mirror-dir ${offline_package_path} -u ${deploy_user} 
```

3，查看已安装 DBMS 分布式迁移服务平台集群
```shell
$ dbms-cluster list

Name      User  Version  Path                          PrivateKey
----      ----  -------  ----                          ----------
dbms-jwt  dbms-cluster  v0.0.0   /Users/marvin/.dbms/dbms-jwt  /Users/marvin/.dbms/dbms-jwt/ssh/id_rsa
```

4，选定某个 DBMS 集群查看集群信息
```shell
$ dbms-cluster display dbms-jwt

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

5，DBMS 集群元数据库创建（dbms-ctl）

元数据库配置文件[示例](../example/database.toml)
```shell
$ dbms-ctl database upsert -s ${dbms-master-ip-leader}:${dbms-master-port} -c ${database.toml}
```

元数据任务提交完成，自动创建元数据库以及对应元数据表且所有 dbms-master 以及 dbms-worker 自动连接元数据库，预计等待时间 30-60s

6，DBMS 集群数据源创建（dbms-ctl）

数据源配置文件[示例](../example/datasource.toml)
```shell
$ dbms-ctl datasource upsert -s ${dbms-master-ip-leader}:${dbms-master-port} -c ${datasource.toml}
```

任务数据源可一次性创建多个数据源，该任务数据源用于后续具体任务执行引用

7，DBMS 集群任务创建（dbms-ctl）

对象评估任务配置[示例](../example/assess_migrate_task.toml)
```shell
$ dbms-ctl assess upsert -s ${dbms-master-ip-leader}:${dbms-master-port} -c ${assess_migrate_task.toml}
```
结构迁移任务配置[示例](../example/struct_migrate_task.toml)
```shell
$ dbms-ctl struct upsert -s ${dbms-master-ip-leader}:${dbms-master-port} -c ${struct_migrate_task.toml}
```
结构对比任务配置[示例](../example/struct_compare_task.toml)
```shell
$ dbms-ctl compare upsert -s ${dbms-master-ip-leader}:${dbms-master-port} -c ${struct_compare_task.toml}
```
SQL 数据迁移任务配置[示例](../example/stmt_migrate_task.toml)
```shell
$ dbms-ctl stmt upsert -s ${dbms-master-ip-leader}:${dbms-master-port} -c ${stmt_migrate_task.toml}
```
CSV 数据迁移任务配置[示例](../example/csv_migrate_task.toml)
```shell
$ dbms-ctl csv upsert -s ${dbms-master-ip-leader}:${dbms-master-port} -c ${csv_migrate_task.toml}
```
自定义 SQL 数据迁移任务配置[示例](../example/sql_migrate_task.toml)
```shell
$ dbms-ctl sql upsert -s ${dbms-master-ip-leader}:${dbms-master-port} -c ${sql_migrate_task.toml}
```
数据校验任务配置[示例](../example/data_compare_task00.toml)
```shell
$ dbms-ctl verify upsert -s ${dbms-master-ip-leader}:${dbms-master-port} -c ${data_compare_task00.toml}
```

8，DBMS 任务管理操作（dbms-ctl）

以某个任务配置为例：task-name = "gct_33to45"

任务启动
```shell
$ dbms-ctl task start -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name}
```

任务停止
```shell
$ dbms-ctl task stop -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name}
```

任务删除（未启动运行任务状态）
```shell
$ dbms-ctl task delete -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name}
```

设置定时任务
```shell
$ dbms-ctl task crontab -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name} --express ${参考 linux crontab 写法}
```

清理定时任务（未启动运行任务状态）
```shell
$ dbms-ctl task clear -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name}
```

8，获取结构迁移兼容以及不兼容性对象信息报告
```shell
$ dbms-ctl struct gen -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name} -o ${output-dir}
```

9，获取结构对比详细信息报告
```shell
$ dbms-ctl compare gen -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name} -o ${output-dir}
```

10，获取对象评估详细信息报告
```shell
$ dbms-ctl assess gen -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name} -o ${output-dir}
```

11，获取数据校验详细信息报告
```shell
$ dbms-ctl verify gen -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name} -o ${output-dir}
```

12，获取任务运行状态日志信息

```shell
$ dbms-ctl task get -s ${dbms-master-ip-leader}:${dbms-master-port} -t ${task-name}
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
$ dbms-ctl --help

CLI dbms-ctl app for dbms-cluster cluster

Usage:
  dbms-ctl [flags]
  dbms-ctl [command]

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
  -h, --help            help for dbms-ctl
  -s, --server string   server addr for app server
  -v, --version         version for app client

Use "dbms-ctl [command] --help" for more information about a command.
```