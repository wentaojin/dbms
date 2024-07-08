<h1 align="center">
  DBMS OPERATION
</h1>
<p align="center">
本文档用于描述 DBMS 数据库分布式迁移服务平台运维管理手册指引
</p>

-------
DBMS Command CLI 是 DBMS 集群管理组件，通过它可以进行日常运维工作，包括部署、启动、关闭、销毁、弹性扩缩容、TiDB 集群升级、DBMS 集群参数管理等。

**Component Download**

从 github 发布页面下载离线包并解压包文件并 shell 安装

```shell
sh local_install.sh
```

**Cluster Operation**

部署用户 sudo 权限

Note：
1. Linux 平台部署集群节点所在服务器必需已提前创建部署用户且必需具备至少具备以下 sudo 权限，非 Linux 平台部署集群节点所在服务器必需已提前部署用户且具备 sudo all 权限（理论同样以下权限也可以，但未实际测试验证）
    ```shell
    {deploy_user} ALL=(root) NOPASSWD: /bin/bash -c uname *
    {deploy_user} ALL=(root) NOPASSWD: /bin/bash -c id *
    {deploy_user} ALL=(root) NOPASSWD: /usr/sbin/groupadd *
    {deploy_user} ALL=(root) NOPASSWD: /usr/sbin/useradd *
    {deploy_user} ALL=(root) NOPASSWD: /usr/bin/chown *
    {deploy_user} ALL=(root) NOPASSWD: /usr/bin/echo *
    {deploy_user} ALL=(root) NOPASSWD: /bin/bash -c su - *
    {deploy_user} ALL=(root) NOPASSWD: /bin/bash -c mkdir -p *
    {deploy_user} ALL=(root) NOPASSWD: /bin/bash -c chmod *
    {deploy_user} ALL=(root) NOPASSWD: /bin/bash -c test *
    {deploy_user} ALL=(root) NOPASSWD: /bin/bash -c mv *
    ```
2. Linux 平台参数配置文件拓扑 topology 运行用户会自动生成最小 sudo 权限，而非 Linux 平台参数配置文件 topology 运行用户统一自动生成 sudo all 权限运行

集群部署参数配置[示例](../example/topology.yaml)  
```shell
dbms-cluster deploy ${cluster_name} ${cluster_version} ${topology.yaml} --mirror-dir ${offline_package_path} -u ${deploy_user} 
```

更多集群运维管理命令

```shell
the application for the dbms cluster

Usage:
  dbms-cluster [flags]
  dbms-cluster [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  deploy      Deploy a cluster for production
  destroy     Destroy a specified DBMS cluster
  disable     Disable automatic enabling of DBMS clusters at boot
  display     Display information of a DBMS cluster
  edit-config Edit DBMS cluster config
  enable      Enable automatic enabling of DBMS clusters at boot
  help        Help about any command
  list        List all clusters
  patch       Replace the remote package with a specified package and restart the service
  reload      Reload a DBMS cluster's config and restart if needed
  restart     Restart a DBMS cluster
  scale-in    Scale in a DBMS cluster
  scale-out   Scale out a DBMS cluster
  start       Start a DBMS cluster
  stop        Stop a DBMS cluster
  upgrade     Upgrade a specified DBMS cluster

Flags:
  -c, --concurrency int     max number of parallel tasks allowed (default 5)
      --format string       (EXPERIMENTAL) The format of output, available values are [default, json] (default "default")
  -h, --help                help for dbms
      --meta-dir string     The meta dir is used to storage dbms meta information (default "/Users/marvin/.dbms")
      --mirror-dir string   The mirror dir is used to storage dbms component tar package
      --skip-confirm        the operation skip confirm, always yes
      --ssh string          (EXPERIMENTAL) The executor type: 'builtin', 'none' (default "builtin")
      --ssh-timeout uint    Timeout in seconds to connect host via SSH, ignored for operations that don't need an SSH connection (default 5)
  -v, --version             version for app client
      --wait-timeout uint   Timeout in seconds to wait for an operation to complete, ignored for operations that don't fit (default 120)

Use "dbms-cluster [command] --help" for more information about a command.
```
