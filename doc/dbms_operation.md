<h1 align="center">
  DBMS OPERATION
</h1>
<p align="center">
    This document is used to describe the operation required by the DBMS sufficient platform
</p>

-------
The DBMS Command CLI is a DBMS cluster management component, through which you can perform daily operation and maintenance work, including deployment, startup, shutdown, destruction, elastic expansion and contraction, upgrade of TiDB cluster, and management of DBMS cluster parameters.

**Component Download**

download the offline package from the github release page and unzip the package files and shell install
```shell
sh local_install.sh
```

**Cluster Operation**

cluster deploy topology [example](../example/topology.yaml)  
```shell
dbms deploy ${cluster_name} ${cluster_version} ${topology.yaml} --mirror-dir ${offline_package_path} -u ${deploy_user} 
```

cluster operation help

```shell
the application for the dbms cluster

Usage:
  dbms [flags]
  dbms [command]

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

Use "dbms [command] --help" for more information about a command.
```
