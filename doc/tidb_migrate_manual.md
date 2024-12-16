<h1 align="center">
  DBMS TiDB MIGRATE MANUAL
</h1>
<p align="center">
  本文档用于描述 DBMS 分布式迁移服务平台 TiDB 迁移功能
</p>

------
### 数据校验

数据校验任务配置[示例](../example/data_compare_task.toml)

TiDB MIGRATE ORACLE、POSTGRES 兼容性数据库，以上游 TiDB 数据库为基准，提供以下功能实现：
- schema、表和列名称区分大小写
- 自定义配置路由规则映射
  - 模式名称路由
  - 表名路由
  - 列名称路由
- 自定义数据库表数据校验范围、校验字段以及可忽略的校验字段
- 上游一致性/非一致性读
- 支持程序 CRC32、数据库 MD5 方式数据校验以及只校验数据表行数
- 断点续传
- 移除主键、唯一键、唯一索引要求限制，采用数据对比过程结合数据记录行数规避

<mark>NOTE:</mark>
- 数据库版本要求 >= ORACLE 10G，MYSQL Compatible Database Null And "" 不额外处理，以原始值对比
- ORACLE 除 LONG/LONG RAW/BFILE 数据类型字段表，采用程序 CRC32 方式校验外，其他的采用数据库 MD5 方式校验，但可通过忽略 LONG/LONG RAW/BFILE 数据类型字段规避方式来使用数据库 MD5 方式校验
- 对于无精度 Number 数据类型统一以 TO_CHAR(38 位整数,24 位小数) VS DECIMAL(65,24) 数据对比，优先保整数部分（TO_CHAR 格式化 MAX 62 位）
- 为尽可能避免 ORA-22835 字段拼接超过 4000，对于字符数据超过 32 字符或者 LOB 类型数据提前 MD5 加密运算，其他维持原样运算
- 只校验数据行数产生的差异不会输出详情修复文件，元数据表只记录数据表不相等，对应的上下游表行数程序日志会显示记录
- 自定义表校验规则
  - compare-field/compare-range 参数，compare-range 优先级高于 compare-field，仅当两个都配置时，以 compare-range 为准
  - compare-field 参数字段是否是索引字段，需自行确认保证，非索引字段影响校验效率以及数据库资源消耗
- 断点续传期间，参数配置文件 chunk-size 不能动态变更，但可通过配置 enable-checkpoint = false 自动清理断点以及已迁移的表数据，重新导出导入
- 如果程序遇到报错，进程不会终止，具体错误表以及对应的错误详情参见元数据表[data_compare_task]数据

### 实时同步

实时同步任务配置[示例](../example/cdc_consume_msg.toml)

TiDB MIGRATE ORACLE、POSTGRES、MYSQL、TIDB 兼容性数据库，基于 TiCDC + Kafka 提供增量数据实时同步功能
- schema、表和列名称区分大小写
- 自定义配置路由规则映射
  - 模式名称路由
  - 表名路由
  - 列名称路由
- 表维度并发消费，可自定义过滤筛选表同步

<mark>NOTE:</mark>
- TiDB 数据库版本必须是 v6.5.5 <= X < v7.0.0 或者 X >= v7.1.2 数据库版本 
- TiDB 集群上下游要求必须存在有效索引（主键或者 NOT NULL 唯一索引），否则不保证上下游数据一致性
- TiDB 支持同步 DDL、DML Event，但不支持非表级别的同步，比如：CREATE DATABASE、DROP DATABASE、ALTER DATABASE 等数据实时同步
- TiCDC changefeed large-message-handle-option claim-check 以及 handle-key-only 不支持配置，但支持 compression 压缩参数
- TiCDC changefeed topic partition-num 参数值要求跟 Kafka 集群对应 topic 分区数相同，数据消费依赖 partition-num 自动协调多分区 DDL 同步，DDL 语句同步可能存在因语法不支持同步报错，可根据具体 DDL rewrite 重写继续同步
- 禁止 DDL 协调期间，进行 ticdc changefeed topic 扩容 partition 操作，否则容易产生某个分区因无法接受到 DDL 消息而假死 Hang 暂停消费的状态，数据无法同步
- 非 DDL 协调期间，进行 ticdc changefeed topic 扩容 partition 操作，需要重启实时同步任务以便可自动识别新增分区，否则可能遗漏新扩容分区数据同步
- Kafka 建议以集群模式且 partition 多副本形式运行， 以便 Kafka 节点实现高可用容灾
- TiDB 全量数据迁移暂不支持，当前只支持增量数据实时同步功能

基于 TiCDC + Kafka 数据实时同步，关键参数建议：

1. TiCDC Changefeed 关键参数

protocol=open-protocol（必须）

partition-num=3

max-message-bytes=67108864（根据数据同步消息大小设置）

replication-factor=3

ticdc changefeed large-message-handle-option claim-check 以及 handle-key-only 不支持配置，但支持 compression 压缩参数

```
--sink-uri="kafka://{serverAddr}:{serverPort}/{default-cdc-topic}?protocol=open-protocol&kafka-version=3.9.0&partition-num=3&auto-create-topic=true&max-message-bytes=67108864&replication-factor=3&max-batch-size=32" --changefeed-id="repl-kafka-task"
```
2. TiCDC 配置文件关键参数

enable-old-value = true

sink dispatchers  partition = "index-value"

```
enable-old-value = true
[filter]
rules = ["marvin.*"]
[sink]
dispatchers = [
    {matcher = ['marvin.*'], topic = "cdc_{schema}_topic", partition = "index-value" },
]
```
3. Kafka 集群参数关键参数(v3.9.0)

num.partitions = ticdc changefeed partition-num （建议通过 ticdc changefeed 自动创建 topic 以及 partition-nums，否则需要人为控制 kafka =  ticdc partition 配置参数）

message.max.bytes >= tidc changefeed max-message-bytes

message.max.bytes <= fetch.max.bytes = replica.fetch.max.bytes
```
num.partitions = 3
message.max.bytes	= 104857600
fetch.max.bytes = 124857600
replica.fetch.max.bytes	= 124857600
```