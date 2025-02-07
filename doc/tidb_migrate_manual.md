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
- SCHEMA 名称区分大小写
- TABLE 名称区分大小写
- COLUMN 名称区分大小写
- 自定义配置路由规则映射
  - SCHEMA 名称路由
  - TABLE 名称路由
  - COLUMN 名称路由
- 自定义数据库表数据校验范围、校验字段以及可忽略的校验字段
- 上游一致性/非一致性读，下游当前读
- 支持程序 CRC32、数据库 MD5 方式数据校验以及只校验数据表行数
- 断点续传
- 非强制要求表结构必须存在有效唯一字段索引，采用数据对比过程结合数据记录行数规避

<mark>NOTE:</mark>
| 约束类型        | 注意事项                                                                                                                                                                                                                                                                                                     | 备注 |
| :-------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :--- |
| 数据库版本      | ORACLE Require Version >= 10G，MYSQL 兼容性数据库 Null And "" 不额外处理，以原始值对比                                                                                                                                                                                                                       |      |
| 数据校验        | 1. ORACLE 除 LONG/LONG RAW/BFILE 数据类型字段表，采用程序 CRC32 方式校验外，其他的采用数据库 MD5 方式校验，但可通过忽略 LONG/LONG RAW/BFILE 数据类型字段规避方式来使用数据库 MD5 方式校验<br>2. ONLY 校验数据行数产生的差异不会输出详情修复文件，元数据表只记录数据表不相等，但对应的上下游表行数程序日志会显示记录|      |
| NUMBER 数据类型 | 对于无精度 Number 数据类型统一以 TO_CHAR(38 位整数,24 位小数) VS DECIMAL(65,24) 数据对比，优先保整数部分（TO_CHAR 格式化 MAX 62 位）                                                                                                                                                                         |      |
| 字符长度        | 为尽可能避免 ORA-22835 字段拼接超过 4000，LOB 类型数据提前 MD5 加密运算，其他维持原样运算                                                                                                                                                                                                                    |      |
| 校验规则        | 1. compare-field/compare-range 参数，compare-range 优先级高于 compare-field，仅当两个都配置时，以 compare-range 为准<br>2. compare-field 参数字段是否是索引字段，需自行确认保证，非索引字段影响校验效率以及数据库资源消耗                                                                                    |      |
| 断点续传        | 断点续传期间，参数配置文件 chunk-size 不能动态变更，但可通过配置 enable-checkpoint = false 自动清理断点以及已迁移的表数据，重新导出导入                                                                                                                                                                      |      |
| 错误处理        | 如果程序遇到报错，进程不会终止，具体错误表以及对应的错误详情参见元数据表 [data_compare_task] 数据                                                                                                                                                                                                            |      |

### 实时同步

实时同步任务配置[示例](../example/cdc_consume_msg.toml)

TiDB MIGRATE ORACLE、POSTGRES、MYSQL、TIDB 兼容性数据库，基于 TiCDC + Kafka 提供增量数据实时同步功能
- SCHEMA 名称区分大小写
- TABLE 名称区分大小写
- COLUMN 名称区分大小写
- 自定义配置路由规则映射
  - SCHEMA 名称路由
  - TABLE 名称路由
  - COLUMN 名称路由
- 表维度并发消费，可自定义过滤筛选表同步
- ONLY 支持增量迁移

<mark>NOTE:</mark>
约束类别|注意事项|备注|
:---|:---|:---|
集群版本|1，v6.5.5 <= X < v7.0.0<br>2，X >= v7.1.2|TiDB 集群版本|
数据库表结构|必须存在有效索引（主键或者 NOT NULL 唯一索引且不存在虚拟生成列），否则不保证上下游数据一致性||
Event 消费|支持数据库表级别 DDL、DML Event 消费同步，但不支持非表级别的同步，比如：CREATE DATABASE、DROP DATABASE、ALTER DATABASE、CREATE USER 等数据实时同步|非表级别数据分发 default-topic 所在分区，除非 default-topic 与 dispatcher topic 指定同个 topic<br>https://github.com/pingcap/tiflow/issues/11882|
Changefeed 参数|1，large-message-handle-option claim-check 以及 handle-key-only 不支持配置，但支持 compression 压缩参数<br>2，topic partition-num 参数值要求跟 kafka 集群对应 topic 分区数相同，数据消费依赖 partition-num 自动协调多分区 DDL 同步，DDL 语句同步可能存在因语法不支持同步报错，可根据具体 DDL rewrite 重写继续同步，保留 rewrite 记录可重复多次生效||
数据库表类型|带有虚拟生成列的数据表同步不支持，可能引入数据不一致问题，但支持存储 store 生成列的数据表同步，对于 store 生成列同步，当上下游表对应字段都存在 virtual column，自动忽略写入下游自动根据表达式自动计算生成值，否则按消息原值同步消费||
DDL 协调|1，DDL 协调期间，禁止进行 changefeed topic partition 扩容操作，否则重启实时同步任务之后容易产生某个分区因无法接受到 DDL Event 而导致假死 HANG 暂停消费，数据无法同步<br>2，非 DDL 协调期间，可进行 changefeed topic partition 扩容操作，但需要重启实时同步任务以便可自动识别新增分区，否则遗漏数据同步，导致上下游数据不一致||
Kafka 运行模式|建议以 Kafka 集群模式 +  partition 多副本形式运行，以实现 kafka 节点数据高可用容灾||
消息字段类型|ticdc 产生的消息事件同个字段类型 id 可能代表不同的数据类型，而不同数据类型对应下游数据库数据类型不一样，比如: text -> clob，blob -> blob，消费处理过程无法识别下游数据类型具体是哪个，自动基于下游元数据辨别，以区分什么类型数据形式（string or []byte）传递<br>TINYTEXT、TINYBLOB -> 249<br>MEDIUMTEXT、MEDIUMBLOB -> 250<br>LONGTEXT、LONGBLOB -> 251<br>TEXT、BLOB -> 252||
数据类型|1，oracle、postgres 数据库不支持特殊字段数据类型 enum、set 同步，而 mysql、tidb 数据库理论可正常支持<br>2，下游数据库 oracle bfile 数据类型同步报错不支持，上游 tidb 常规用 varchar 存放文件路径或者 longtext / blob 存放数据，而下游 oracle bfile 存放文件指针<br>3. 除 mysql、tidb 以外的所有下游数据库 DELETE 语句 char 类型统一自适应字段长度自动补齐空格，以避免因空格缺失而导致数据无法正确处理的问题（比如: oracle 数据库查询或者 DML 操作不会自动补空格导致无法查询到数据），而 INSERT 语句以上游 tidb char 数据类型原值同步消费||
DELETE 语句|所有下游数据库 DELETE 语句统一以上游数据库消息有效索引字段（主键 OR NOT NULL 唯一索引）删除，原因尽可能规避下游数据库类似 TEXT、BLOB、LONG、LONG RAW 等大字段文本或者二进制数据字段类型，存在 WHERE 查询条件限制而删除报错（类似 oralce ORA-00932: inconsistent datatypes: expected - got XXXX）||
NULL OR 空字符串|所有下游数据库 NULL 以及空字符串按消息原值同步，需要注意 oracle NULL 以及空字符串是相等的，而 pg、mysql、tidb 数据库 NULL 以及空字符串是不相等的||

<mark>NOTE:</mark>
| 数据类型 | 类型说明 | oracle | postgres | mysql | tidb | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| date | 日期类型，存储年月日 | 常规 date/timestamp 数据类型承载，自适应数据类型采用 to_date 统一带时分秒或者 to_timestamp 统一带时分秒毫秒 6 位，其他数据类型不自动处理，原值写入 | Y(date) | Y | Y |  |
| datetime | 时间类型，可存储年月日时分秒 | 同上 | Y(timstamp) | Y | Y |  |
| timestamp | 时间类型，可存储年月日时分秒毫秒 | 同上 | Y(timstamp) | Y | Y |  |
| year | 自然年类型，存储年 | 常规 number/varchar2 数据类型承载，不自动处理，原值写入 | Y(int) | Y | Y |  |
| time | 可用于指示一天内的时间，还可用于指两个事件之间的时间间隔 | 常规 date、timsestamp、interval day 数据类型承载，自适应下游数据库对应字段进行额外格式化处理（to_date/to_timestamp 只带时分秒、interval 格式化），其他数据类型承载不自动处理，原值写入 | 常规 time、interval 数据类型承载，自适应下游数据库对应字段进行额外格式化处理（time 原值写入、interval 格式化），其他数据类型承载不自动处理，原值写入 | Y | Y |  |


**TiCDC + Kafka 数据实时同步使用参数建议：**

1. TiCDC Changefeed 关键参数

```
--sink-uri="kafka://{serverAddr}:{serverPort}/{default-cdc-topic}?protocol=open-protocol&kafka-version=3.9.0&partition-num=3&auto-create-topic=true&max-message-bytes=67108864&replication-factor=3&max-batch-size=32" --changefeed-id="repl-kafka-task"

必须满足：
protocol=open-protocol

建议参数：
partition-num=3
max-message-bytes=67108864（根据数据同步消息大小设置）
replication-factor=3
auto-create-topic=true
max-batch-size=32
```
1. TiCDC 配置文件关键参数

```
enable-old-value = true
[filter]
rules = ["marvin.*"]
[sink]
dispatchers = [
    {matcher = ['marvin.*'], topic = "cdc_{schema}_topic", partition = "index-value" },
]

必须满足：
enable-old-value = true
sink dispatchers  partition = "index-value"
```
3. Kafka 集群参数关键参数(v3.9.0)

```
num.partitions = 3
message.max.bytes	= 104857600
fetch.max.bytes = 124857600
replica.fetch.max.bytes	= 124857600

建议参数：
num.partitions = ticdc changefeed partition-num （建议通过 ticdc changefeed 自动创建 topic 以及 partition-nums，否则需要人为控制 kafka =  ticdc partition 配置参数）
message.max.bytes >= tidc changefeed max-message-bytes
message.max.bytes <= fetch.max.bytes = replica.fetch.max.bytes
```