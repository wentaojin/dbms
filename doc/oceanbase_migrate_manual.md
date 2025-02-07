<h1 align="center">
  DBMS TiDB MIGRATE MANUAL
</h1>
<p align="center">
  本文档用于描述 DBMS 分布式迁移服务平台 OCEANBASE 迁移功能
</p>


### 实时同步

实时同步任务配置[示例](../example/cdc_consume_msg.toml)

OCEANBASE MIGRATE ORACLE、POSTGRES、MYSQL、TIDB 兼容性数据库，基于 OMS + Kafka 提供增量数据实时同步功能
- SCHEMA 名称区分大小写
- TABLE 名称区分大小写
- COLUMN 名称区分大小写
- 自定义配置路由规则映射
  - SCHEMA 名称路由
  - TABLE 名称路由
  - COLUMN 名称路由
- 表维度并发消费，可自定义过滤筛选表同步
- 上游 ONLY 支持 OCEANBASE MYSQL 模式
- 支持 OCEANBASE OMS 全量 + 增量迁移（全量 ROW）

<mark>NOTE:</mark>

OMS KAFKA 同步链路创建、注意事项、同步 DDL 支持范围以及 DDL 语句投递说明参考[跳转链接](https://www.oceanbase.com/docs/enterprise-oms-doc-cn-1000000001781598)

约束类别|注意事项|备注|
:---|:---|:---|
数据库版本|1. 同步 OceanBase 数据库 V2.2.73 及之后版本、V3.x 之前版本的数据时，请使用 OBCDC V2.2.73 之后、V3.x 之前版本或 OBCDC V3.2.4.5 之后版本，以确保事务内 DML 行变更的顺序<br>2. 同步 OceanBase 数据库 V2.2.73 之前版本的数据时，无法保证事务内 DML 行变更的顺序||
数据表对象| OMS 数据同步的对象仅支持物理表，不支持其它对象||
消息格式| OMS 必须选 DefaultExtendColumnType JSON 消息格式，其他消息格式不支持||
分区规则| OMS 必须选 Hash 分区规则，根据主键值或分片列值 Hash 选择 Kafka Topic 的分区||
Kafka 版本|OMS 支持的 Kafka 版本为 V0.9、V1.0、V2.x 和 V3.x,当 Kafka 版本为 V0.9 时，不支持结构同步|测试环境版本 Kafka v3.9.0|



