<h1 align="center">
  DBMS ORACLE MIGRATE MANUAL
</h1>
<p align="center">
  本文档用于描述 DBMS 分布式迁移服务平台 ORACLE 迁移功能
</p>

------
### 对象评估
收集现有 ORACLE 数据库中的表、索引、分区表、字段长度等信息，输出类似AWR报告文件的文件，用于评估迁移成本

------
### 数据扫描
数据扫描任务配置[示例](../example/data_scan_task.toml)

ORACLE MIGRATE MYSQL 兼容性数据库，提供以下功能实现（数据扫描 NUMBER 数据类型，识别 NUMBER 数据类型适配建议）：
- schema、表名区分大小写
- 全表以及表采样扫描，自定义表采样率以及 Hint
- 断点续传
- 一致性读/非一致性读

<mark>NOTE:</mark>
- 断点续传期间，参数配置文件 chunk-size 不能动态变更，但可通过配置 enable-checkpoint = false 自动清理断点以及已迁移的表数据，重新导出导入
- 如果程序遇到报错，进程不会终止，具体错误表以及对应的错误详情参见元数据表[data_scan_task]数据

------
### 结构迁移
结构迁移任务配置[示例](../example/struct_migrate_task.toml) 

ORACLE MIGRATE MYSQL 兼容性数据库，提供以下功能实现：
- schema、表和列名称区分大小写
- schema 表定义、索引、约束迁移
- 自定义配置路由规则映射
  - schema 名称路由
  - 数据库表名路由
  - 数据库表列名称路由
- 自定义配置迁移规则映射，迁移规则优先级：列->表->架构->任务->内置
  - 列数据类型自定义
  - 列默认值自定义
  - 表属性自定义（仅限 TiDB 数据库）
  - 列默认值内置 sysdate -> now() 和 sys_guid -> uuid 规则，并提供 [buildin_datatype_rule] 和 [buildin_defaultval_rule] 元数据表，能够编写更多规则
- 经过转换的表结构可直接在下游数据库进行创建
- ORACLE 迁移 MYSQL 兼容性数据库[内置列数据类型映射规则](../doc/buildin_rule_reverse_o.md)
- 数据库序列 Sequence 迁移（需要下游数据库支持序列）

 
<mark>NOTE:</mark>
- 分区表、临时表和聚簇表统一转换为普通表。如有必要，请获取完整的不兼容对象信息清单，然后进行手动转换
- 忽略物化视图转换且输出到不兼容对象信息清单
- MYSQL 兼容性数据库唯一约束基于唯一索引字段，MYSQL 兼容性数据库下游只会创建唯一索引
- ORACLE NUMBER 数据类型
  - 如果下游数据库是 TiDB，如无自定义规则统一以 Decimal 数据类型转换（规避 Decimal Join Bigint 数据类型字段性能低）
  - 如果下游数据库是 MYSQL，如无自定义规则按 TINYINT/SMALLINT/INT/BIGINT/DECIMAL 数据类型转换
- ORACLE FUNCTION-BASED NORMAL、BITMAP 不兼容索引输出到不兼容对象信息清单
- 表字符集和排序规则
  - 如果下游数据库是 TiDB，则忽略 ORACLE 数据库字符集，统一 UTF8MB4 和 UTF8MB4_BIN 转换
  - 如果下游数据库是 MYSQL，则会根据 ORACLE 数据库字符集和排序规则映射规则进行转换（例如：ZHS16GBK -> GBK）
- Schema 数据库表列函数默认值不转换，保留上游默认值，下游建表时是否报错取决于下游数据库是否支持当前函数默认值
- 如果程序遇到报错，进程不会终止，具体错误表以及对应的错误详情参见元数据表[struct_migrate_task]数据

------

### 结构对比
结构对比任务配置[示例](../example/struct_compare_task.toml)

ORACLE MIGRATE MYSQL 兼容性数据库，以上游数据库 ORACLE 表结构为基准，提供以下功能实现：
- schema、表和列名称区分大小写
- 自定义配置路由规则映射
  - 模式名称路由
  - 表名路由
  - 列名称路由
- 自定义配置迁移规则映射，迁移规则优先级：列->表->架构->任务->内置
  - 列数据类型自定义
  - 列默认自定义
- 索引以及约束对比，依据索引类型、约束类型既对比索引、约束名是否一致，也对比索引字段、约束字段是否一致

<mark>NOTE:</mark>
- 数据库表数据类型非自定义部分以内置转换规则为基准进行对比
- ORACLE 字符数据类型 CHAR/BYTES，默认 BYTES，而 MYSQL/TiDB 是字符长度，当前只对比数值大小，忽略字节/字符区别
- 字符集和排序规则
  - 如果下游数据库是 TiDB，则忽略 ORACLE 数据库字符集，统一 UTF8MB4 和 UTF8MB4_BIN 转换对比
  - 如果下游数据库是 MYSQL，则会根据 ORACLE 数据库字符集和排序规则映射规则进行转换对比（例如：ZHS16GBK -> GBK）
- 外键和检查约束
  - 如果下游数据库是 TiDB，则排除外键、检查约束对比
  - 如果下游数据库是 MYSQL，对于低版本只检查外键约束，高版本外键、检查约束都对比
- MYSQL 兼容性数据库 timestamp 类型只支持精度 6，ORACLE 精度最大是 9，会检查出来但是保持原样
- 如果程序遇到报错，进程不会终止，具体错误表以及对应的错误详情参见元数据表[struct_compare_task]数据

------
### 数据迁移 
STMT 数据迁移任务配置[示例](../example/stmt_migrate_task.toml)

CSV 数据迁移任务配置[示例](../example/csv_migrate_task.toml)

ORACLE MIGRATE MYSQL 兼容性数据库，提供以下功能实现：
- schema、表和列名称区分大小写
- 自定义配置路由规则映射
  - 模式名称路由
  - 表名路由
  - 列名称路由
- 自定义数据库表迁移范围以及配置 SQL Hint 
- 断点续传
- 一致性/非一致性读
- 表数据字符集以数据源配置 charset 进行传输自动转换
- 基于 ORACLE 数据库版本支持自动识别并采用最优的方式迁移
- 数据存放空间检查是否满足数据表大小，不满足自动跳过，直至找到满足条件的数据表导出或者任务结束 （only csv migrate task）

<mark>NOTE:</mark>
- 数据迁移要求数据表存在主键或者唯一键，否则因异常错误退出或者手工中断退出，断点续传【replace into】无法替换，数据可能会导致重复【除非手工清理下游重新导入】
- 断点续传期间，参数配置文件 chunk-size 不能动态变更，但可通过配置 enable-checkpoint = false 自动清理断点以及已迁移的表数据，重新导出导入
- 基于 ORACLE 数据库版本支持自动识别并采用最优的方式迁移
- 如果程序遇到报错，进程不会终止，具体错误表以及对应的错误详情参见元数据表[data_migrate_task]数据


SQL 数据迁移任务配置[示例](../example/sql_migrate_task.toml)

ORACLE MIGRATE MYSQL 兼容性数据库，提供以下功能实现：
- schema、表和列名称区分大小写
- 自定义配置路由规则映射
  - 列名称路由
- 一致性/非一致性读
- 表数据字符集以数据源配置 charset 进行传输自动转换

<mark>NOTE:</mark>
- 数据迁移要求数据表存在主键或者唯一键，否则因异常错误退出或者手工中断退出，断点续传【replace into】无法替换，数据可能会导致重复【除非手工清理下游重新导入】
- 如果程序遇到报错，进程不会终止，具体错误表以及对应的错误详情参见元数据表[sql_migrate_task]数据
------
### 数据校验

数据校验任务配置[示例](../example/data_compare_task00.toml)

ORACLE MIGRATE MYSQL 兼容性数据库，以上游 ORACLE 数据库为基准，提供以下功能实现：
- schema、表和列名称区分大小写
- 自定义配置路由规则映射
  - 模式名称路由
  - 表名路由
  - 列名称路由
- 自定义数据库表数据校验范围、校验字段以及可忽略的校验字段
- 上游一致性/非一致性读，下游当前读
- 支持程序 CRC32、数据库 MD5 方式数据校验以及只校验数据表行数
- 断点续传
- 移除主键、唯一键、唯一索引要求限制，采用数据对比过程结合数据记录行数规避

<mark>NOTE:</mark>
- 数据库版本要求 >= ORACLE 10G
- 除 LONG/LONG RAW/BFILE 数据类型字段表，采用程序 CRC32 方式校验外，其他的采用数据库 MD5 方式校验，但可通过忽略 LONG/LONG RAW/BFILE 数据类型字段规避方式来使用数据库 MD5 方式校验
- 只校验数据行数产生的差异不会输出详情修复文件，元数据表只记录数据表不相等，对应的上下游表行数程序日志会显示记录
- 自定义表校验规则
  - compare-field/compare-range 参数，compare-range 优先级高于 compare-field，仅当两个都配置时，以 compare-range 为准
  - compare-field 参数字段是否是索引字段，需自行确认保证，非索引字段影响校验效率以及数据库资源消耗
- 断点续传期间，参数配置文件 chunk-size 不能动态变更，但可通过配置 enable-checkpoint = false 自动清理断点以及已迁移的表数据，重新导出导入
- 如果程序遇到报错，进程不会终止，具体错误表以及对应的错误详情参见元数据表[data_compare_task]数据

### 实时同步（未来计划）

ORACLE MIGRATE MYSQL 兼容性数据库，计划基于 logminer 提供增量数据实时同步功能

<mark>NOTE:</mark>
- 基于 logminer 日志进行增量数据同步，存在 logminer 同等限制，且只同步 INSERT/DELETE/UPDATE DML 以及 DROP TABLE/TRUNCATE TABLE DDL，执行过 TRUNCATE TABLE/ DROP TABLE 可能需要重新增加表附加日志
- 基于 logminer 日志数据同步，挖掘速率取决于重做日志磁盘 + 归档日志磁盘【若在归档日志中】以及 PGA 内存
- 全量数据迁移参见 <a href="#数据迁移-">数据迁移</a>