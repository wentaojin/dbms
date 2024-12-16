<h1 align="center">
  DBMS POSTGRES MIGRATE MANUAL
</h1>
<p align="center">
  本文档用于描述 DBMS 分布式迁移服务平台 POSTGRES 迁移功能
</p>

------
### 结构迁移
结构迁移任务配置[示例](../example/struct_migrate_task.toml) 

Postgres MIGRATE MYSQL 兼容性数据库，提供以下功能实现：
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
  - 列默认值无内置规则，并提供 [buildin_datatype_rule] 和 [buildin_defaultval_rule] 元数据表，能够编写更多规则
- 经过转换的表结构可直接在下游数据库进行创建
- 断点续传
- Postgres 迁移 MYSQL 兼容性数据库[内置列数据类型映射规则](../doc/buildin_rule_reverse_p)
- 数据库序列 Sequence 迁移（需要下游数据库支持序列）

 
<mark>NOTE:</mark>
- POSTGRES 数据库版本要求 >= 9.5，数据库字符类型支持 PostgreSQL EUC_TW、UTF8、EUC_CN、排序规则支持 C、POSIX、ZH_TW、ZH_TW.UTF8、ZH_TW.UTF-8、ZH_CN、ZH_CN.UTF8、ZH_CN.UTF-8、EN_US、EN_US.UTF8、EN_US.UTF-8
  - 如果下游数据库是 MYSQL / TiDB，则忽略 POSTGRES 数据库字符集，统一 UTF8MB4 和 UTF8MB4_BIN、UTF8MB4_0900_AI_CI 转换
- 分区表、临时表、聚簇表、外部表以及复合表统一转换为普通表。如有必要，请获取完整的不兼容对象信息清单，然后进行手动转换
- 物化视图、普通视图统一忽略转换且输出到不兼容对象信息清单
- MYSQL 兼容性数据库唯一约束基于唯一索引字段，MYSQL 兼容性数据库下游只会创建唯一索引
- POSTGRES 不兼容索引输出到不兼容对象信息清单
- Schema 数据库表列函数默认值不转换，保留上游默认值，下游建表时是否报错取决于下游数据库是否支持当前函数默认值
- 如果程序遇到报错，进程不会终止，具体错误表以及对应的错误详情参见元数据表[struct_migrate_task]数据

------

### 结构对比
结构对比任务配置[示例](../example/struct_compare_task.toml)

POSTGRES MIGRATE MYSQL 兼容性数据库，以上游数据库 POSTGRES 表结构为基准，提供以下功能实现：
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
- 字符集和排序规则
  - 如果下游数据库是 TiDB / MYSQL，则忽略 POSTGRES 数据库字符集，统一 UTF8MB4 转换对比，而排序规则根据内置规则映射转换对比 UTF8MB4_BIN、UTF8MB4_0900_AI_CI 对比
- 外键和检查约束
  - 如果下游数据库是 TiDB，则排除外键、检查约束对比
  - 如果下游数据库是 MYSQL，对于低版本只检查外键约束，高版本外键、检查约束都对比
- 如果程序遇到报错，进程不会终止，具体错误表以及对应的错误详情参见元数据表[struct_compare_task]数据

------
### 数据迁移 
STMT 数据迁移任务配置[示例](../example/stmt_migrate_task.toml)

CSV 数据迁移任务配置[示例](../example/csv_migrate_task.toml)

POSTGRES MIGRATE MYSQL 兼容性数据库，提供以下功能实现：
- schema、表和列名称区分大小写
- 自定义配置路由规则映射
  - 模式名称路由
  - 表名路由
  - 列名称路由
- 自定义数据库表迁移范围以及配置 SQL Hint 
- 断点续传
- 一致性/非一致性读
- 表数据字符集以数据源配置 charset 进行传输自动转换
- 数据存放空间检查是否满足数据表大小，不满足自动跳过，直至找到满足条件的数据表导出或者任务结束 （only csv migrate task）
- TiDB 数据库 csv 数据迁移支持自动导入（Require: TiDB Version >= v7.5）

<mark>NOTE:</mark>
- 数据迁移要求数据表存在主键或者唯一键，否则因异常错误退出或者手工中断退出，断点续传【replace into】无法替换，数据可能会导致重复【除非手工清理下游重新导入】
- 断点续传期间，参数配置文件 chunk-size 不能动态变更，但可通过配置 enable-checkpoint = false 自动清理断点以及已迁移的表数据，重新导出导入
- 如果程序遇到报错，进程不会终止，具体错误表以及对应的错误详情参见元数据表[data_migrate_task]数据

------
### 数据校验

数据校验任务配置[示例](../example/data_compare_task.toml)

POSTGRES MIGRATE MYSQL 兼容性数据库，以上游 POSTGRES 数据库为基准，提供以下功能实现：
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
- 只校验数据行数产生的差异不会输出详情修复文件，元数据表只记录数据表不相等，对应的上下游表行数程序日志会显示记录
- 自定义表校验规则
  - compare-field/compare-range 参数，compare-range 优先级高于 compare-field，仅当两个都配置时，以 compare-range 为准
  - compare-field 参数字段是否是索引字段，需自行确认保证，非索引字段影响校验效率以及数据库资源消耗
- 断点续传期间，参数配置文件 chunk-size 不能动态变更，但可通过配置 enable-checkpoint = false 自动清理断点以及已迁移的表数据，重新导出导入
- 如果程序遇到报错，进程不会终止，具体错误表以及对应的错误详情参见元数据表[data_compare_task]数据

### 实时同步（未来计划）

POSTGRES MIGRATE MYSQL 兼容性数据库，计划ing

<mark>NOTE:</mark>
- 全量数据迁移参见 <a href="#数据迁移-">数据迁移</a>