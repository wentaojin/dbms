<h1 align="center">
  DBMS ORACLE MIGRATE MYSQL
</h1>
<p align="center">
    This document is used to describe the oracle migrate function by the DBMS sufficient platform
</p>

------
### Schema Object Assess
Collect information such as tables, indexes, partition tables, field lengths and other information in the existing ORACLE database, and output a file similar to AWR report file to evaluate the cost of migrating

------
### Table Structure Migrate
The struct migrate task config [example](../example/struct_migrate_task.toml), current support oracle migrate mysql compatible database, provides the following functional implementations:
- the schema、table and column name Case Sensitive
- the schema table define、index、constraint migrate
- custom configuration route rule mapping
  - schema name route
  - table name route
  - column name route
- custom configuration migrate rule mapping, migrate rule priority：column -> table -> schema -> task -> builtin
  - the column datatype custom
  - the column default custom
  - the table attribute custom (only tidb database)
  - the column default value builtin sysdate -> now() and sys_guid -> uuid rules, and provides the [buildin_datatype_rule] and [buildin_defaultval_rule] metadata tables with the ability to write more rules
- the table structure direct create to the downstream database
- the oracle migrate mysql compatible database [builtin column datatype mapping rule](../doc/buildin_rule_reverse_o.md)
- the schema table sequences migrate(need the downstream database support sequence)
 
<mark>NOTE:</mark>
- convert partition tables, temporary tables, and clustered tables into normal tables. If necessary, please obtain complete incompatible object information and then convert manually.
- the Materialized Views are not converted, and the Materialized Views output incompatible
object
- the unique constraints are based on unique index fields, and only unique indexes will be created downstream
- the oracle FUNCTION-BASED NORMAL、BITMAP incompatible index output incompatible object
- the schema table charset and collation
  - if the downstream database is tidb, ignore the oracle database character set, unify UTF8MB4 and UTF8MB4_BIN conversion
  - if the downstream database is mysql, it will be converted according to the oracle database character set and collation mapping rules (for example: ZHS16GBK -> GBK)
- the schema table column function default values are not converted, keep the upstream default value, whether an error is reported when creating a table downstream depends on whether the downstream database supports the current function default value
- If an error is reported, the process will not be terminated, and a warning message will be output at the end of the log. For the specific error table and corresponding error details, see the metadata table [struct_migrate_task] data

------

### Table Structure Compare
The Database table structure comparison is based on the upstream database, provides the following functional implementations:

