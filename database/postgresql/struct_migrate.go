/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package postgresql

import (
	"fmt"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"strings"
)

func (d *Database) GetDatabaseSchema() ([]string, error) {
	var (
		schemas []string
		err     error
	)
	columns, res, err := d.GeneralQuery(`SELECT
	nspname
FROM
	pg_catalog.pg_namespace
WHERE
	nspname not like 'pg_%'
	and nspname != 'information_schema'`)
	if err != nil {
		return schemas, err
	}
	for _, col := range columns {
		for _, r := range res {
			schemas = append(schemas, r[col])
		}
	}
	return schemas, nil
}

func (d *Database) GetDatabaseTable(schemaName string) ([]string, error) {
	var (
		tables []string
		err    error
	)
	columns, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT
	tablename
FROM
	pg_catalog.pg_tables
WHERE
	schemaname = '%s'`, schemaName))
	if err != nil {
		return tables, err
	}
	for _, col := range columns {
		for _, r := range res {
			tables = append(tables, r[col])
		}
	}
	return tables, nil
}

func (d *Database) GetDatabaseCharset() (string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT
	pg_encoding_to_char(encoding) as charset
FROM
	pg_catalog.pg_database
WHERE
	datname = current_database()`))
	if err != nil {
		return "", err
	}
	return res[0]["charset"], nil
}

func (d *Database) GetDatabaseCollation() (string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT
	datcollate
FROM
	pg_catalog.pg_database
WHERE
	datname = current_database()`))
	if err != nil {
		return "", err
	}
	return res[0]["datcollate"], nil
}

func (d *Database) GetDatabaseVersion() (string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`show server_version`))
	if err != nil {
		return "", err
	}
	return res[0]["server_version"], nil
}

func (d *Database) GetDatabasePartitionTable(schemaName string) ([]string, error) {
	version, err := d.GetDatabaseVersion()
	if err != nil {
		return nil, err
	}
	if stringutil.VersionOrdinal(strings.Fields(version)[0]) < stringutil.VersionOrdinal(constant.PostgresqlDatabasePartitionTableSupportVersionRequire) {
		return nil, nil
	} else {
		var tables []string
		_, res, err := d.GeneralQuery(fmt.Sprintf(`WITH RECURSIVE inheritance_tree AS (
    SELECT 
        inhparent::regclass AS parent, 
        inhrelid::regclass AS child
    FROM 
        pg_catalog.pg_inherits
    UNION ALL
    SELECT 
        it.parent, 
        c.inhrelid::regclass
    FROM 
        pg_catalog.pg_inherits c
    JOIN 
        inheritance_tree it ON c.inhparent = it.child
)
, top_level_parents AS (
    SELECT 
        DISTINCT parent 
    FROM 
        inheritance_tree
    WHERE 
        NOT EXISTS (
            SELECT 
                1
            FROM 
                inheritance_tree it_inner
            WHERE 
                it_inner.child = inheritance_tree.parent
        )
)
SELECT 
    split_part(top_level_parents.parent::text, '.', 2) AS table_name
FROM 
    top_level_parents
JOIN 
    pg_catalog.pg_class c ON top_level_parents.parent = c.oid
JOIN 
    pg_catalog.pg_namespace nsp ON c.relnamespace = nsp.oid
WHERE 
    nsp.nspname = '%s'
ORDER BY 
   table_name`, schemaName))
		if err != nil {
			return tables, err
		}
		for _, r := range res {
			tables = append(tables, r["table_name"])
		}
		return tables, nil
	}

}

func (d *Database) GetDatabaseTemporaryTable(schemaName string) ([]string, error) {
	// postgresql database temporary tables are divided into session temporary tables and transaction temporary tables.
	// The table definition disappears with the suspension of the session or the termination of the transaction, so the definition of the temporary table is skipped.
	return nil, nil
}

func (d *Database) GetDatabaseClusteredTable(schemaName string) ([]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT 
	    cls.relname AS table_name
	FROM 
	    pg_catalog.pg_class cls
	JOIN 
	    pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
	JOIN 
	   pg_catalog.pg_index idx ON cls.oid = idx.indrelid
	WHERE 
	    nsp.nspname = '%s'
	    AND cls.relkind = 'r'
	    AND idx.indisclustered`, schemaName))
	if err != nil {
		return nil, err
	}
	var tables []string
	for _, r := range res {
		tables = append(tables, r["table_name"])
	}
	return tables, nil
}

func (d *Database) GetDatabaseMaterializedView(schemaName string) ([]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT 
    cls.relname AS table_name
FROM 
    pg_catalog.pg_class cls
JOIN 
    pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
WHERE 
    nsp.nspname = '%s'
    AND cls.relkind = 'm'`, schemaName))
	if err != nil {
		return nil, err
	}
	var tables []string
	for _, r := range res {
		tables = append(tables, r["table_name"])
	}
	return tables, nil
}

func (d *Database) GetDatabaseNormalView(schemaName string) ([]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT 
    cls.relname AS table_name
FROM 
    pg_catalog.pg_class cls
JOIN 
    pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
WHERE 
    nsp.nspname = '%s'
    AND cls.relkind = 'v'`, schemaName))
	if err != nil {
		return nil, err
	}
	var tables []string
	for _, r := range res {
		tables = append(tables, r["table_name"])
	}
	return tables, nil
}

func (d *Database) GetDatabaseCompositeTypeTable(schemaName string) ([]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT 
    cls.relname AS table_name
FROM 
    pg_catalog.pg_class cls
JOIN 
    pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
WHERE 
    nsp.nspname = '%s'
    AND cls.relkind = 'c'`, schemaName))
	if err != nil {
		return nil, err
	}
	var tables []string
	for _, r := range res {
		tables = append(tables, r["table_name"])
	}
	return tables, nil
}

func (d *Database) GetDatabaseExternalTable(schemaName string) ([]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT 
    cls.relname AS table_name
FROM 
    pg_catalog.pg_class cls
JOIN 
    pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
WHERE 
    nsp.nspname = '%s'
    AND cls.relkind = 'f'`, schemaName))
	if err != nil {
		return nil, err
	}
	var tables []string
	for _, r := range res {
		tables = append(tables, r["table_name"])
	}
	return tables, nil
}

func (d *Database) GetDatabaseTableType(schemaName string) (map[string]string, error) {
	tableTypeMap := make(map[string]string)

	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT 
    cls.relname AS table_name,
    CASE
	WHEN cls.relkind = 'r' THEN 'HEAP'
    WHEN cls.relkind = 'v' THEN 'VIEW'
    WHEN cls.relkink = 'm' THEN 'MATERIALIZED VIEW'
    WHEN cls.relkink = 'c' THEN 'USER DEFINE TYPE TABLE'
    WHEN cls.relkink = 'f' THEN 'EXTERNAL TABLE'
 	ELSE
 		'UNKNOWN'
 	END AS table_type
FROM 
    pg_catalog.pg_class cls
JOIN 
    pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
WHERE 
    nsp.nspname = '%s'
    and cls.relkink not in ('i','s','t') -- exclude index、sequence、TOAST`, schemaName))
	if err != nil {
		return nil, err
	}

	for _, r := range res {
		if len(r) > 2 || len(r) == 0 || len(r) == 1 {
			return tableTypeMap, fmt.Errorf("postgresql schema [%s] table type values should be 2, result: %v", schemaName, r)
		}
		tableTypeMap[r["TABLE_NAME"]] = r["TABLE_TYPE"]
	}

	tables, err := d.GetDatabasePartitionTable(schemaName)
	if err != nil {
		return nil, err
	}
	for _, t := range tables {
		if _, ok := tableTypeMap[t]; ok {
			tableTypeMap[t] = "PARTITIONED"
		}
	}

	tables, err = d.GetDatabaseClusteredTable(schemaName)
	if err != nil {
		return nil, err
	}
	for _, t := range tables {
		if _, ok := tableTypeMap[t]; ok {
			tableTypeMap[t] = "CLUSTERED"
		}
	}

	tables, err = d.GetDatabaseTemporaryTable(schemaName)
	if err != nil {
		return nil, err
	}
	for _, t := range tables {
		if _, ok := tableTypeMap[t]; ok {
			tableTypeMap[t] = "TEMPORARY"
		}
	}

	return tableTypeMap, nil
}

func (d *Database) GetDatabaseTableColumnInfo(schemaName string, tableName string, collation bool) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTablePrimaryKey(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableUniqueKey(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableForeignKey(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableCheckKey(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableNormalIndex(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableUniqueIndex(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableComment(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableColumnComment(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableCharset(schemaName string, tableName string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableCollation(schemaName, tableName string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSchemaCollation(schemaName string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableOriginStruct(schemaName, tableName, tableType string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSequence(schemaName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseRole() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseConsistentPos() (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableColumnNameTableDimensions(schemaName, tableName string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableColumnNameSqlDimensions(sqlStr string) ([]string, map[string]string, map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableRows(schemaName, tableName string) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableSize(schemaName, tableName string) (float64, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableChunkTask(taskName, schemaName, tableName string, chunkSize uint64, callTimeout uint64) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableChunkData(querySQL string, batchSize, callTimeout int, dbCharsetS, dbCharsetT, columnDetailO string, dataChan chan []interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableCsvData(querySQL string, callTimeout int, taskFlow, dbCharsetS, dbCharsetT, columnDetailO string, escapeBackslash bool, nullValue, separator, delimiter string, dataChan chan []string) error {
	//TODO implement me
	panic("implement me")
}

func (d *Database) FindDatabaseTableBestColumn(schemaNameS, tableNameS, columnNameS string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableColumnAttribute(schemaNameS, tableNameS, columnNameS string, collationS bool) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableColumnBucket(schemaNameS, tableNameS string, columnNameS, datatypeS string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableCompareData(querySQL string, callTimeout int, dbCharsetS, dbCharsetT string) ([]string, uint32, map[string]int64, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTablePartitionExpress(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}
