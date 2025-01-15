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
)

func (d *Database) GetDatabaseSchemaPrimaryTables(schemaName string) ([]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT 
    cons.conname AS "CONSTRAINT_NAME",
	tbl.relname AS "TABLE_NAME",
    string_agg(a.attname, '|+|') AS "COLUMN_LIST"
FROM 
    pg_constraint cons
JOIN 
    pg_class tbl ON cons.conrelid = tbl.oid
JOIN 
    pg_namespace ns ON tbl.relnamespace = ns.oid
JOIN 
    pg_attribute a ON a.attrelid = cons.conrelid AND a.attnum = ANY(cons.conkey)
WHERE 
    ns.nspname = '%s' AND contype = 'p'
GROUP BY 
    cons.conname,tbl.relname`, schemaName))
	if err != nil {
		return nil, err
	}
	var tables []string
	for _, r := range res {
		tables = append(tables, r["TABLE_NAME"])
	}
	return tables, nil
}

func (d *Database) GetDatabaseSchemaTableValidIndex(schemaName string, tableName string) (bool, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`WITH unique_constraints AS (
    SELECT 
        cons.conname AS constraint_or_index_name,
        tbl.relname AS table_name,
        a.attname AS column_name,
        CASE WHEN a.attnotnull THEN 'YES' ELSE 'NO' END AS is_not_null
    FROM 
        pg_constraint cons
    JOIN 
        pg_class tbl ON cons.conrelid = tbl.oid
    JOIN 
        pg_namespace ns ON tbl.relnamespace = ns.oid
    JOIN 
        pg_attribute a ON a.attrelid = cons.conrelid AND a.attnum = ANY(cons.conkey)
    WHERE 
        ns.nspname = '%s'  -- 替换为您的模式名称
        AND cons.contype IN ('u')
        AND tbl.relname = '%s'  -- 替换为您的表名
),
unique_indexes AS (
    SELECT 
        idx.indexrelid::regclass AS constraint_or_index_name,
        tbl.relname AS table_name,
        a.attname AS column_name,
        CASE WHEN a.attnotnull THEN 'YES' ELSE 'NO' END AS is_not_null
    FROM 
        pg_index idx
    JOIN 
        pg_class tbl ON idx.indrelid = tbl.oid
    JOIN 
        pg_namespace ns ON tbl.relnamespace = ns.oid
    JOIN 
        pg_attribute a ON a.attrelid = idx.indrelid AND a.attnum = ANY(idx.indkey)
    WHERE 
        ns.nspname = '%s'
        AND idx.indisunique 
        AND tbl.relname = '%s'
)
SELECT * FROM unique_constraints
UNION ALL
SELECT * FROM unique_indexes
ORDER BY table_name, column_name`, schemaName, tableName, schemaName, tableName))
	if err != nil {
		return false, err
	}
	indexNameColumns := make(map[string][]string)
	indexNameNulls := make(map[string]int)

	for _, r := range res {
		if vals, ok := indexNameColumns[r["constraint_or_index_name"]]; ok {
			indexNameColumns[r["constraint_or_index_name"]] = append(vals, r["column_name"])
		} else {
			indexNameColumns[r["constraint_or_index_name"]] = append(indexNameColumns[r["constraint_or_index_name"]], r["column_name"])
		}

		if r["is_not_null"] == "YES" {
			if vals, ok := indexNameNulls[r["constraint_or_index_name"]]; ok {
				indexNameNulls[r["constraint_or_index_name"]] = vals + 1
			} else {
				indexNameNulls[r["constraint_or_index_name"]] = 1
			}
		}
	}

	for k, v := range indexNameColumns {
		if val, ok := indexNameNulls[k]; ok && val == len(v) {
			return true, nil
		}
	}
	return false, nil
}

func (d *Database) GetDatabaseTableColumnMetadata(schemaName string, tableName string) ([]map[string]string, error) {
	version, err := d.GetDatabaseVersion()
	if err != nil {
		return nil, err
	}
	virtualC := false
	if stringutil.VersionOrdinal(version) >= stringutil.VersionOrdinal(constant.PostgresqlDatabaseVirtualGeneratedColumnSupportVersionRequire) {
		virtualC = true
	}

	var sqlStr string
	if virtualC {
		sqlStr = fmt.Sprintf(`SELECT
		column_name AS "COLUMN_NAME",
		data_type AS "DATA_TYPE",
		COALESCE(character_maximum_length,0) AS "DATA_LENGTH",
		CASE 
			WHEN COALESCE(is_generated, '') = '' THEN 'NO'
			WHEN is_generated = 'NEVER' THEN 'NO'
			ELSE 'YES'
		END AS "IS_GENERATED"
	FROM
	information_schema.columns
	WHERE
		table_schema = '%s'
		AND table_name = '%s'
		ORDER BY ordinal_position`, schemaName, tableName)
	} else {
		sqlStr = fmt.Sprintf(`SELECT
		column_name AS "COLUMN_NAME",
		data_type AS "DATA_TYPE",
		COALESCE(character_maximum_length,0) AS "DATA_LENGTH",
		'NO' AS "IS_GENERATED"
	FROM
	information_schema.columns
	WHERE
		table_schema = '%s'
		AND table_name = '%s'
		ORDER BY ordinal_position`, schemaName, tableName)
	}
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return res, err
	}
	return res, nil
}
