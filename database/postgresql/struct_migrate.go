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
	"strconv"
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
	pg_namespace
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
	pg_tables
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
	pg_database
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
	pg_database
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
        pg_inherits
    UNION ALL
    SELECT 
        it.parent, 
        c.inhrelid::regclass
    FROM 
        pg_inherits c
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
    pg_class c ON top_level_parents.parent = c.oid
JOIN 
    pg_namespace nsp ON c.relnamespace = nsp.oid
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
	    pg_class cls
	JOIN 
	    pg_namespace nsp ON cls.relnamespace = nsp.oid
	JOIN 
	   pg_index idx ON cls.oid = idx.indrelid
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
    pg_class cls
JOIN 
    pg_namespace nsp ON cls.relnamespace = nsp.oid
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
    pg_class cls
JOIN 
    pg_namespace nsp ON cls.relnamespace = nsp.oid
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
    pg_class cls
JOIN 
    pg_namespace nsp ON cls.relnamespace = nsp.oid
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
    pg_class cls
JOIN 
    pg_namespace nsp ON cls.relnamespace = nsp.oid
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
    pg_class cls
JOIN 
    pg_namespace nsp ON cls.relnamespace = nsp.oid
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

func (d *Database) GetDatabaseTableColumnInfo(schemaName string, tableName string) ([]map[string]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT
	col.table_schema,
	col.table_name,
	col.column_name,
	col.data_type,
	col.character_maximum_length AS char_length,
	col.numeric_precision AS data_precision,
	col.numeric_scale AS data_scale,
	col.datetime_precision,
	CASE
		col.is_nullable WHEN 'YES' THEN 'Y'
		WHEN 'NO' THEN 'N'
		ELSE 'UNKNOWN'
	END AS nullable,
	col.column_default AS data_default,
	col.character_set_name AS charset,
	col.collation_name AS collation,
	temp.column_comment AS comment
FROM
	information_schema.columns col
JOIN (
	SELECT
		a.attname AS column_name,
		d.description AS column_comment
	FROM
		pg_attribute a
	LEFT JOIN pg_description d ON
		a.attrelid = d.classoid
	WHERE
		a.attrelid = (
		SELECT
			c.oid
		FROM
			pg_class c
		JOIN pg_namespace n ON
			n.oid = c.relnamespace
		WHERE
			n.nspname = '%s'
			AND c.relname = '%s'
    )
		AND a.attnum > 0
		AND not a.attisdropped
) temp ON
	col.column_name = temp.column_name
WHERE
	col.table_schema = '%s'
	AND col.table_name = '%s'
ORDER BY
	col.ordinal_position`, schemaName, tableName, schemaName, tableName))
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTablePrimaryKey(schemaName string, tableName string) ([]map[string]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT 
    conname AS constraint_name,
    string_agg(a.attname, '|+|') AS column_list
FROM 
    pg_constraint cons
JOIN 
    pg_class tbl ON cons.conrelid = tbl.oid
JOIN 
    pg_namespace ns ON tbl.relnamespace = ns.oid
JOIN 
    pg_attribute a ON a.attrelid = cons.conrelid AND a.attnum = ANY(cons.conkey)
WHERE 
    ns.nspname = '%s' AND 
    tbl.relname = '%s' AND 
    contype = 'p'
GROUP BY 
    conname`, schemaName, tableName))
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableUniqueKey(schemaName string, tableName string) ([]map[string]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT 
    conname AS constraint_name,
    string_agg(a.attname, '|+|') AS column_list
FROM 
    pg_constraint cons
JOIN 
    pg_class tbl ON cons.conrelid = tbl.oid
JOIN 
    pg_namespace ns ON tbl.relnamespace = ns.oid
JOIN 
    pg_attribute a ON a.attrelid = cons.conrelid AND a.attnum = ANY(cons.conkey)
WHERE 
    ns.nspname = '%s' AND 
    tbl.relname = '%s' AND 
    contype = 'u'
GROUP BY 
    conname`, schemaName, tableName))
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableForeignKey(schemaName string, tableName string) ([]map[string]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT 
    conname AS constraint_name,
    string_agg(a.attname, '|+|') AS column_list,
    string_agg(b.attname, '|+|') AS rcolumn_list,
    split_part(confrelid::regclass::text, '.', 1) AS rowner,
    split_part(confrelid::regclass::text, '.', 2) AS rtable_name,
    case confupdtype when 'a' then 'NO ACTION' when 'r' then 'RESTRICT' when 'c' then 'CASCADE' when 'n' then 'SET NULL' when 'd' then 'SET DEFAULT' else 'UNKNOWN' end AS update_rule,
    case confdeltype when 'a' then 'NO ACTION' when 'r' then 'RESTRICT' when 'c' then 'CASCADE' when 'n' then 'SET NULL' when 'd' then 'SET DEFAULT' else 'UNKNOWN' end AS delete_rule
FROM 
    pg_constraint cons
JOIN 
    pg_class tbl ON cons.conrelid = tbl.oid
JOIN 
    pg_namespace ns ON tbl.relnamespace = ns.oid
JOIN 
    pg_attribute a ON a.attrelid = cons.conrelid AND a.attnum = ANY(cons.conkey)
JOIN 
    pg_attribute b ON b.attrelid = cons.confrelid AND b.attnum = ANY(cons.confkey)
WHERE 
    ns.nspname = '%s' AND 
    tbl.relname = '%s' AND 
    cons.contype = 'f'
GROUP BY 
    cons.conname, confrelid, confupdtype, confdeltype`, schemaName, tableName))
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableCheckKey(schemaName string, tableName string) ([]map[string]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT 
    conname AS constraint_name,
    pg_get_constraintdef(cons.oid) AS definition,
    string_agg(att.attname, '|+|') AS involved_columns
FROM 
    pg_constraint cons
JOIN 
    pg_class tbl ON cons.conrelid = tbl.oid
JOIN 
    pg_namespace ns ON tbl.relnamespace = ns.oid
LEFT JOIN 
    pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = ANY(cons.conkey)
WHERE 
    ns.nspname = '%s' AND 
    tbl.relname = '%s' AND 
    contype = 'c'
GROUP BY 
    cons.oid, conname`, schemaName, tableName))
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableNormalIndex(schemaName string, tableName string) ([]map[string]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`WITH index_details AS (
    SELECT 
        idx.indrelid AS table_oid,
        idx.indexrelid AS index_oid,
        unnest(idx.indkey) AS attnum,
        CASE WHEN idx.indisunique IS TRUE THEN 'UNIQUE' ELSE 'NONUNIQUE' END AS indisunique,
        idx.indexprs
    FROM 
        pg_index idx
    JOIN 
        pg_class tbl ON tbl.oid = idx.indrelid
    JOIN 
        pg_namespace n ON n.oid = tbl.relnamespace
    WHERE 
        n.nspname = '%s' AND
        tbl.relname = '%s' AND
        NOT idx.indisprimary AND 
        NOT idx.indisunique AND
        NOT EXISTS (
            SELECT 1 
            FROM pg_constraint cons 
            WHERE cons.conrelid = tbl.oid AND cons.conindid = idx.indexrelid AND cons.contype = 'u'
        ) AND
        idx.indisvalid
),
-- regular index
regular_columns AS (
    SELECT 
        idxd.table_oid,
        idxd.index_oid,
        idxd.indisunique,
    	string_agg(quote_ident(att.attname), '|+|') AS column_lists
    FROM 
        index_details idxd
    JOIN 
        pg_attribute att ON att.attrelid = idxd.table_oid AND att.attnum = idxd.attnum AND NOT att.attisdropped
    WHERE 
        idxd.indexprs IS NULL
    GROUP BY 
    	idxd.table_oid,
        idxd.index_oid,
        idxd.indisunique
),
-- expr index
expr_columns AS (
    SELECT 
        idxd.table_oid,
        idxd.index_oid,
        idxd.indisunique,
        replace(pg_get_expr(idxd.indexprs, idxd.table_oid),',','|+|') AS column_lists
    FROM 
        index_details idxd
    WHERE 
        idxd.indexprs IS NOT null
    GROUP BY 
    	idxd.table_oid,
        idxd.index_oid,
        idxd.indisunique,
        column_lists
),
-- merge
all_columns AS (
    SELECT * FROM regular_columns
    UNION
    SELECT * FROM expr_columns
)
SELECT 
    nmsp_table.nspname AS table_schema,
    tbl.relname AS table_name,
    nmsp_index.nspname AS index_schema,
    idx_class.relname AS index_name,
    idxd.column_lists,
    idxd.indisunique AS is_unique,
    am.amname AS index_type
FROM 
    all_columns idxd
JOIN 
    pg_class idx_class ON idx_class.oid = idxd.index_oid
JOIN  
    pg_namespace nmsp_index ON idx_class.relnamespace = nmsp_index.oid
JOIN  
    pg_class tbl ON tbl.oid = idxd.table_oid
JOIN  
    pg_namespace nmsp_table ON tbl.relnamespace = nmsp_table.oid
JOIN  
    pg_am am ON am.oid = idx_class.relam`, schemaName, tableName))
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableUniqueIndex(schemaName string, tableName string) ([]map[string]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`WITH index_details AS (
    SELECT 
        idx.indrelid AS table_oid,
        idx.indexrelid AS index_oid,
        unnest(idx.indkey) AS attnum,
        CASE WHEN idx.indisunique IS TRUE THEN 'UNIQUE' ELSE 'NONUNIQUE' END AS indisunique,
        idx.indexprs
    FROM 
        pg_index idx
    JOIN 
        pg_class tbl ON tbl.oid = idx.indrelid
    JOIN 
        pg_namespace n ON n.oid = tbl.relnamespace
    WHERE 
        n.nspname = '%s' AND
        tbl.relname = '%s' AND
        NOT idx.indisprimary AND 
        idx.indisunique AND
        NOT EXISTS (
            SELECT 1 
            FROM pg_constraint cons 
            WHERE cons.conrelid = tbl.oid AND cons.conindid = idx.indexrelid AND cons.contype = 'u'
        ) AND
        idx.indisvalid
),
-- regular index
regular_columns AS (
    SELECT 
        idxd.table_oid,
        idxd.index_oid,
        idxd.indisunique,
    	string_agg(quote_ident(att.attname), '|+|') AS column_lists
    FROM 
        index_details idxd
    JOIN 
        pg_attribute att ON att.attrelid = idxd.table_oid AND att.attnum = idxd.attnum AND NOT att.attisdropped
    WHERE 
        idxd.indexprs IS NULL
    GROUP BY 
    	idxd.table_oid,
        idxd.index_oid,
        idxd.indisunique
),
-- expr index
expr_columns AS (
    SELECT 
        idxd.table_oid,
        idxd.index_oid,
        idxd.indisunique,
        replace(pg_get_expr(idxd.indexprs, idxd.table_oid),',','|+|') AS column_lists
    FROM 
        index_details idxd
    WHERE 
        idxd.indexprs IS NOT null
    GROUP BY 
    	idxd.table_oid,
        idxd.index_oid,
        idxd.indisunique,
        column_lists
),
-- merge
all_columns AS (
    SELECT * FROM regular_columns
    UNION
    SELECT * FROM expr_columns
)
SELECT 
    nmsp_table.nspname AS table_schema,
    tbl.relname AS table_name,
    nmsp_index.nspname AS index_schema,
    idx_class.relname AS index_name,
    idxd.column_lists,
    idxd.indisunique AS is_unique,
    am.amname AS index_type
FROM 
    all_columns idxd
JOIN 
    pg_class idx_class ON idx_class.oid = idxd.index_oid
JOIN  
    pg_namespace nmsp_index ON idx_class.relnamespace = nmsp_index.oid
JOIN  
    pg_class tbl ON tbl.oid = idxd.table_oid
JOIN  
    pg_namespace nmsp_table ON tbl.relnamespace = nmsp_table.oid
JOIN  
    pg_am am ON am.oid = idx_class.relam`, schemaName, tableName))
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableComment(schemaName string, tableName string) ([]map[string]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT 
    c.relname AS table_name,
    d.description AS comments
FROM 
    pg_description d
JOIN 
    pg_class c ON c.oid = d.objoid
JOIN 
    pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN 
    pg_attribute a ON a.attrelid = d.objoid AND a.attnum = d.objsubid
WHERE 
    n.nspname = '%s' AND
    c.relname = '%s' AND
    d.classoid = 'pg_class'::regclass AND
    d.objsubid = 0`, schemaName, tableName))
	if err != nil {
		return res, fmt.Errorf("get database table comment failed: %v", err)
	}
	if len(res) > 1 {
		return res, fmt.Errorf("get database schema [%s] table [%s] comments exist multiple values: [%v]", schemaName, tableName, res)
	}
	return res, nil
}

func (d *Database) GetDatabaseTableColumnComment(schemaName string, tableName string) ([]map[string]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT 
    a.attname AS column_name,
    d.description AS column_description
FROM 
    pg_attribute a
JOIN 
    pg_class c ON c.oid = a.attrelid
JOIN 
    pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN 
    pg_description d ON d.objoid = a.attrelid AND d.objsubid = a.attnum AND d.classoid = 'pg_class'::regclass
WHERE 
    n.nspname = '%s' AND 
    c.relname = '%s' AND 
    a.attnum > 0 AND NOT a.attisdropped
ORDER BY 
    a.attnum;`, schemaName, tableName))
	if err != nil {
		return res, fmt.Errorf("get database table column comment failed: %v", err)
	}
	if len(res) == 0 {
		return res, fmt.Errorf("database table [%s.%s] column comments cann't be null", schemaName, tableName)
	}
	return res, nil
}

func (d *Database) GetDatabaseTableCharset(schemaName string, tableName string) (string, error) {
	// Setting the table-level character set and collation rules is not supported in the pg database. Instead, the table-level character set automatically integrates the database-level character set and collation rules. However, the table field level supports setting the character set and collation rules.
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT
	pg_encoding_to_char(encoding) as charset
FROM
	pg_database
WHERE
	datname = current_database()`))
	if err != nil {
		return "", fmt.Errorf("get database table charset failed: %v", err)
	}
	return res[0]["charset"], nil
}

func (d *Database) GetDatabaseTableCollation(schemaName, tableName string) (string, error) {
	// Setting the table-level character set and collation rules is not supported in the pg database. Instead, the table-level character set automatically integrates the database-level character set and collation rules. However, the table field level supports setting the character set and collation rules.
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT
	datcollate
	FROM
	pg_database
	WHERE
	datname = current_database()`))
	if err != nil {
		return "", fmt.Errorf("get database table collation failed: %v", err)
	}
	return res[0]["datcollate"], nil
}

func (d *Database) GetDatabaseSchemaCollation(schemaName string) (string, error) {
	// Setting the schema-level character set and collation rules is not supported in the pg database. Instead, the schema-level character set automatically integrates the database-level character set and collation rules. However, the table field level supports setting the character set and collation rules.
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT
	datcollate
	FROM
	pg_database
	WHERE
	datname = current_database()`))
	if err != nil {
		return "", fmt.Errorf("get database schema collation failed: %v", err)
	}
	return res[0]["datcollate"], nil
}

func (d *Database) GetDatabaseTableOriginStruct(schemaName, tableName, tableType string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseSequence(schemaName string) ([]map[string]string, error) {
	version, err := d.GetDatabaseVersion()
	if err != nil {
		return nil, err
	}
	var queryStr string
	if stringutil.VersionOrdinal(version) > stringutil.VersionOrdinal("10") {
		queryStr = fmt.Sprintf(`SELECT
	sequenceowner AS sequence_owner,
	sequencename AS sequence_name,
	min_value,
	max_value,
	increment_by,
	cycle AS cycle_flag
	cache_size,
	last_value
FROM pg_sequences
WHERE
    sequenceowner = '%s'`, schemaName)
		_, res, err := d.GeneralQuery(queryStr)
		if err != nil {
			return res, err
		}
		return res, nil
	} else {
		queryStr = fmt.Sprintf(`SELECT
	sequence_schema AS sequence_schema,
	sequence_name AS sequence_name,
	minimum_value AS min_value,
	maximum_value AS max_value,
	increment AS increment_by,
	cycle_option AS cycle_flag
FROM information_schema.sequences
WHERE
    sequence_schema = '%s'`, schemaName)
		_, res, err := d.GeneralQuery(queryStr)
		if err != nil {
			return res, err
		}

		for _, r := range res {
			_, seqs, err := d.GeneralQuery(fmt.Sprintf(`SELECT last_value,cache_value FROM %s.%s`, r["sequence_schema"], r["sequence_name"]))
			if err != nil {
				return nil, err
			}
			r["last_value"] = seqs[0]["cache_value"]
			r["cache_size"] = seqs[0]["cache_value"]
		}
		return res, nil
	}
}

func (d *Database) GetDatabaseRole() (string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT pg_is_in_recovery() AS role`))
	if err != nil {
		return "", fmt.Errorf("get database role failed: %v", err)
	}
	return res[0]["role"], nil
}

func (d *Database) GetDatabaseConsistentPos() (uint64, error) {
	version, err := d.GetDatabaseVersion()
	if err != nil {
		return 0, err
	}
	var queryStr string
	if stringutil.VersionOrdinal(version) > stringutil.VersionOrdinal("10") {
		queryStr = fmt.Sprintf(`SELECT pg_current_wal_lsn() AS scn`)
	} else {
		queryStr = fmt.Sprintf(`SELECT pg_current_xlog_location() AS scn`)
	}

	_, res, err := d.GeneralQuery(queryStr)
	if err != nil {
		return 0, err
	}

	size, err := stringutil.StrconvUintBitSize(res[0]["scn"], 64)
	if err != nil {
		return 0, err
	}
	return size, nil
}

func (d *Database) GetDatabaseTableColumnNameTableDimensions(schemaName, tableName string) ([]string, error) {
	rows, err := d.QueryContext(d.Ctx, fmt.Sprintf(`SELECT * FROM "%s"."%s" LIMIT 1`, schemaName, tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return columns, err
	}
	return columns, nil
}

func (d *Database) GetDatabaseTableColumnNameSqlDimensions(sqlStr string) ([]string, map[string]string, map[string]string, error) {
	rows, err := d.QueryContext(d.Ctx, fmt.Sprintf(`SELECT * FROM (%v) t LIMIT 1`, sqlStr))
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	var columns []string
	columnTypeMap := make(map[string]string)
	columnScaleMap := make(map[string]string)

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return columns, columnTypeMap, columnScaleMap, err
	}

	for _, c := range columnTypes {
		columns = append(columns, c.Name())
		columnTypeMap[c.Name()] = c.DatabaseTypeName()
		_, dataScale, ok := c.DecimalSize()
		if ok {
			columnScaleMap[c.Name()] = strconv.FormatInt(dataScale, 10)
		}
	}
	return columns, columnTypeMap, columnScaleMap, nil
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

func (d *Database) GetDatabaseTableChunkData(querySQL string, queryArgs []interface{}, batchSize, callTimeout int, dbCharsetS, dbCharsetT, columnDetailO string, dataChan chan []interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableCsvData(querySQL string, queryArgs []interface{}, callTimeout int, taskFlow, dbCharsetS, dbCharsetT, columnDetailO string, escapeBackslash bool, nullValue, separator, delimiter string, dataChan chan []string) error {
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

func (d *Database) GetDatabaseTablePartitionExpress(schemaName string, tableName string) ([]map[string]string, error) {
	//TODO implement me
	panic("implement me")
}
