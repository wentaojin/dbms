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
package mysql

import (
	"fmt"
	"strings"

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/utils/constant"
)

func (d *Database) GetDatabaseSchema() ([]string, error) {
	sqlStr := fmt.Sprintf(`SELECT
	DISTINCT(SCHEMA_NAME) AS SCHEMA_NAME
FROM
	INFORMATION_SCHEMA.SCHEMATA`)
	var schemas []string
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return nil, err
	}
	for _, s := range res {
		schemas = append(schemas, s["SCHEMA_NAME"])
	}
	return schemas, nil
}

func (d *Database) GetDatabaseTable(schemaName string) ([]string, error) {
	sqlStr := fmt.Sprintf(`SELECT
	TABLE_NAME
FROM
	INFORMATION_SCHEMA.TABLES
where
	TABLE_SCHEMA = '%s'
	AND TABLE_TYPE = 'BASE TABLE'`, schemaName)
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return nil, err
	}
	var tables []string
	if len(res) > 0 {
		for _, r := range res {
			tables = append(tables, r["TABLE_NAME"])
		}
	}
	return tables, nil
}

func (d *Database) GetDatabaseSequence(schemaName string) ([]map[string]string, error) {
	_, res, err := d.GeneralQuery(`SELECT VERSION() AS VERSION`)
	if err != nil {
		return nil, err
	}

	var sqlStr string
	if strings.Contains(res[0]["VERSION"], constant.DatabaseTypeTiDB) {
		var version string
		if strings.Contains(res[0]["VERSION"], constant.MYSQLCompatibleDatabaseVersionDelimiter) {
			version = strings.Split(res[0]["VERSION"], constant.MYSQLCompatibleDatabaseVersionDelimiter)[0]
		} else {
			version = res[0]["VERSION"]
		}

		if stringutil.VersionOrdinal(version) >= stringutil.VersionOrdinal(constant.TIDBDatabaseSequenceSupportVersion) {
			sqlStr = fmt.Sprintf(`SELECT
	SEQUENCE_SCHEMA AS SEQUENCE_OWNER,
	SEQUENCE_NAME,
	MIN_VALUE,
	MAX_VALUE,
	INCREMENT AS INCREMENT_BY,
	CYCLE AS CYCLE_FLAG,
	CACHE_VALUE AS CACHE_SIZE,
	START AS LAST_NUMBER
FROM INFORMATION_SCHEMA.SEQUENCES
WHERE
    TABLE_CATALOG = '%s'`, schemaName)
		}
	} else {
		var version string
		if strings.Contains(res[0]["VERSION"], constant.MYSQLCompatibleDatabaseVersionDelimiter) {
			version = strings.Split(res[0]["VERSION"], constant.MYSQLCompatibleDatabaseVersionDelimiter)[0]
		} else {
			version = res[0]["VERSION"]
		}

		if stringutil.VersionOrdinal(version) >= stringutil.VersionOrdinal(constant.MYSQLDatabaseSequenceSupportVersion) {
			sqlStr = fmt.Sprintf(`SELECT
	SEQUENCE_SCHEMA AS SEQUENCE_OWNER,
	SEQUENCE_NAME,
	MIN_VALUE,
	MAX_VALUE,
	INCREMENT AS INCREMENT_BY,
	CYCLE AS CYCLE_FLAG,
	CACHE_VALUE AS CACHE_SIZE,
	START AS LAST_NUMBER
FROM INFORMATION_SCHEMA.SEQUENCES
WHERE
    TABLE_CATALOG = '%s'`, schemaName)
		}
	}

	if strings.EqualFold(sqlStr, "") {
		return nil, nil
	}

	_, res, err = d.GeneralQuery(sqlStr)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (d *Database) GetDatabaseCharset() (string, error) {
	sqlStr := fmt.Sprintf(`SHOW VARIABLES LIKE 'character_set_server'`)
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return "", err
	}
	return res[0]["VALUE"], nil
}

func (d *Database) GetDatabaseCollation() (string, error) {
	sqlStr := fmt.Sprintf(`SHOW VARIABLES LIKE 'collation_server'`)
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return "", err
	}
	return res[0]["VALUE"], nil
}

func (d *Database) GetDatabasePartitionTable(schemaName string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTemporaryTable(schemaName string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseClusteredTable(schemaName string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseMaterializedView(schemaName string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseNormalView(schemaName string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseCompositeTypeTable(schemaName string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseExternalTable(schemaName string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableType(schemaName string) (map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTablePrimaryKey(schemaName string, tableName string) ([]map[string]string, error) {
	sqlStr := fmt.Sprintf(`SELECT
	TC.CONSTRAINT_NAME,
	CASE
		TC.CONSTRAINT_TYPE 
		WHEN 'PRIMARY KEY' THEN
		'PK'
		WHEN 'UNIQUE' THEN
		'UK'
		ELSE 'UKNOWN'
	END AS CONSTRAINT_TYPE,
	GROUP_CONCAT(KU.COLUMN_NAME ORDER BY KU.ORDINAL_POSITION SEPARATOR ',') COLUMN_LIST
FROM
	INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC,
	INFORMATION_SCHEMA.KEY_COLUMN_USAGE KU
WHERE
	TC.TABLE_SCHEMA = KU.TABLE_SCHEMA
	AND TC.TABLE_NAME = KU.TABLE_NAME
	AND TC.CONSTRAINT_SCHEMA = KU.CONSTRAINT_SCHEMA
	AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
	AND TC.CONSTRAINT_CATALOG = KU.CONSTRAINT_CATALOG
	AND TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
	AND TC.TABLE_SCHEMA = '%s'
	AND TC.TABLE_NAME = '%s'
GROUP BY
	TC.CONSTRAINT_NAME,
	TC.CONSTRAINT_TYPE`, schemaName, tableName)
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableUniqueKey(schemaName string, tableName string) ([]map[string]string, error) {
	sqlStr := fmt.Sprintf(`SELECT
	TC.CONSTRAINT_NAME,
	CASE
		TC.CONSTRAINT_TYPE 
		WHEN 'PRIMARY KEY' THEN
		'PK'
		WHEN 'UNIQUE' THEN
		'UK'
		ELSE 'UKNOWN'
	END AS CONSTRAINT_TYPE,
	GROUP_CONCAT(KU.COLUMN_NAME ORDER BY KU.ORDINAL_POSITION SEPARATOR ',') COLUMN_LIST
FROM
	INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC,
	INFORMATION_SCHEMA.KEY_COLUMN_USAGE KU
WHERE
	TC.TABLE_SCHEMA = KU.TABLE_SCHEMA
	AND TC.TABLE_NAME = KU.TABLE_NAME
	AND TC.CONSTRAINT_SCHEMA = KU.CONSTRAINT_SCHEMA
	AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
	AND TC.CONSTRAINT_CATALOG = KU.CONSTRAINT_CATALOG
	AND TC.CONSTRAINT_TYPE = 'UNIQUE'
	AND TC.TABLE_SCHEMA = '%s'
	AND TC.TABLE_NAME = '%s'
GROUP BY
	TC.CONSTRAINT_NAME,
	TC.CONSTRAINT_TYPE`, schemaName, tableName)
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableForeignKey(schemaName string, tableName string) ([]map[string]string, error) {
	sqlStr := fmt.Sprintf(`SELECT
		TC.CONSTRAINT_NAME,
		KU.COLUMN_NAME COLUMN_LIST,
		KU.REFERENCED_TABLE_SCHEMA R_OWNER,
		KU.REFERENCED_TABLE_NAME RTABLE_NAME,
		KU.REFERENCED_COLUMN_NAME RCOLUMN_LIST,
		RC.DELETE_RULE,
		RC.UPDATE_RULE
FROM
	INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC,
	INFORMATION_SCHEMA.KEY_COLUMN_USAGE KU,
	INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
WHERE
	TC.TABLE_SCHEMA = KU.TABLE_SCHEMA
	AND TC.TABLE_NAME = KU.TABLE_NAME
	AND TC.CONSTRAINT_SCHEMA = KU.CONSTRAINT_SCHEMA
	AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
	AND TC.CONSTRAINT_CATALOG = KU.CONSTRAINT_CATALOG
	AND TC.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG
	AND TC.TABLE_SCHEMA = RC.CONSTRAINT_SCHEMA
	AND TC.TABLE_NAME = RC.TABLE_NAME
	AND TC.CONSTRAINT_TYPE = 'FOREIGN KEY'
	AND TC.TABLE_SCHEMA = '%s'
	AND TC.TABLE_NAME = '%s'`, schemaName, tableName)
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableCheckKey(schemaName string, tableName string) ([]map[string]string, error) {
	_, res, err := d.GeneralQuery(`SELECT VERSION() AS VERSION`)
	if err != nil {
		return nil, err
	}

	var sqlStr string
	if strings.Contains(res[0]["VERSION"], constant.DatabaseTypeTiDB) {
		var dbVersion string

		if strings.Contains(res[0]["VERSION"], constant.MYSQLCompatibleDatabaseVersionDelimiter) {
			dbVersion = strings.Split(res[0]["VERSION"], constant.MYSQLCompatibleDatabaseVersionDelimiter)[2]
		} else {
			dbVersion = res[0]["VERSION"]
		}
		if stringutil.VersionOrdinal(strings.Trim(dbVersion, "v")) >= stringutil.VersionOrdinal(constant.TIDBDatabaseCheckConstraintSupportVersion) {
			sqlStr = fmt.Sprintf(`SELECT
	TC.CONSTRAINT_NAME,
	RC.CHECK_CLAUSE SEARCH_CONDITION
FROM
	INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC,
	INFORMATION_SCHEMA.CHECK_CONSTRAINTS RC
WHERE
	TC.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG
	AND TC.TABLE_SCHEMA = RC.CONSTRAINT_SCHEMA
	AND TC.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
	AND TC.CONSTRAINT_TYPE = 'CHECK'
	AND TC.TABLE_SCHEMA = '%s'
	AND TC.TABLE_NAME = '%s'`, schemaName, tableName)
		} else {
			return nil, nil
		}
	} else {
		var dbVersion string

		if strings.Contains(res[0]["VERSION"], constant.MYSQLCompatibleDatabaseVersionDelimiter) {
			dbVersion = strings.Split(res[0]["VERSION"], constant.MYSQLCompatibleDatabaseVersionDelimiter)[0]
		} else {
			dbVersion = res[0]["VERSION"]
		}
		if stringutil.VersionOrdinal(dbVersion) >= stringutil.VersionOrdinal(constant.MYSQLDatabaseCheckConstraintSupportVersion) {
			sqlStr = fmt.Sprintf(`SELECT
	TC.CONSTRAINT_NAME,
	RC.CHECK_CLAUSE SEARCH_CONDITION
FROM
	INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC,
	INFORMATION_SCHEMA.CHECK_CONSTRAINTS RC
WHERE
	TC.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG
	AND TC.TABLE_SCHEMA = RC.CONSTRAINT_SCHEMA
	AND TC.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
	AND TC.CONSTRAINT_TYPE = 'CHECK'
	AND TC.TABLE_SCHEMA = '%s'
	AND TC.TABLE_NAME = '%s'`, schemaName, tableName)
		} else {
			return nil, nil
		}
	}

	_, res, err = d.GeneralQuery(sqlStr)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableNormalIndex(schemaName string, tableName string) ([]map[string]string, error) {
	_, res, err := d.GeneralQuery(`SELECT VERSION() AS VERSION`)
	if err != nil {
		return nil, err
	}

	var sqlStr string
	if strings.Contains(res[0]["VERSION"], constant.DatabaseTypeTiDB) {
		sqlStr = fmt.Sprintf(`SELECT 
		INDEX_NAME,
		INDEX_TYPE,
		IFNULL(EXPRESSION, '') COLUMN_EXPRESSION,
		IF(NON_UNIQUE = 1,"NONUNIQUE","UNIQUE") AS UNIQUENESS,
		GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR '|+|') AS COLUMN_LIST
FROM
	INFORMATION_SCHEMA.STATISTICS
WHERE
	INDEX_NAME NOT IN ('PRIMARY', 'UNIQUE')
	AND TABLE_SCHEMA = '%s'
	AND TABLE_NAME = '%s'
	AND NON_UNIQUE = 1
GROUP BY
	INDEX_NAME,
	UNIQUENESS,
	INDEX_TYPE,
	COLUMN_EXPRESSION`, schemaName, tableName)
	} else {
		var mysqlDBVersion string

		if strings.Contains(res[0]["VERSION"], constant.MYSQLCompatibleDatabaseVersionDelimiter) {
			mysqlDBVersion = strings.Split(res[0]["VERSION"], constant.MYSQLCompatibleDatabaseVersionDelimiter)[0]
		} else {
			mysqlDBVersion = res[0]["VERSION"]
		}
		if stringutil.VersionOrdinal(mysqlDBVersion) >= stringutil.VersionOrdinal(constant.MYSQLDatabaseExpressionIndexSupportVersion) {
			sqlStr = fmt.Sprintf(`SELECT 
		INDEX_NAME,
		INDEX_TYPE,
		IFNULL(EXPRESSION, '') COLUMN_EXPRESSION,
		IF(NON_UNIQUE = 1,"NONUNIQUE","UNIQUE") AS UNIQUENESS,
		GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR '|+|') AS COLUMN_LIST
FROM
	INFORMATION_SCHEMA.STATISTICS
WHERE
	INDEX_NAME NOT IN ('PRIMARY', 'UNIQUE')
	AND TABLE_SCHEMA = '%s'
	AND TABLE_NAME = '%s'
	AND NON_UNIQUE = 1
GROUP BY
	INDEX_NAME,
	UNIQUENESS,
	INDEX_TYPE,
	COLUMN_EXPRESSION`, schemaName, tableName)
		} else {
			sqlStr = fmt.Sprintf(`SELECT 
		INDEX_NAME,
		INDEX_TYPE,
		IF(NON_UNIQUE = 1,"NONUNIQUE","UNIQUE") AS UNIQUENESS,
		GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR '|+|') AS COLUMN_LIST
FROM
	INFORMATION_SCHEMA.STATISTICS
WHERE
	INDEX_NAME NOT IN ('PRIMARY', 'UNIQUE')
	AND TABLE_SCHEMA = '%s'
	AND TABLE_NAME = '%s'
	AND NON_UNIQUE = 1
GROUP BY
	INDEX_NAME,
	UNIQUENESS,
	INDEX_TYPE`, schemaName, tableName)
		}
	}

	_, res, err = d.GeneralQuery(sqlStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableUniqueIndex(schemaName string, tableName string) ([]map[string]string, error) {
	_, res, err := d.GeneralQuery(`SELECT VERSION() AS VERSION`)
	if err != nil {
		return nil, err
	}

	var sqlStr string
	if strings.Contains(res[0]["VERSION"], constant.DatabaseTypeTiDB) {
		sqlStr = fmt.Sprintf(`SELECT 
		INDEX_NAME,
		INDEX_TYPE,
		IFNULL(EXPRESSION, '') COLUMN_EXPRESSION,
		IF(NON_UNIQUE = 1,"NONUNIQUE","UNIQUE") AS UNIQUENESS,
		GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR '|+|') AS COLUMN_LIST
FROM
	INFORMATION_SCHEMA.STATISTICS
WHERE
	INDEX_NAME NOT IN ('PRIMARY', 'UNIQUE')
	AND TABLE_SCHEMA = '%s'
	AND TABLE_NAME = '%s'
	AND NON_UNIQUE <> 1
GROUP BY
	INDEX_NAME,
	UNIQUENESS,
	INDEX_TYPE,
	COLUMN_EXPRESSION`, schemaName, tableName)
	} else {
		var mysqlDBVersion string

		if strings.Contains(res[0]["VERSION"], constant.MYSQLCompatibleDatabaseVersionDelimiter) {
			mysqlDBVersion = strings.Split(res[0]["VERSION"], constant.MYSQLCompatibleDatabaseVersionDelimiter)[0]
		} else {
			mysqlDBVersion = res[0]["VERSION"]
		}
		if stringutil.VersionOrdinal(mysqlDBVersion) >= stringutil.VersionOrdinal(constant.MYSQLDatabaseExpressionIndexSupportVersion) {
			sqlStr = fmt.Sprintf(`SELECT 
		INDEX_NAME,
		INDEX_TYPE,
		IFNULL(EXPRESSION, '') COLUMN_EXPRESSION,
		IF(NON_UNIQUE = 1,"NONUNIQUE","UNIQUE") AS UNIQUENESS,
		GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR '|+|') AS COLUMN_LIST
FROM
	INFORMATION_SCHEMA.STATISTICS
WHERE
	INDEX_NAME NOT IN ('PRIMARY', 'UNIQUE')
	AND TABLE_SCHEMA = '%s'
	AND TABLE_NAME = '%s'
	AND NON_UNIQUE <> 1
GROUP BY
	INDEX_NAME,
	UNIQUENESS,
	INDEX_TYPE,
	COLUMN_EXPRESSION`, schemaName, tableName)
		} else {
			sqlStr = fmt.Sprintf(`SELECT 
		INDEX_NAME,
		INDEX_TYPE,
		IF(NON_UNIQUE = 1,"NONUNIQUE","UNIQUE") AS UNIQUENESS,
		GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR '|+|') AS COLUMN_LIST
FROM
	INFORMATION_SCHEMA.STATISTICS
WHERE
	INDEX_NAME NOT IN ('PRIMARY', 'UNIQUE')
	AND TABLE_SCHEMA = '%s'
	AND TABLE_NAME = '%s'
	AND NON_UNIQUE <> 1
GROUP BY
	INDEX_NAME,
	UNIQUENESS,
	INDEX_TYPE`, schemaName, tableName)
		}
	}

	_, res, err = d.GeneralQuery(sqlStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableComment(schemaName string, tableName string) ([]map[string]string, error) {
	sqlStr := fmt.Sprintf(`SELECT
	TABLE_NAME,
	TABLE_COMMENT
FROM
	INFORMATION_SCHEMA.TABLES
WHERE
	TABLE_SCHEMA = '%s'
	AND TABLE_NAME = '%s'`, schemaName, tableName)

	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableColumnInfo(schemaName string, tableName string) ([]map[string]string, error) {
	sqlStr := fmt.Sprintf(`SELECT
		COLUMN_NAME,
		DATA_TYPE,
		IFNULL(CHARACTER_MAXIMUM_LENGTH, 0) DATA_LENGTH,
		IFNULL(NUMERIC_SCALE, 0) DATA_SCALE,
		IFNULL(NUMERIC_PRECISION, 0) DATA_PRECISION,
		IFNULL(DATETIME_PRECISION, 0) DATETIME_PRECISION,
		IF(IS_NULLABLE = 'NO','N','Y') NULLABLE,
		IFNULL(COLUMN_DEFAULT, 'NULLSTRING') DATA_DEFAULT,
		IFNULL(COLUMN_COMMENT, '') COMMENTS,
		IFNULL(CHARACTER_SET_NAME, 'UNKNOWN') CHARACTER_SET_NAME,
		IFNULL(COLLATION_NAME, 'UNKNOWN') COLLATION_NAME
FROM
	INFORMATION_SCHEMA.COLUMNS
WHERE
	TABLE_SCHEMA = '%s'
	AND TABLE_NAME = '%s'
ORDER BY
	ORDINAL_POSITION`, schemaName, tableName)

	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableColumnComment(schemaName string, tableName string) ([]map[string]string, error) {
	sqlStr := fmt.Sprintf(`SELECT
		COLUMN_NAME,
		IFNULL(COLUMN_COMMENT, '') COMMENTS
FROM
	INFORMATION_SCHEMA.COLUMNS
WHERE
	TABLE_SCHEMA = '%s'
	AND TABLE_NAME = '%s'
ORDER BY
	ORDINAL_POSITION`, schemaName, tableName)

	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseSchemaCollation(schemaName string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableCharset(schemaName, tableName string) (string, error) {
	sqlStr := fmt.Sprintf(`SELECT
	IFNULL(CCSA.CHARACTER_SET_NAME, 'UNKNOWN') CHARACTER_SET_NAME
FROM
	INFORMATION_SCHEMA.TABLES T,
	INFORMATION_SCHEMA.COLLATION_CHARACTER_SET_APPLICABILITY CCSA
WHERE
	CCSA.COLLATION_NAME = T.TABLE_COLLATION
	AND T.TABLE_SCHEMA = '%s'
	AND T.TABLE_NAME = '%s'`, schemaName, tableName)

	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return "", err
	}
	return res[0]["CHARACTER_SET_NAME"], nil
}

func (d *Database) GetDatabaseTableCollation(schemaName, tableName string) (string, error) {
	sqlStr := fmt.Sprintf(`SELECT
	IFNULL(T.TABLE_COLLATION, 'UNKNOWN') COLLATION
FROM
	INFORMATION_SCHEMA.TABLES T,
	INFORMATION_SCHEMA.COLLATION_CHARACTER_SET_APPLICABILITY CCSA
WHERE
	CCSA.COLLATION_NAME = T.TABLE_COLLATION
	AND T.TABLE_SCHEMA = '%s'
	AND T.TABLE_NAME = '%s'`, schemaName, tableName)
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return "", err
	}
	return res[0]["COLLATION"], nil
}

func (d *Database) GetDatabaseTableOriginStruct(schemaName, tableName, tableType string) (string, error) {
	sqlStr := fmt.Sprintf(`SHOW CREATE TABLE %s.%s`, schemaName, tableName)
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return "", err
	}
	return res[0]["Create Table"], nil
}
