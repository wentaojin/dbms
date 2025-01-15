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
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func (d *Database) GetDatabaseRole() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseVersion() (string, error) {
	_, res, err := d.GeneralQuery(`SELECT VERSION() AS VERSION`)
	if err != nil {
		return "", err
	}

	var version string
	verinfos := strings.Split(res[0]["VERSION"], constant.MYSQLCompatibleDatabaseVersionDelimiter)

	if strings.Contains(stringutil.StringLower(res[0]["VERSION"]), stringutil.StringLower(constant.DatabaseTypeTiDB)) {
		for _, ver := range verinfos {
			if strings.HasPrefix(ver, "v") {
				version = strings.TrimPrefix(ver, "v")
			}
		}
	} else if strings.Contains(stringutil.StringLower(res[0]["VERSION"]), stringutil.StringLower(constant.DatabaseTypeOceanbase)) {
		for _, ver := range verinfos {
			if strings.HasPrefix(ver, "v") {
				version = strings.TrimPrefix(ver, "v")
			}
		}
	} else {
		version = verinfos[0]
	}
	return version, nil
}

func (d *Database) GetDatabaseSchemaPrimaryTables(schemaName string) ([]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT
	TC.TABLE_NAME,
	TC.CONSTRAINT_NAME,
	CASE
		TC.CONSTRAINT_TYPE 
		WHEN 'PRIMARY KEY' THEN
		'PK'
		WHEN 'UNIQUE' THEN
		'UK'
		ELSE 'UKNOWN'
	END AS CONSTRAINT_TYPE,
	GROUP_CONCAT(KU.COLUMN_NAME ORDER BY KU.ORDINAL_POSITION SEPARATOR '|+|') COLUMN_LIST
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
GROUP BY
	TC.TABLE_NAME,
	TC.CONSTRAINT_NAME,
	TC.CONSTRAINT_TYPE`, schemaName))
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
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT 
	s.INDEX_NAME,
	s.COLUMN_NAME,
	c.IS_NULLABLE
FROM
	INFORMATION_SCHEMA.STATISTICS s,
	INFORMATION_SCHEMA.COLUMNS c
WHERE
	s.TABLE_SCHEMA = c.TABLE_SCHEMA
	AND s.TABLE_NAME = c.TABLE_NAME
	AND s.COLUMN_NAME = c.COLUMN_NAME
	AND s.INDEX_NAME NOT IN ('PRIMARY')
	AND s.TABLE_SCHEMA = '%s'
	AND s.TABLE_NAME = '%s'
	AND s.NON_UNIQUE <> 1
	AND c.GENERATION_EXPRESSION = ''`, schemaName, tableName))
	if err != nil {
		return false, err
	}

	indexNameColumns := make(map[string][]string)
	indexNameNulls := make(map[string]int)

	for _, r := range res {
		if vals, ok := indexNameColumns[r["INDEX_NAME"]]; ok {
			indexNameColumns[r["INDEX_NAME"]] = append(vals, r["COLUMN_NAME"])
		} else {
			indexNameColumns[r["INDEX_NAME"]] = append(indexNameColumns[r["INDEX_NAME"]], r["COLUMN_NAME"])
		}

		if r["IS_NULLABLE"] == "NO" {
			if vals, ok := indexNameNulls[r["INDEX_NAME"]]; ok {
				indexNameNulls[r["INDEX_NAME"]] = vals + 1
			} else {
				indexNameNulls[r["INDEX_NAME"]] = 1
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
	if stringutil.VersionOrdinal(version) >= stringutil.VersionOrdinal(constant.MYSQLDatabaseVirtualColumnSupportVersion) {
		virtualC = true
	}

	var sqlStr string
	if virtualC {
		sqlStr = fmt.Sprintf(`SELECT
		COLUMN_NAME,
		DATA_TYPE,
		IFNULL(CHARACTER_MAXIMUM_LENGTH, 0) DATA_LENGTH,
		CASE 
			WHEN COALESCE(GENERATION_EXPRESSION, '') = '' THEN 'NO'
			ELSE 'YES'
		END AS IS_GENERATED
	FROM
	INFORMATION_SCHEMA.COLUMNS
	WHERE
	TABLE_SCHEMA = '%s'
	AND TABLE_NAME = '%s'
	ORDER BY
	ORDINAL_POSITION`, schemaName, tableName)
	} else {
		sqlStr = fmt.Sprintf(`SELECT
	COLUMN_NAME,
	DATA_TYPE,
	IFNULL(CHARACTER_MAXIMUM_LENGTH, 0) DATA_LENGTH,
	'NO' AS IS_GENERATED
FROM
INFORMATION_SCHEMA.COLUMNS
WHERE
TABLE_SCHEMA = '%s'
AND TABLE_NAME = '%s'
ORDER BY
ORDINAL_POSITION`, schemaName, tableName)
	}
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseConsistentPos(ctx context.Context, tx *sql.Tx) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableColumnNameTableDimensions(schemaName, tableName string) ([]string, error) {
	rows, err := d.QueryContext(d.Ctx, fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT 1", schemaName, tableName))
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
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableRows(schemaName, tableName string) (uint64, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT 
    IFNULL(TABLE_ROWS,0) AS NUM_ROWS
FROM 
    INFORMATION_SCHEMA.TABLES
WHERE 
    TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'`, schemaName, tableName))
	if err != nil {
		return 0, err
	}
	if len(res) != 1 {
		return 0, fmt.Errorf("get mysql compatible schema table [%v] rows by statistics falied, results: [%v]",
			fmt.Sprintf("%s.%s", schemaName, tableName), res)
	}
	numRows, err := strconv.ParseUint(res[0]["NUM_ROWS"], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("get mysql compatible schema table [%v] rows [%s] by statistics strconv.Atoi falied: %v",
			fmt.Sprintf("%s.%s", schemaName, tableName), res[0]["NUM_ROWS"], err)
	}
	return numRows, nil
}

func (d *Database) GetDatabaseTableSize(schemaName, tableName string) (float64, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT 
	ROUND(SUM((DATA_LENGTH+INDEX_LENGTH)/1024/1024),2) AS SIZE_MB
  FROM INFORMATION_SCHEMA.TABLES
WHERE 
    TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'`, schemaName, tableName))
	if err != nil {
		return 0, err
	}
	if len(res) == 0 {
		return 0, nil
	}
	if len(res) != 1 {
		return 0, fmt.Errorf("get mysql compatible schema table [%v] size by segment falied, results: [%v]",
			fmt.Sprintf("%s.%s", schemaName, tableName), res)
	}
	sizeMB, err := strconv.ParseFloat(res[0]["SIZE_MB"], 64)
	if err != nil {
		return 0, fmt.Errorf("get mysql compatible schema table [%v] size(MB) [%s] by segment strconv.Atoi falied: %v",
			fmt.Sprintf("%s.%s", schemaName, tableName), res[0]["SIZE_MB"], err)
	}
	return sizeMB, nil
}

func (d *Database) GetDatabaseTableChunkTask(taskName, schemaName, tableName string, chunkSize uint64, callTimeout uint64, batchSize int, dataChan chan []map[string]string) error {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableStmtData(querySQL string, queryArgs []interface{}, batchSize, callTimeout int, dbCharsetS, dbCharsetT, columnDetailS string, dataChan chan []interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableNonStmtData(taskFlow, querySQL string, queryArgs []interface{}, batchSize, callTimeout int, dbCharsetS, dbCharsetT, columnDetailS string, dataChan chan []interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableCsvData(querySQL string, queryArgs []interface{}, callTimeout int, taskFlow, dbCharsetS, dbCharsetT, columnDetailO string, escapeBackslash bool, nullValue, separator, delimiter string, dataChan chan []string) error {
	//TODO implement me
	panic("implement me")
}
