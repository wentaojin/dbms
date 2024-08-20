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
	"strconv"
	"strings"

	"github.com/wentaojin/dbms/utils/constant"
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

	if strings.Contains(res[0]["VERSION"], constant.DatabaseTypeTiDB) {
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

func (d *Database) GetDatabaseConsistentPos() (uint64, error) {
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

func (d *Database) GetDatabaseTableChunkData(querySQL string, queryArgs []interface{}, batchSize, callTimeout int, dbCharsetS, dbCharsetT, columnDetailS string, dataChan chan []interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableCsvData(querySQL string, queryArgs []interface{}, callTimeout int, taskFlow, dbCharsetS, dbCharsetT, columnDetailO string, escapeBackslash bool, nullValue, separator, delimiter string, dataChan chan []string) error {
	//TODO implement me
	panic("implement me")
}
