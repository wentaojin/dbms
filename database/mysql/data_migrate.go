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
	"strings"

	"github.com/wentaojin/dbms/utils/constant"
)

func (d *Database) GetDatabaseVersion() (string, error) {
	_, res, err := d.GeneralQuery(`SELECT VERSION() AS VERSION`)
	if err != nil {
		return "", err
	}

	var version string
	verinfos := strings.Split(res[0]["VERSION"], constant.MYSQLDatabaseVersionDelimiter)

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

func (d *Database) GetDatabaseCharset() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseConsistentPos() (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableType(schemaName string) (map[string]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableColumnInfo(schemaName string, tableName string, collation bool) ([]map[string]string, error) {
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

func (d *Database) GetDatabaseTableChunkData(querySQL string, batchSize, callTimeout int, dbCharsetS, dbCharsetT, columnDetailS string, dataChan chan []interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableCsvData(querySQL string, callTimeout int, taskFlow, dbCharsetS, dbCharsetT, columnDetailO string, escapeBackslash bool, nullValue, separator, delimiter string, dataChan chan []string) error {
	//TODO implement me
	panic("implement me")
}
