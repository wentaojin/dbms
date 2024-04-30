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
package database

import (
	"context"
	"database/sql"
	"strings"

	"github.com/wentaojin/dbms/database/mysql"

	"github.com/wentaojin/dbms/database/oracle"
	"github.com/wentaojin/dbms/model/datasource"
	"github.com/wentaojin/dbms/utils/constant"
)

type IDatabase interface {
	PrepareContext(ctx context.Context, sqlStr string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, sqlStr string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, sqlStr string, args ...any) (sql.Result, error)
	GeneralQuery(sqlStr string) ([]string, []map[string]string, error)
	PingDatabaseConnection() error
	Close() error
	IDatabaseObjectFilter
	IDatabaseAssessMigrate
	IDatabaseStructMigrate
	IDatabaseSequenceMigrate
	IDatabaseDataMigrate
	IDatabaseDataCompare
	IDatabaseStructCompare
}

type IDatabaseObjectFilter interface {
	FilterDatabaseSchema() ([]string, error)
	FilterDatabaseTable(sourceSchema string, includeTableS, excludeTableS []string) ([]string, error)
}

type IDatabaseSchemaTableRule interface {
	GenSchemaTableTypeRule() string
	GenSchemaNameRule() (string, string, error)
	GenSchemaTableNameRule() (string, string, error)
	GenSchemaTableColumnRule() (string, string, string, string, error)
}

func NewDatabase(ctx context.Context, datasource *datasource.Datasource, migrateOracleSchema string) (IDatabase, error) {
	var (
		database IDatabase
		err      error
	)
	switch {
	case strings.EqualFold(datasource.DbType, constant.DatabaseTypeOracle):
		database, err = oracle.NewDatabase(ctx, datasource, migrateOracleSchema)
		if err != nil {
			return database, err
		}
	case strings.EqualFold(datasource.DbType, constant.DatabaseTypeTiDB) || strings.EqualFold(datasource.DbType, constant.DatabaseTypeMySQL):
		database, err = mysql.NewDatabase(ctx, datasource)
		if err != nil {
			return database, err
		}
	}

	return database, nil
}
