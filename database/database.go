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
	"github.com/wentaojin/dbms/database/postgresql"
	"github.com/wentaojin/dbms/utils/structure"
	"golang.org/x/sync/errgroup"
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
	GeneralQuery(sqlStr string, args ...any) ([]string, []map[string]string, error)
	PingDatabaseConnection() error
	Close() error
	IDatabaseTableFilter
	IDatabaseAssessMigrate
	IDatabaseStructMigrate
	IDatabaseSequenceMigrate
	IDatabaseDataMigrate
	IDatabaseDataCompare
	IDatabaseStructCompare
}

type IDatabaseTableFilter interface {
	FilterDatabaseTable(sourceSchema string, includeTableS, excludeTableS []string) (*structure.TableObjects, error)
	FilterDatabaseSequence(sourceSchema string, includeSequenceS, excludeSequenceS []string) (*structure.SequenceObjects, error)
}

type IDatabaseSchemaTableRule interface {
	GenSchemaTableTypeRule() string
	GenSchemaNameRule() (string, string, error)
	GenSchemaTableNameRule() (string, string, error)
	GetSchemaTableColumnNameRule() (map[string]string, error)
	GenSchemaTableColumnSelectRule() (string, string, string, string, error)
}

// IDatabaseRunner used for database table migrate runner
type IDatabaseRunner interface {
	Init() error
	Run() error
	Resume() error
	Last() error
}

func IDatabaseRun(ctx context.Context, i IDatabaseRunner) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		err := i.Init()
		if err != nil {
			return err
		}
		return nil
	})

	g.Go(func() error {
		err := i.Run()
		if err != nil {
			return err
		}
		return nil
	})

	g.Go(func() error {
		err := i.Resume()
		if err != nil {
			return err
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	err := i.Last()
	if err != nil {
		return err
	}
	return nil
}

func NewDatabase(ctx context.Context, datasource *datasource.Datasource, migrateOracleSchema string, callTimeout int64) (IDatabase, error) {
	var (
		database IDatabase
		err      error
	)
	switch {
	case strings.EqualFold(datasource.DbType, constant.DatabaseTypeOracle):
		database, err = oracle.NewDatabase(ctx, datasource, migrateOracleSchema, callTimeout)
		if err != nil {
			return database, err
		}
	case strings.EqualFold(datasource.DbType, constant.DatabaseTypeTiDB) || strings.EqualFold(datasource.DbType, constant.DatabaseTypeMySQL):
		database, err = mysql.NewDatabase(ctx, datasource, callTimeout)
		if err != nil {
			return database, err
		}
	case strings.EqualFold(datasource.DbType, constant.DatabaseTypePostgresql):
		database, err = postgresql.NewDatabase(ctx, datasource, callTimeout)
		if err != nil {
			return database, err
		}
	}

	return database, nil
}
