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
	"strings"
	"time"

	"github.com/wentaojin/dbms/utils/stringutil"

	_ "github.com/go-sql-driver/mysql"

	"github.com/wentaojin/dbms/model/datasource"
)

const (
	MYSQLDatabaseMaxIdleConn     = 512
	MYSQLDatabaseMaxConn         = 1024
	MYSQLDatabaseConnMaxLifeTime = 300 * time.Second
	MYSQLDatabaseConnMaxIdleTime = 200 * time.Second
)

type Database struct {
	Ctx         context.Context
	DBConn      *sql.DB
	CallTimeout int64 // unit: seconds, sql execute timeout
}

func NewDatabase(ctx context.Context, datasource *datasource.Datasource, callTimeout int64) (*Database, error) {
	if !strings.EqualFold(datasource.ConnectCharset, "") {
		datasource.ConnectParams = fmt.Sprintf("charset=%s&%s", strings.ToLower(datasource.ConnectCharset), datasource.ConnectParams)
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?%s",
		datasource.Username, datasource.Password, datasource.Host, datasource.Port, datasource.ConnectParams)

	mysqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("error on open mysql database connection: %v", err)
	}

	mysqlDB.SetMaxIdleConns(MYSQLDatabaseMaxIdleConn)
	mysqlDB.SetMaxOpenConns(MYSQLDatabaseMaxConn)
	mysqlDB.SetConnMaxLifetime(MYSQLDatabaseConnMaxLifeTime)
	mysqlDB.SetConnMaxIdleTime(MYSQLDatabaseConnMaxIdleTime)

	if err = mysqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("error on ping mysql database connection: %v", err)
	}
	return &Database{Ctx: ctx, DBConn: mysqlDB, CallTimeout: callTimeout}, nil
}

func (d *Database) PingDatabaseConnection() error {
	err := d.DBConn.Ping()
	if err != nil {
		return fmt.Errorf("error on ping mysql database connection:%v", err)
	}
	return nil
}

func (d *Database) PrepareContext(ctx context.Context, sqlStr string) (*sql.Stmt, error) {
	return d.DBConn.PrepareContext(ctx, sqlStr)
}

func (d *Database) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return d.DBConn.QueryContext(ctx, query, args...)
}

func (d *Database) BeginTxn(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return d.DBConn.BeginTx(ctx, opts)
}

func (d *Database) CommitTxn(txn *sql.Tx) error {
	return txn.Commit()
}

func (d *Database) Transaction(ctx context.Context, opts *sql.TxOptions, fns []func(ctx context.Context, tx *sql.Tx) error) error {
	tx, err := d.BeginTxn(ctx, opts)
	if err != nil {
		return err
	}
	for _, fn := range fns {
		if err = fn(ctx, tx); err != nil {
			tx.Rollback()
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}
	return nil
}

func (d *Database) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return d.DBConn.ExecContext(ctx, query, args...)
}

func (d *Database) GeneralQuery(query string, args ...any) ([]string, []map[string]string, error) {
	var (
		columns []string
		results []map[string]string
	)

	deadline := time.Now().Add(time.Duration(d.CallTimeout) * time.Second)

	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()

	rows, err := d.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	// general query, automatic get column name
	columns, err = rows.Columns()
	if err != nil {
		return columns, results, fmt.Errorf("query rows.Columns failed, sql: [%v], error: [%v]", query, err)
	}

	values := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for i := range values {
		scans[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return columns, results, fmt.Errorf("query rows.Scan failed, sql: [%v], error: [%v]", query, err)
		}

		row := make(map[string]string)
		for k, v := range values {
			// Notes: oracle database NULL and ""
			//	1, if the return value is NULLABLE, it represents the value is NULL, oracle sql query statement had be required the field NULL judgement, and if the filed is NULL, it returns that the value is NULLABLE
			//	2, if the return value is nil, it represents the value is NULL
			//	3, if the return value is "", it represents the value is "" string
			//	4, if the return value is 'NULL' or 'null', it represents the value is NULL or null string
			if v == nil {
				row[columns[k]] = "NULLABLE"
			} else {
				// Handling empty string and other values, the return value output string
				row[columns[k]] = stringutil.BytesToString(v)
			}
		}
		results = append(results, row)
	}

	if err = rows.Err(); err != nil {
		return columns, results, fmt.Errorf("query rows.Next failed, sql: [%v], error: [%v]", query, err.Error())
	}
	return columns, results, nil
}

func (d *Database) Close() error {
	return d.DBConn.Close()
}
