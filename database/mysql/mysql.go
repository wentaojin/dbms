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
	Ctx    context.Context
	DBConn *sql.DB
}

func NewDatabase(ctx context.Context, datasource *datasource.Datasource) (*Database, error) {
	if !strings.EqualFold(datasource.ConnectCharset, "") {
		datasource.ConnectParams = fmt.Sprintf("charset=%s&%s", strings.ToLower(datasource.ConnectStatus), datasource.ConnectParams)
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
	return &Database{Ctx: ctx, DBConn: mysqlDB}, nil
}

func PingDatabaseConnection(datasource *datasource.Datasource) error {
	if !strings.EqualFold(datasource.ConnectCharset, "") {
		datasource.ConnectParams = fmt.Sprintf("charset=%s&%s", strings.ToLower(datasource.ConnectStatus), datasource.ConnectParams)
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?%s",
		datasource.Username, datasource.Password, datasource.Host, datasource.Port, datasource.ConnectParams)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("error on open mysql database connection: %v", err)
	}

	err = db.Ping()
	if err != nil {
		return fmt.Errorf("database ping failed, database error: [%v]", err)
	}
	return nil
}

func (d *Database) QueryContext(query string) (*sql.Rows, error) {
	return d.DBConn.QueryContext(d.Ctx, query)
}

func (d *Database) ExecContext(query string, args ...any) (sql.Result, error) {
	return d.DBConn.ExecContext(d.Ctx, query, args)
}

func (d *Database) GeneralQuery(query string) ([]string, []map[string]string, error) {
	var (
		columns []string
		results []map[string]string
	)
	rows, err := d.QueryContext(query)
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
				row[columns[k]] = string(v)
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
