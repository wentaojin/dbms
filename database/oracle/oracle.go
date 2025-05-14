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
package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/godror/godror"
	"github.com/godror/godror/dsn"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/model/datasource"
)

type Database struct {
	Ctx         context.Context
	DBConn      *sql.DB
	CallTimeout int64 // unit: seconds, sql execute timeout
}

func NewDatabase(ctx context.Context, datasource *datasource.Datasource, currentSchema string, callTimeout int64) (*Database, error) {
	// https://pkg.go.dev/github.com/godror/godror
	// https://github.com/godror/godror/blob/db9cd12d89cdc1c60758aa3f36ece36cf5a61814/doc/connection.md
	// https://godror.github.io/godror/doc/connection.html
	// You can specify connection timeout seconds with "?connect_timeout=15" - Ping uses this timeout, NOT the Deadline in Context!
	// For more connection options, see [Godor Connection Handling](https://godror.github.io/godror/doc/connection.html).
	var (
		connString    string
		oraDSN        dsn.ConnectionParams
		err           error
		sessionParams []string
	)

	//https://www.syntio.net/en/labs-musings/efficient-fetching-of-data-from-oracle-database-in-golang/
	// https://github.com/godror/godror/pull/65
	//connClass := fmt.Sprintf("pool_%v", xid.New().String())
	//connString = fmt.Sprintf("oracle://@%s/%s?connectionClass=%s&%s",
	//	stringutil.StringsBuilder(datasource.Host, ":", strconv.Itoa(datasource.Port)),
	//	datasource.ServiceName, "connClass", datasource.ConnectParams)
	if strings.EqualFold(datasource.ConnectParams, "") {
		connString = fmt.Sprintf("oracle://@%s/%s?standaloneConnection=1",
			stringutil.StringBuilder(datasource.Host, ":", strconv.FormatUint(datasource.Port, 10)),
			datasource.ServiceName)
	} else {
		connString = fmt.Sprintf("oracle://@%s/%s?standaloneConnection=1&%s",
			stringutil.StringBuilder(datasource.Host, ":", strconv.FormatUint(datasource.Port, 10)),
			datasource.ServiceName, datasource.ConnectParams)
	}

	oraDSN, err = godror.ParseDSN(connString)
	if err != nil {
		return nil, err
	}

	oraDSN.Username, oraDSN.Password = datasource.Username, godror.NewPassword(datasource.Password)

	if !strings.EqualFold(datasource.PdbName, "") {
		sessionParams = append(sessionParams, fmt.Sprintf(`ALTER SESSION SET CONTAINER = %s`, datasource.PdbName))
	}

	// session params
	sessionParams = append(sessionParams, []string{
		"BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE); END;",
		"BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE); END;",
		"BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE); END;",
		"BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', FALSE); END;",
		"BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE); END;",
	}...)
	if !strings.EqualFold(datasource.Username, currentSchema) && !strings.EqualFold(currentSchema, "") {
		sessionParams = append(sessionParams, fmt.Sprintf(`ALTER SESSION SET CURRENT_SCHEMA = "%s"`, currentSchema))
	}

	if !strings.EqualFold(datasource.SessionParams, "") {
		sessionParams = append(sessionParams, stringutil.StringSplit(datasource.SessionParams, constant.StringSeparatorComma)...)
	}

	// close external auth
	oraDSN.ExternalAuth = false
	oraDSN.OnInitStmts = sessionParams

	// charset
	if !strings.EqualFold(datasource.ConnectCharset, "") {
		oraDSN.CommonParams.Charset = datasource.ConnectCharset
	}

	sqlDB := sql.OpenDB(godror.NewConnector(oraDSN))
	sqlDB.SetMaxIdleConns(0)
	sqlDB.SetMaxOpenConns(0)
	sqlDB.SetConnMaxLifetime(0)

	err = sqlDB.Ping()
	if err != nil {
		return nil, fmt.Errorf("connection string [%s] error on creating ping oracle database connection: %v", oraDSN.String(), err)
	}
	return &Database{Ctx: ctx, DBConn: sqlDB, CallTimeout: callTimeout}, nil
}

func (d *Database) PingDatabaseConnection() error {
	err := d.DBConn.Ping()
	if err != nil {
		return fmt.Errorf("error on testing ping oracle database connection: %v", err)
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

func (d *Database) GetDatabaseSchema() ([]string, error) {
	var (
		schemas []string
		err     error
	)
	columns, res, err := d.GeneralQuery(`SELECT DISTINCT USERNAME FROM DBA_USERS`)
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
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT TABLE_NAME FROM DBA_TABLES WHERE OWNER = '%s' AND (IOT_TYPE IS NUll OR IOT_TYPE='IOT')`, schemaName))
	if err != nil {
		return tables, err
	}
	for _, r := range res {
		tables = append(tables, r["TABLE_NAME"])
	}
	return tables, nil
}

func (d *Database) GetDatabaseTableColumnNameWithoutFormat(schemaName, tableName string, columnDelimiter ...string) ([]string, error) {
	columns, _, err := d.GeneralQuery(fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE ROWNUM = 1`, schemaName, tableName))
	if err != nil {
		return nil, err
	}
	if len(columnDelimiter) == 1 {
		var newColumns []string
		for _, c := range columns {
			newColumns = append(newColumns, fmt.Sprintf("%s%s%s", columnDelimiter, c, columnDelimiter))
		}
		return newColumns, nil
	} else if len(columnDelimiter) > 1 {
		return nil, fmt.Errorf("column delimiter params [%v] values is over one, it should be one", columnDelimiter)
	}
	return columns, nil
}
