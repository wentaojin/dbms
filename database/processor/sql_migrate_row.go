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
package processor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type SqlMigrateRow struct {
	Ctx           context.Context
	TaskMode      string
	TaskFlow      string
	Smt           *task.SqlMigrateTask
	DatabaseS     database.IDatabase
	DatabaseT     database.IDatabase
	DatabaseTStmt *sql.Stmt
	DBCharsetS    string
	DBCharsetT    string
	SqlThreadT    int
	BatchSize     int
	CallTimeout   int
	SafeMode      bool
	ReadChan      chan []interface{}
	WriteChan     chan []interface{}
}

func (r *SqlMigrateRow) MigrateRead() error {
	defer close(r.ReadChan)
	startTime := time.Now()

	var execQuerySQL string

	sqlText, err := stringutil.Decrypt(r.Smt.SqlQueryS, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	switch {
	case strings.EqualFold(r.Smt.ConsistentReadS, "YES"):
		execQuerySQL = stringutil.StringBuilder(`SELECT * FROM (`, sqlText, `) AS OF SCN `, r.Smt.SnapshotPointS)
	default:
		execQuerySQL = sqlText
	}

	logger.Info("sql migrate task rows extractor starting",
		zap.String("task_name", r.Smt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_t", r.Smt.SchemaNameT),
		zap.String("table_name_t", r.Smt.TableNameT),
		zap.String("sql_query_s", execQuerySQL),
		zap.String("startTime", startTime.String()))

	err = r.DatabaseS.GetDatabaseTableChunkData(execQuerySQL, nil, r.BatchSize, r.CallTimeout, r.DBCharsetS, r.DBCharsetT, r.Smt.ColumnDetailO, r.ReadChan)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] source sql [%v] execute failed: %v", r.Smt.TaskName, r.TaskMode, r.TaskFlow, execQuerySQL, err)
	}

	endTime := time.Now()
	logger.Info("sql migrate task rows extractor finished",
		zap.String("task_name", r.Smt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_t", r.Smt.SchemaNameT),
		zap.String("table_name_t", r.Smt.TableNameT),
		zap.String("sql_query_s", execQuerySQL),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}

func (r *SqlMigrateRow) MigrateProcess() error {
	defer close(r.WriteChan)
	for batchRows := range r.ReadChan {
		r.WriteChan <- batchRows
	}
	return nil
}

func (r *SqlMigrateRow) MigrateApply() error {
	startTime := time.Now()

	logger.Info("sql migrate task applier starting",
		zap.String("task_name", r.Smt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_t", r.Smt.SchemaNameT),
		zap.String("table_name_t", r.Smt.TableNameT),
		zap.String("startTime", startTime.String()))

	columnDetailSCounts := len(stringutil.StringSplit(r.Smt.ColumnDetailS, constant.StringSeparatorComma))
	preArgNums := columnDetailSCounts * r.BatchSize

	g := &errgroup.Group{}
	g.SetLimit(r.SqlThreadT)

	for dataC := range r.WriteChan {
		vals := dataC
		g.Go(func() error {
			// prepare exec
			if len(vals) == preArgNums {
				_, err := r.DatabaseTStmt.ExecContext(r.Ctx, vals...)
				if err != nil {
					return fmt.Errorf("the task [%s] schema_name_t [%s] table_name_t [%s] task_mode [%s] task_flow [%v] tagert prepare sql stmt execute failed: %v", r.Smt.TaskName, r.Smt.SchemaNameT, r.Smt.TableNameT, r.TaskMode, r.TaskFlow, err)
				}
			} else {
				bathSize := len(vals) / columnDetailSCounts
				switch {
				case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL):
					sqlStr := GenMYSQLCompatibleDatabasePrepareStmt(r.Smt.SchemaNameT, r.Smt.TableNameT, r.Smt.SqlHintT, r.Smt.ColumnDetailT, bathSize, r.SafeMode)
					_, err := r.DatabaseT.ExecContext(r.Ctx, sqlStr, vals...)
					if err != nil {
						return fmt.Errorf("the task [%s] schema_name_t [%s] table_name_t [%s] task_mode [%s] task_flow [%v] tagert prepare sql stmt execute failed: %v", r.Smt.TaskName, r.Smt.SchemaNameT, r.Smt.TableNameT, r.TaskMode, r.TaskFlow, err)
					}
				default:
					return fmt.Errorf("the task_name [%s] schema_name_t [%s] table_name_t [%s] task_mode [%s] task_flow [%s] prepare sql stmt isn't support, please contact author", r.Smt.TaskName, r.Smt.SchemaNameT, r.Smt.TableNameT, r.TaskMode, r.TaskFlow)
				}

			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	logger.Info("sql migrate task applier finished",
		zap.String("task_name", r.Smt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_t", r.Smt.SchemaNameT),
		zap.String("table_name_t", r.Smt.TableNameT),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}
