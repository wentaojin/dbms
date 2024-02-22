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
package taskflow

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/wentaojin/dbms/database"

	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/utils/stringutil"

	"go.uber.org/zap"

	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"
)

type DataMigrateRow struct {
	Ctx           context.Context
	TaskMode      string
	TaskFlow      string
	Dmt           *task.DataMigrateTask
	DatabaseS     database.IDatabase
	DatabaseT     database.IDatabase
	DatabaseTStmt *sql.Stmt
	DBCharsetS    string
	DBCharsetT    string
	SqlThreadT    int
	BatchSize     int
	CallTimeout   int
	SafeMode      bool
	ReadChan      chan []map[string]interface{}
	WriteChan     chan []interface{}
}

func (r *DataMigrateRow) MigrateRead() error {
	defer close(r.ReadChan)
	startTime := time.Now()

	var (
		originQuerySQL string
		execQuerySQL   string
		columnDetailS  string
	)

	convertRaw, err := stringutil.CharsetConvert([]byte(r.Dmt.ColumnDetailS), constant.CharsetUTF8MB4, r.DBCharsetS)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	columnDetailS = stringutil.BytesToString(convertRaw)

	switch {
	case strings.EqualFold(r.Dmt.ConsistentReadS, "YES") && strings.EqualFold(r.Dmt.SqlHintS, ""):
		originQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, strconv.FormatUint(r.Dmt.GlobalScnS, 10), ` WHERE `, r.Dmt.ChunkDetailS)
		execQuerySQL = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, strconv.FormatUint(r.Dmt.GlobalScnS, 10), ` WHERE `, r.Dmt.ChunkDetailS)
	case strings.EqualFold(r.Dmt.ConsistentReadS, "YES") && !strings.EqualFold(r.Dmt.SqlHintS, ""):
		originQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, strconv.FormatUint(r.Dmt.GlobalScnS, 10), ` WHERE `, r.Dmt.ChunkDetailS)
		execQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, strconv.FormatUint(r.Dmt.GlobalScnS, 10), ` WHERE `, r.Dmt.ChunkDetailS)
	case strings.EqualFold(r.Dmt.ConsistentReadS, "NO") && !strings.EqualFold(r.Dmt.SqlHintS, ""):
		originQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, r.Dmt.ChunkDetailS)
		execQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, r.Dmt.ChunkDetailS)
	default:
		originQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, r.Dmt.ChunkDetailS)
		execQuerySQL = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, r.Dmt.ChunkDetailS)
	}

	logger.Info("data migrate task chunk rows extractor starting",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", r.Dmt.ChunkDetailS),
		zap.String("sql_query_s", execQuerySQL),
		zap.String("origin_sql_s", originQuerySQL),
		zap.String("startTime", startTime.String()))

	err = r.DatabaseS.QueryDatabaseTableChunkData(execQuerySQL, r.BatchSize, r.CallTimeout, r.DBCharsetS, r.DBCharsetT, r.ReadChan)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] source sql [%v] execute failed: %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, execQuerySQL, err)
	}

	endTime := time.Now()
	logger.Info("data migrate task chunk rows extractor finished",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", r.Dmt.ChunkDetailS),
		zap.String("sql_query_s", execQuerySQL),
		zap.String("origin_sql_s", originQuerySQL),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}

func (r *DataMigrateRow) MigrateProcess() error {
	defer close(r.WriteChan)

	for dataC := range r.ReadChan {
		var batchRows []interface{}
		for _, dMap := range dataC {
			// get value order by column
			var rowTemps []interface{}
			columnDetails := stringutil.StringSplit(r.Dmt.ColumnDetailS, constant.StringSeparatorComma)
			for _, c := range columnDetails {
				if val, ok := dMap[stringutil.StringTrim(c, constant.StringSeparatorQuotationMarks)]; ok {
					rowTemps = append(rowTemps, val)
				}
			}
			if len(rowTemps) != len(columnDetails) {
				return fmt.Errorf("oracle current task [%s] schema [%s] task_mode [%s] taskflow [%s] data migrate column counts vs data counts isn't match, please contact author or reselect", r.Dmt.TaskName, r.Dmt.SchemaNameS, r.TaskMode, r.TaskFlow)
			} else {
				batchRows = append(batchRows, rowTemps...)
			}
		}
		r.WriteChan <- batchRows
	}
	return nil
}

func (r *DataMigrateRow) MigrateApply() error {
	startTime := time.Now()

	logger.Info("data migrate task chunk rows applier starting",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", r.Dmt.ChunkDetailS),
		zap.String("startTime", startTime.String()))

	columnDetailSCounts := len(stringutil.StringSplit(r.Dmt.ColumnDetailS, constant.StringSeparatorComma))
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
					return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] tagert prepare sql stmt execute failed: %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, err)
				}
			} else {
				bathSize := len(vals) / columnDetailSCounts
				sqlStr := GenMYSQLCompatibleDatabasePrepareStmt(r.Dmt.SchemaNameT, r.Dmt.TableNameT, r.Dmt.ColumnDetailT, bathSize, r.SafeMode)
				_, err := r.DatabaseT.ExecContext(r.Ctx, sqlStr, vals...)
				if err != nil {
					return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] tagert prepare sql stmt execute failed: %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, err)
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	logger.Info("data migrate task chunk rows applier finished",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", r.Dmt.ChunkDetailS),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}
