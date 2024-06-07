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

	"github.com/golang/snappy"

	"golang.org/x/sync/errgroup"

	"github.com/wentaojin/dbms/database"

	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/utils/stringutil"

	"go.uber.org/zap"

	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"
)

type StmtMigrateRow struct {
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
	ReadChan      chan []interface{}
	WriteChan     chan []interface{}
}

func (r *StmtMigrateRow) MigrateRead() error {
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

	desChunkDetailS, err := stringutil.Decrypt(r.Dmt.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
	if err != nil {
		return err
	}
	chunkDetailS := stringutil.BytesToString(decChunkDetailS)

	switch {
	case strings.EqualFold(r.Dmt.ConsistentReadS, "YES") && strings.EqualFold(r.Dmt.SqlHintS, ""):
		originQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, strconv.FormatUint(r.Dmt.GlobalScnS, 10), ` WHERE `, chunkDetailS)
		execQuerySQL = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, strconv.FormatUint(r.Dmt.GlobalScnS, 10), ` WHERE `, chunkDetailS)
	case strings.EqualFold(r.Dmt.ConsistentReadS, "YES") && !strings.EqualFold(r.Dmt.SqlHintS, ""):
		originQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, strconv.FormatUint(r.Dmt.GlobalScnS, 10), ` WHERE `, chunkDetailS)
		execQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, strconv.FormatUint(r.Dmt.GlobalScnS, 10), ` WHERE `, chunkDetailS)
	case strings.EqualFold(r.Dmt.ConsistentReadS, "NO") && !strings.EqualFold(r.Dmt.SqlHintS, ""):
		originQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		execQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
	default:
		originQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		execQuerySQL = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
	}

	logger.Info("stmt migrate task chunk rows extractor starting",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", chunkDetailS),
		zap.String("sql_query_s", execQuerySQL),
		zap.String("origin_sql_s", originQuerySQL),
		zap.String("startTime", startTime.String()))

	err = r.DatabaseS.GetDatabaseTableChunkData(execQuerySQL, r.BatchSize, r.CallTimeout, r.DBCharsetS, r.DBCharsetT, r.Dmt.ColumnDetailO, r.ReadChan)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] source sql [%v] execute failed: %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, execQuerySQL, err)
	}

	endTime := time.Now()
	logger.Info("stmt migrate task chunk rows extractor finished",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", chunkDetailS),
		zap.String("sql_query_s", execQuerySQL),
		zap.String("origin_sql_s", originQuerySQL),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}

func (r *StmtMigrateRow) MigrateProcess() error {
	defer close(r.WriteChan)
	for batchRows := range r.ReadChan {
		r.WriteChan <- batchRows
	}
	return nil
}

func (r *StmtMigrateRow) MigrateApply() error {
	startTime := time.Now()

	desChunkDetailS, err := stringutil.Decrypt(r.Dmt.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
	if err != nil {
		return err
	}
	chunkDetailS := stringutil.BytesToString(decChunkDetailS)
	logger.Info("stmt migrate task chunk rows applier starting",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", chunkDetailS),
		zap.String("startTime", startTime.String()))

	columnDetailSCounts := len(stringutil.StringSplit(r.Dmt.ColumnDetailO, constant.StringSeparatorComma))
	argRowsNums := columnDetailSCounts * r.BatchSize

	g := &errgroup.Group{}
	g.SetLimit(r.SqlThreadT)

	for dataC := range r.WriteChan {
		vals := dataC
		g.Go(func() error {
			// prepare exec
			if len(vals) == argRowsNums {
				_, err := r.DatabaseTStmt.ExecContext(r.Ctx, vals...)
				if err != nil {
					return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] tagert prepare sql stmt execute params [%v] failed: %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, vals, err)
				}
			} else {
				bathSize := len(vals) / columnDetailSCounts
				switch {
				case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL):
					sqlStr := GenMYSQLCompatibleDatabasePrepareStmt(r.Dmt.SchemaNameT, r.Dmt.TableNameT, r.Dmt.SqlHintT, r.Dmt.ColumnDetailT, bathSize, r.SafeMode)
					_, err := r.DatabaseT.ExecContext(r.Ctx, sqlStr, vals...)
					if err != nil {
						return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] tagert prepare sql stmt execute params [%v] failed: %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, vals, err)
					}
				default:
					return fmt.Errorf("oracle current task [%s] schema [%s] task_mode [%s] task_flow [%s] prepare sql stmt isn't support, please contact author", r.Dmt.TaskName, r.Dmt.SchemaNameS, r.TaskMode, r.TaskFlow)
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	logger.Info("stmt migrate task chunk rows applier finished",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", chunkDetailS),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}
