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
	Ctx               context.Context
	TaskMode          string
	TaskFlow          string
	Dmt               *task.DataMigrateTask
	DatabaseS         database.IDatabase
	DatabaseT         database.IDatabase
	DatabaseTStmt     *sql.Stmt
	DBCharsetS        string
	DBCharsetT        string
	SqlThreadT        int
	BatchSize         int
	CallTimeout       int
	SafeMode          bool
	EnablePrepareStmt bool
	GarbledReplace    string
	ReadChan          chan []interface{}
	WriteChan         chan []interface{}
	Progress          *Progress
}

func (r *StmtMigrateRow) MigrateRead() error {
	defer close(r.ReadChan)
	startTime := time.Now()

	var (
		originQuerySQL string
		execQuerySQL   string
		columnDetailS  string
	)

	consistentReadS, err := strconv.ParseBool(r.Dmt.ConsistentReadS)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] parse consistent_read_s bool failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, err)
	}

	switch r.TaskFlow {
	case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB, constant.TaskFlowPostgresToMySQL, constant.TaskFlowPostgresToTiDB:
		convertRaw, err := stringutil.CharsetConvert([]byte(r.Dmt.ColumnDetailS), constant.CharsetUTF8MB4, r.DBCharsetS)
		if err != nil {
			return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
		}
		columnDetailS = stringutil.BytesToString(convertRaw)
	default:
		return fmt.Errorf("the task_flow [%s] task_mode [%s] isn't support, please contact author or reselect", r.TaskFlow, r.TaskMode)
	}

	desChunkDetailS, err := stringutil.Decrypt(r.Dmt.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
	if err != nil {
		return err
	}
	chunkDetailS := stringutil.BytesToString(decChunkDetailS)

	switch r.TaskFlow {
	case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB:
		switch {
		case consistentReadS && strings.EqualFold(r.Dmt.SqlHintS, ""):
			originQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
			execQuerySQL = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
		case consistentReadS && !strings.EqualFold(r.Dmt.SqlHintS, ""):
			originQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
			execQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
		case !consistentReadS && !strings.EqualFold(r.Dmt.SqlHintS, ""):
			originQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
			execQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		default:
			originQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
			execQuerySQL = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		}
	case constant.TaskFlowPostgresToMySQL, constant.TaskFlowPostgresToTiDB:
		switch {
		case consistentReadS && strings.EqualFold(r.Dmt.SqlHintS, ""):
			originQuerySQL = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointS, `'`, constant.StringSeparatorSemicolon, `SELECT `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
			execQuerySQL = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointS, `'`, constant.StringSeparatorSemicolon, `SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		case consistentReadS && !strings.EqualFold(r.Dmt.SqlHintS, ""):
			originQuerySQL = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointS, `'`, constant.StringSeparatorSemicolon, `SELECT `, r.Dmt.SqlHintS, ` `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
			execQuerySQL = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointS, `'`, constant.StringSeparatorSemicolon, `SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		case !consistentReadS && !strings.EqualFold(r.Dmt.SqlHintS, ""):
			originQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
			execQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		default:
			originQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
			execQuerySQL = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		}
	default:
		return fmt.Errorf("the task_flow [%s] task_mode [%s] isn't support, please contact author or reselect", r.TaskFlow, r.TaskMode)
	}

	logger.Info("data migrate task chunk rows extractor starting",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", chunkDetailS),
		zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
		zap.String("sql_query_s", execQuerySQL),
		zap.String("origin_sql_s", originQuerySQL),
		zap.String("startTime", startTime.String()))

	var queryCondArgsS []interface{}
	if strings.EqualFold(r.Dmt.ChunkDetailArgS, "") {
		queryCondArgsS = nil
	} else {
		err = stringutil.UnmarshalJSON([]byte(r.Dmt.ChunkDetailArgS), &queryCondArgsS)
		if err != nil {
			return fmt.Errorf("the database target query args [%v] running failed: [%v]", r.Dmt.ChunkDetailArgS, err)
		}
	}

	if r.EnablePrepareStmt && r.DatabaseTStmt != nil {
		err = r.DatabaseS.GetDatabaseTableStmtData(execQuerySQL, queryCondArgsS, r.BatchSize, r.CallTimeout, r.DBCharsetS, r.DBCharsetT, r.Dmt.ColumnDetailO, r.GarbledReplace, r.ReadChan)
		if err != nil {
			return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] source sql [%v] args [%v] execute failed: %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, execQuerySQL, queryCondArgsS, err)
		}
	} else {
		err = r.DatabaseS.GetDatabaseTableNonStmtData(r.TaskFlow, execQuerySQL, queryCondArgsS, r.BatchSize, r.CallTimeout, r.DBCharsetS, r.DBCharsetT, r.Dmt.ColumnDetailO, r.GarbledReplace, r.ReadChan)
		if err != nil {
			return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] source sql [%v] args [%v] execute failed: %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, execQuerySQL, queryCondArgsS, err)
		}
	}

	endTime := time.Now()
	logger.Info("data migrate task chunk rows extractor finished",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", chunkDetailS),
		zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
		zap.String("sql_query_s", execQuerySQL),
		zap.String("origin_sql_s", originQuerySQL),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}

func (r *StmtMigrateRow) MigrateProcess() error {
	defer close(r.WriteChan)

	columnDetailSCounts := len(stringutil.StringSplit(r.Dmt.ColumnDetailO, constant.StringSeparatorComma))
	argRowsNums := columnDetailSCounts * r.BatchSize

	for batchRows := range r.ReadChan {
		if r.EnablePrepareStmt && r.DatabaseTStmt != nil {
			valsLen := len(batchRows)
			if valsLen == argRowsNums {
				r.Progress.UpdateTableRowsReaded(uint64(r.BatchSize))
			} else {
				bathSize := valsLen / columnDetailSCounts
				r.Progress.UpdateTableRowsReaded(uint64(bathSize))
			}
		} else {
			valsLen := len(batchRows[0].([]string))
			r.Progress.UpdateTableRowsReaded(uint64(valsLen))
		}
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
	logger.Info("data migrate task chunk rows applier starting",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", chunkDetailS),
		zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
		zap.String("startTime", startTime.String()))

	columnDetailSCounts := len(stringutil.StringSplit(r.Dmt.ColumnDetailO, constant.StringSeparatorComma))
	argRowsNums := columnDetailSCounts * r.BatchSize

	g := &errgroup.Group{}
	g.SetLimit(r.SqlThreadT)

	for dataC := range r.WriteChan {
		vals := dataC
		g.Go(func() error {
			if r.EnablePrepareStmt && r.DatabaseTStmt != nil {
				valsLen := len(vals)
				// prepare exec
				if valsLen == argRowsNums {
					_, err := r.DatabaseTStmt.ExecContext(r.Ctx, vals...)
					if err != nil {
						return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] tagert prepare sql stmt execute params [%v] failed: %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, vals, err)
					}
					r.Progress.UpdateTableRowsProcessed(uint64(r.BatchSize))
				} else {
					bathSize := valsLen / columnDetailSCounts
					switch r.TaskFlow {
					case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL, constant.TaskFlowPostgresToTiDB, constant.TaskFlowPostgresToMySQL:
						sqlStr := GenMYSQLCompatibleDatabasePrepareStmt(r.Dmt.SchemaNameT, r.Dmt.TableNameT, r.Dmt.SqlHintT, r.Dmt.ColumnDetailT, bathSize, r.SafeMode)
						_, err := r.DatabaseT.ExecContext(r.Ctx, sqlStr, vals...)
						if err != nil {
							return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] tagert prepare sql stmt execute params [%v] failed: %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, vals, err)
						}
					default:
						return fmt.Errorf("the task_name [%s] schema_name_s [%s] task_mode [%s] task_flow [%s] prepare sql stmt isn't support, please contact author", r.Dmt.TaskName, r.Dmt.SchemaNameS, r.TaskMode, r.TaskFlow)
					}
					r.Progress.UpdateTableRowsProcessed(uint64(bathSize))
				}
			} else {
				switch r.TaskFlow {
				case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL, constant.TaskFlowPostgresToTiDB, constant.TaskFlowPostgresToMySQL:
					var newColumnDetailT []string
					for _, c := range stringutil.StringSplit(r.Dmt.ColumnDetailT, constant.StringSeparatorComma) {
						newColumnDetailT = append(newColumnDetailT, stringutil.TrimIfBothExist(c, '`'))
					}

					valsLen := len(vals[0].([]string))
					sqlStr := GenMYSQLCompatibleDatabaseInsertStmtSQL(
						r.Dmt.SchemaNameT,
						r.Dmt.TableNameT,
						r.Dmt.SqlHintT,
						newColumnDetailT,
						vals[0].([]string),
						r.SafeMode,
					)
					_, err := r.DatabaseT.ExecContext(r.Ctx, sqlStr)
					if err != nil {
						return fmt.Errorf("the task [%s] schema_name_s [%s] table_name_s [%s] task_mode [%s] task_flow [%v] tagert sql [%s] execute failed: %v", r.Dmt.TaskName, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.TaskMode, r.TaskFlow, sqlStr, err)
					}
					r.Progress.UpdateTableRowsProcessed(uint64(valsLen))
				default:
					return fmt.Errorf("the task_name [%s] schema_name_t [%s] table_name_t [%s] task_mode [%s] task_flow [%s] sql query isn't support, please contact author", r.Dmt.TaskName, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.TaskMode, r.TaskFlow)
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
		zap.String("chunk_detail_s", chunkDetailS),
		zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}
