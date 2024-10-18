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
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/golang/snappy"

	"github.com/wentaojin/dbms/model"

	"golang.org/x/sync/errgroup"

	"github.com/wentaojin/dbms/database"

	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/utils/stringutil"

	"go.uber.org/zap"

	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"
)

type DataCompareRow struct {
	Ctx            context.Context
	StartTime      time.Time
	TaskMode       string
	TaskFlow       string
	Dmt            *task.DataCompareTask
	DatabaseS      database.IDatabase
	DatabaseT      database.IDatabase
	BatchSize      int
	WriteThread    int
	CallTimeout    int
	DBCharsetS     string
	DBCharsetT     string
	RepairStmtFlow string
	Separator      string
}

func (r *DataCompareRow) CompareMethod() string {
	return r.Dmt.CompareMethod
}

func (r *DataCompareRow) CompareRows() error {
	startTime := time.Now()

	var (
		execQueryS, execQueryT                                   string
		columnDetailS, columnDetailT, chunkDetailS, chunkDetailT string
		queryCondArgsS, queryCondArgsT                           []interface{}
	)

	desChunkDetailS, err := stringutil.Decrypt(r.Dmt.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
	if err != nil {
		return err
	}
	desChunkDetailS = stringutil.BytesToString(decChunkDetailS)

	desChunkDetailT, err := stringutil.Decrypt(r.Dmt.ChunkDetailT, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	decChunkDetailT, err := snappy.Decode(nil, []byte(desChunkDetailT))
	if err != nil {
		return err
	}

	desChunkDetailT = stringutil.BytesToString(decChunkDetailT)

	convertRaw, err := stringutil.CharsetConvert([]byte(r.Dmt.ColumnDetailS), constant.CharsetUTF8MB4, r.DBCharsetS)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	columnDetailS = stringutil.BytesToString(convertRaw)
	convertRaw, err = stringutil.CharsetConvert([]byte(desChunkDetailS), constant.CharsetUTF8MB4, r.DBCharsetS)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	chunkDetailS = stringutil.BytesToString(convertRaw)

	convertRaw, err = stringutil.CharsetConvert([]byte(r.Dmt.ColumnDetailT), constant.CharsetUTF8MB4, r.DBCharsetT)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailT, err)
	}
	columnDetailT = stringutil.BytesToString(convertRaw)
	convertRaw, err = stringutil.CharsetConvert([]byte(desChunkDetailT), constant.CharsetUTF8MB4, r.DBCharsetT)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	chunkDetailT = stringutil.BytesToString(convertRaw)

	dbTypeSli := stringutil.StringSplit(r.TaskFlow, constant.StringSeparatorAite)

	switch stringutil.StringUpper(dbTypeSli[0]) {
	case constant.DatabaseTypeOracle:
		if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
		} else if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
		} else if strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		}
	case constant.DatabaseTypeTiDB:
		if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` AS OF TIMESTAMP '", r.Dmt.SnapshotPointS, "' WHERE ", chunkDetailS)
		} else if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintS, " ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` AS OF TIMESTAMP '", r.Dmt.SnapshotPointS, "' WHERE ", chunkDetailS)
		} else if strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintS, " ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` WHERE ", chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder("SELECT ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` WHERE ", chunkDetailS)
		}
	case constant.DatabaseTypeMySQL:
		if !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintS, " ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` WHERE ", chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder("SELECT ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` WHERE ", chunkDetailS)
		}
	case constant.DatabaseTypePostgresql:
		if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointS, `'`, constant.StringSeparatorSemicolon, `SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointS, `'`, constant.StringSeparatorSemicolon, `SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else if strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		}
	default:
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] db_type_s [%s] is not supported, please contact author or reselect", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, dbTypeSli[0])
	}

	switch stringutil.StringUpper(dbTypeSli[1]) {
	case constant.DatabaseTypeOracle:
		if !strings.EqualFold(r.Dmt.SqlHintT, "") && strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else if !strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" AS OF SCN `, r.Dmt.SnapshotPointT, ` WHERE `, chunkDetailT)
		} else if strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" AS OF SCN `, r.Dmt.SnapshotPointT, ` WHERE `, chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		}
	case constant.DatabaseTypeTiDB:
		if !strings.EqualFold(r.Dmt.SqlHintT, "") && strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintT, " ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` WHERE ", chunkDetailT)
		} else if !strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintT, " ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` AS OF TIMESTAMP '", r.Dmt.SnapshotPointT, "' WHERE ", chunkDetailT)
		} else if strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` AS OF TIMESTAMP '", r.Dmt.SnapshotPointT, "' WHERE ", chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder("SELECT ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` WHERE ", chunkDetailT)
		}
	case constant.DatabaseTypeMySQL:
		if !strings.EqualFold(r.Dmt.SqlHintT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintT, " ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` WHERE ", chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder("SELECT ", columnDetailT, " FROM ", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` WHERE ", chunkDetailT)
		}
	case constant.DatabaseTypePostgresql:
		if !strings.EqualFold(r.Dmt.SqlHintT, "") && strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else if !strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointT, `'`, constant.StringSeparatorSemicolon, `SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else if strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointT, `'`, constant.StringSeparatorSemicolon, `SELECT `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		}
	default:
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] db_type_t [%s] is not supported, please contact author or reselect", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, dbTypeSli[1])
	}

	logger.Info("data compare task chunk compare starting",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("compare_method", r.Dmt.CompareMethod),
		zap.String("chunk_id", r.Dmt.ChunkID),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
		zap.String("startTime", startTime.String()))

	if strings.EqualFold(r.Dmt.ChunkDetailArgS, "") {
		queryCondArgsS = nil
	} else {
		err = stringutil.UnmarshalJSON([]byte(r.Dmt.ChunkDetailArgS), &queryCondArgsS)
		if err != nil {
			return fmt.Errorf("the database source query args [%v] running failed: [%v]", r.Dmt.ChunkDetailArgS, err)
		}
	}

	if strings.EqualFold(r.Dmt.ChunkDetailArgT, "") {
		queryCondArgsT = nil
	} else {
		err = stringutil.UnmarshalJSON([]byte(r.Dmt.ChunkDetailArgT), &queryCondArgsT)
		if err != nil {
			return fmt.Errorf("the database target query args [%v] running failed: [%v]", r.Dmt.ChunkDetailArgT, err)
		}
	}

	resultSM := make(chan string, 1)
	resultTM := make(chan string, 1)

	g, ctx := errgroup.WithContext(r.Ctx)

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
			streamTime := time.Now()
			_, resultS, err := r.DatabaseS.GetDatabaseTableCompareRow(execQueryS, queryCondArgsS...)
			if err != nil {
				return fmt.Errorf("the database source query sql [%v] args [%v] running failed: [%v]", execQueryS, queryCondArgsS, err)
			}
			resultSM <- resultS[0]["ROWSCOUNT"]

			logger.Info("data compare task chunk upstream compare sql",
				zap.String("task_name", r.Dmt.TaskName),
				zap.String("task_mode", r.TaskMode),
				zap.String("task_flow", r.TaskFlow),
				zap.String("schema_name_s", r.Dmt.SchemaNameS),
				zap.String("table_name_s", r.Dmt.TableNameS),
				zap.String("compare_method", r.Dmt.CompareMethod),
				zap.String("chunk_id", r.Dmt.ChunkID),
				zap.String("chunk_detail_s", desChunkDetailS),
				zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
				zap.String("chunk_result_s", resultS[0]["ROWSCOUNT"]),
				zap.String("upstream_database_time", time.Now().Sub(streamTime).String()))
			return nil
		}
	})

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
			streamTime := time.Now()
			_, resultT, err := r.DatabaseT.GetDatabaseTableCompareRow(execQueryT, queryCondArgsT...)
			if err != nil {
				return fmt.Errorf("the database source target sql [%v] args [%v] running failed: [%v]", execQueryT, queryCondArgsT, err)
			}
			resultTM <- resultT[0]["ROWSCOUNT"]
			logger.Info("data compare task chunk downstream compare sql",
				zap.String("task_name", r.Dmt.TaskName),
				zap.String("task_mode", r.TaskMode),
				zap.String("task_flow", r.TaskFlow),
				zap.String("compare_method", r.Dmt.CompareMethod),
				zap.String("chunk_id", r.Dmt.ChunkID),
				zap.String("schema_name_t", r.Dmt.SchemaNameT),
				zap.String("table_name_t", r.Dmt.TableNameT),
				zap.String("chunk_detail_t", desChunkDetailT),
				zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
				zap.String("chunk_result_t", resultT[0]["ROWSCOUNT"]),
				zap.String("downstream_database_time", time.Now().Sub(streamTime).String()))
			return nil
		}
	})

	if err = g.Wait(); err != nil {
		return err
	}

	resultStrS := <-resultSM
	resultStrT := <-resultTM

	endTime := time.Now()

	resultS, err := decimal.NewFromString(resultStrS)
	if err != nil {
		return fmt.Errorf("parse the database source rowcounts [%s] failed: %v", err, resultStrS)
	}

	resultT, err := decimal.NewFromString(resultStrT)
	if err != nil {
		return fmt.Errorf("parse the database target rowcounts [%s] failed: %v", err, resultStrT)
	}

	if resultS.Equal(resultT) {
		logger.Info("data compare task chunk compare is equaled",
			zap.String("task_name", r.Dmt.TaskName),
			zap.String("task_mode", r.TaskMode),
			zap.String("task_flow", r.TaskFlow),
			zap.String("schema_name_s", r.Dmt.SchemaNameS),
			zap.String("table_name_s", r.Dmt.TableNameS),
			zap.String("compare_method", r.Dmt.CompareMethod),
			zap.String("chunk_id", r.Dmt.ChunkID),
			zap.String("chunk_detail_s", desChunkDetailS),
			zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
			zap.String("schema_name_t", r.Dmt.SchemaNameT),
			zap.String("table_name_t", r.Dmt.TableNameT),
			zap.String("chunk_detail_t", desChunkDetailT),
			zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
			zap.String("table_rows_s", resultStrS),
			zap.String("table_rows_t", resultStrT),
			zap.String("cost", endTime.Sub(startTime).String()))

		errW := model.Transaction(r.Ctx, func(txnCtx context.Context) error {
			_, err = model.GetIDataCompareTaskRW().UpdateDataCompareTask(txnCtx,
				&task.DataCompareTask{TaskName: r.Dmt.TaskName, SchemaNameS: r.Dmt.SchemaNameS, TableNameS: r.Dmt.TableNameS, ChunkID: r.Dmt.ChunkID},
				map[string]interface{}{
					"TaskStatus": constant.TaskDatabaseStatusEqual,
					"Duration":   fmt.Sprintf("%f", time.Now().Sub(r.StartTime).Seconds()),
				})
			if err != nil {
				return err
			}
			_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
				TaskName:    r.Dmt.TaskName,
				SchemaNameS: r.Dmt.SchemaNameS,
				TableNameS:  r.Dmt.TableNameS,
				LogDetail: fmt.Sprintf("%v [%v] data compare task [%v] taskflow [%v] source table [%v.%v] chunk_id [%s] chunk [%s] success",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(constant.TaskModeDataCompare),
					r.Dmt.TaskName,
					r.TaskFlow,
					r.Dmt.SchemaNameS,
					r.Dmt.TableNameS,
					r.Dmt.ChunkID,
					r.Dmt.ChunkDetailS),
			})
			if err != nil {
				return err
			}
			return nil
		})
		if errW != nil {
			return errW
		}

		logger.Info("data compare task chunk compare finished",
			zap.String("task_name", r.Dmt.TaskName),
			zap.String("task_mode", r.TaskMode),
			zap.String("task_flow", r.TaskFlow),
			zap.String("schema_name_s", r.Dmt.SchemaNameS),
			zap.String("table_name_s", r.Dmt.TableNameS),
			zap.String("compare_method", r.Dmt.CompareMethod),
			zap.String("chunk_id", r.Dmt.ChunkID),
			zap.String("chunk_detail_s", desChunkDetailS),
			zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
			zap.String("schema_name_t", r.Dmt.SchemaNameT),
			zap.String("table_name_t", r.Dmt.TableNameT),
			zap.String("chunk_detail_t", desChunkDetailT),
			zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
			zap.String("cost", time.Now().Sub(startTime).String()))

		return nil
	}

	logger.Info("data compare task chunk compare isn't equaled",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("compare_method", r.Dmt.CompareMethod),
		zap.String("chunk_id", r.Dmt.ChunkID),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
		zap.String("table_rows_s", resultStrS),
		zap.String("table_rows_t", resultStrT),
		zap.String("cost", endTime.Sub(startTime).String()))

	errW := model.Transaction(r.Ctx, func(txnCtx context.Context) error {
		_, err = model.GetIDataCompareTaskRW().UpdateDataCompareTask(txnCtx,
			&task.DataCompareTask{TaskName: r.Dmt.TaskName, SchemaNameS: r.Dmt.SchemaNameS, TableNameS: r.Dmt.TableNameS, ChunkID: r.Dmt.ChunkID},
			map[string]interface{}{
				"TaskStatus": constant.TaskDatabaseStatusNotEqual,
				"Duration":   fmt.Sprintf("%f", time.Now().Sub(r.StartTime).Seconds()),
			})
		if err != nil {
			return err
		}
		_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
			TaskName:    r.Dmt.TaskName,
			SchemaNameS: r.Dmt.SchemaNameS,
			TableNameS:  r.Dmt.TableNameS,
			LogDetail: fmt.Sprintf("%v [%v] data compare task [%v] taskflow [%v] source table [%v.%v] chunk_id [%s] chunk [%s] success",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeDataCompare),
				r.Dmt.TaskName,
				r.TaskFlow,
				r.Dmt.SchemaNameS,
				r.Dmt.TableNameS,
				r.Dmt.ChunkID,
				r.Dmt.ChunkDetailS),
		})
		if err != nil {
			return err
		}

		_, err = model.GetIDataCompareResultRW().CreateDataCompareResult(txnCtx, &task.DataCompareResult{
			TaskName:     r.Dmt.TaskName,
			SchemaNameS:  r.Dmt.SchemaNameS,
			TableNameS:   r.Dmt.TableNameS,
			SchemaNameT:  r.Dmt.SchemaNameT,
			TableNameT:   r.Dmt.TableNameT,
			ChunkID:      r.Dmt.ChunkID,
			ChunkDetailS: r.Dmt.ChunkDetailS,
			FixStmtType:  constant.DataCompareFixStmtTypeRows,
			FixDetailT:   fmt.Sprintf("rowCountsS:%v rowCountsT:%v", resultS, resultT),
		})
		if err != nil {
			return err
		}
		return nil
	})
	if errW != nil {
		return errW
	}

	logger.Info("data compare task chunk compare finished",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("compare_method", r.Dmt.CompareMethod),
		zap.String("chunk_id", r.Dmt.ChunkID),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}

func (r *DataCompareRow) CompareMd5ORCrc32() error {
	startTime := time.Now()

	var (
		execQueryS, execQueryT                                   string
		columnDetailS, columnDetailT, chunkDetailS, chunkDetailT string
		queryCondArgsS, queryCondArgsT                           []interface{}
	)

	desChunkDetailS, err := stringutil.Decrypt(r.Dmt.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
	if err != nil {
		return err
	}
	desChunkDetailS = stringutil.BytesToString(decChunkDetailS)

	desChunkDetailT, err := stringutil.Decrypt(r.Dmt.ChunkDetailT, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	decChunkDetailT, err := snappy.Decode(nil, []byte(desChunkDetailT))
	if err != nil {
		return err
	}

	desChunkDetailT = stringutil.BytesToString(decChunkDetailT)

	convertRaw, err := stringutil.CharsetConvert([]byte(r.Dmt.ColumnDetailS), constant.CharsetUTF8MB4, r.DBCharsetS)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	columnDetailS = stringutil.BytesToString(convertRaw)
	convertRaw, err = stringutil.CharsetConvert([]byte(desChunkDetailS), constant.CharsetUTF8MB4, r.DBCharsetS)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	chunkDetailS = stringutil.BytesToString(convertRaw)

	convertRaw, err = stringutil.CharsetConvert([]byte(r.Dmt.ColumnDetailT), constant.CharsetUTF8MB4, r.DBCharsetT)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailT, err)
	}
	columnDetailT = stringutil.BytesToString(convertRaw)
	convertRaw, err = stringutil.CharsetConvert([]byte(desChunkDetailT), constant.CharsetUTF8MB4, r.DBCharsetT)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	chunkDetailT = stringutil.BytesToString(convertRaw)

	dbTypeSli := stringutil.StringSplit(r.TaskFlow, constant.StringSeparatorAite)

	switch stringutil.StringUpper(dbTypeSli[0]) {
	case constant.DatabaseTypeOracle:
		if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
		} else if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
		} else if strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		}
		if strings.EqualFold(r.Dmt.CompareMethod, constant.DataCompareMethodDatabaseCheckMD5) {
			execQueryS = fmt.Sprintf(`SELECT
	TO_CHAR(NVL(TO_NUMBER(SUM(TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 1, 8),'xxxxxxxx')+
	TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 9, 8),'xxxxxxxx')+
	TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 17, 8),'xxxxxxxx')+ 
	TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 25, 8),'xxxxxxxx'))),0)) AS ROWSCHECKSUM
FROM
	(%v) subq`, execQueryS)
		}
	case constant.DatabaseTypeTiDB:
		if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` AS OF TIMESTAMP '", r.Dmt.SnapshotPointS, "' WHERE ", chunkDetailS)
		} else if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintS, " ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` AS OF TIMESTAMP '", r.Dmt.SnapshotPointS, "' WHERE ", chunkDetailS)
		} else if strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintS, " ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` WHERE ", chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder("SELECT ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` WHERE ", chunkDetailS)
		}
		if strings.EqualFold(r.Dmt.CompareMethod, constant.DataCompareMethodDatabaseCheckMD5) {
			execQueryS = fmt.Sprintf(`
SELECT
	 CAST(IFNULL(SUM(CONV(SUBSTRING(subq.ROWSCHECKSUM, 1, 8),16,10)+ 
	 CONV(SUBSTRING(subq.ROWSCHECKSUM, 9, 8),16,10)+
	 CONV(SUBSTRING(subq.ROWSCHECKSUM, 17, 8),16,10)+
	 CONV(SUBSTRING(subq.ROWSCHECKSUM, 25, 8),16,10)),0) AS CHAR) AS ROWSCHECKSUM
FROM
	(%v) subq`, execQueryS)
		}
	case constant.DatabaseTypeMySQL:
		if !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintS, " ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` WHERE ", chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder("SELECT ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` WHERE ", chunkDetailS)
		}
		if strings.EqualFold(r.Dmt.CompareMethod, constant.DataCompareMethodDatabaseCheckMD5) {
			execQueryS = fmt.Sprintf(`
SELECT
	 CAST(IFNULL(SUM(CONV(SUBSTRING(subq.ROWSCHECKSUM, 1, 8),16,10)+ 
	 CONV(SUBSTRING(subq.ROWSCHECKSUM, 9, 8),16,10)+
	 CONV(SUBSTRING(subq.ROWSCHECKSUM, 17, 8),16,10)+
	 CONV(SUBSTRING(subq.ROWSCHECKSUM, 25, 8),16,10)),0) AS CHAR) AS ROWSCHECKSUM
FROM
	(%v) subq`, execQueryS)
		}
	case constant.DatabaseTypePostgresql:
		if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointS, `'`, constant.StringSeparatorSemicolon, `SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointS, `'`, constant.StringSeparatorSemicolon, `SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else if strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		}
		if strings.EqualFold(r.Dmt.CompareMethod, constant.DataCompareMethodDatabaseCheckMD5) {
			execQueryS = fmt.Sprintf(`SELECT
	TO_CHAR(COALESCE(TO_NUMBER(SUM(TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 1, 8),'99999999')+
	TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 9, 8),'99999999')+
	TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 17, 8),'99999999')+ 
	TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 25, 8),'99999999'))),0)) AS ROWSCHECKSUM
FROM
	(%v) subq`, execQueryS)
		}
	default:
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] db_type_s [%s] is not supported, please contact author or reselect", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, dbTypeSli[0])
	}

	switch stringutil.StringUpper(dbTypeSli[1]) {
	case constant.DatabaseTypeOracle:
		if !strings.EqualFold(r.Dmt.SqlHintT, "") && strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else if !strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" AS OF SCN `, r.Dmt.SnapshotPointT, ` WHERE `, chunkDetailT)
		} else if strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" AS OF SCN `, r.Dmt.SnapshotPointT, ` WHERE `, chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		}
		if strings.EqualFold(r.Dmt.CompareMethod, constant.DataCompareMethodDatabaseCheckMD5) {
			execQueryT = fmt.Sprintf(`SELECT
	TO_CHAR(NVL(TO_NUMBER(SUM(TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 1, 8),'xxxxxxxx')+
	TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 9, 8),'xxxxxxxx')+
	TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 17, 8),'xxxxxxxx')+ 
	TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 25, 8),'xxxxxxxx'))),0)) AS ROWSCHECKSUM
FROM
	(%v) subq`, execQueryT)
		}
	case constant.DatabaseTypeTiDB:
		if !strings.EqualFold(r.Dmt.SqlHintT, "") && strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintT, " ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` WHERE ", chunkDetailT)
		} else if !strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintT, " ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` AS OF TIMESTAMP '", r.Dmt.SnapshotPointT, "' WHERE ", chunkDetailT)
		} else if strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` AS OF TIMESTAMP '", r.Dmt.SnapshotPointT, "' WHERE ", chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder("SELECT ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` WHERE ", chunkDetailT)
		}
		if strings.EqualFold(r.Dmt.CompareMethod, constant.DataCompareMethodDatabaseCheckMD5) {
			execQueryT = fmt.Sprintf(`
SELECT
	 CAST(IFNULL(SUM(CONV(SUBSTRING(subq.ROWSCHECKSUM, 1, 8),16,10)+ 
	 CONV(SUBSTRING(subq.ROWSCHECKSUM, 9, 8),16,10)+
	 CONV(SUBSTRING(subq.ROWSCHECKSUM, 17, 8),16,10)+
	 CONV(SUBSTRING(subq.ROWSCHECKSUM, 25, 8),16,10)),0) AS CHAR) AS ROWSCHECKSUM
FROM
	(%v) subq`, execQueryT)
		}
	case constant.DatabaseTypeMySQL:
		if !strings.EqualFold(r.Dmt.SqlHintT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintT, " ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` WHERE ", chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder("SELECT ", columnDetailT, " FROM ", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` WHERE ", chunkDetailT)
		}
		if strings.EqualFold(r.Dmt.CompareMethod, constant.DataCompareMethodDatabaseCheckMD5) {
			execQueryT = fmt.Sprintf(`
SELECT
	 CAST(IFNULL(SUM(CONV(SUBSTRING(subq.ROWSCHECKSUM, 1, 8),16,10)+ 
	 CONV(SUBSTRING(subq.ROWSCHECKSUM, 9, 8),16,10)+
	 CONV(SUBSTRING(subq.ROWSCHECKSUM, 17, 8),16,10)+
	 CONV(SUBSTRING(subq.ROWSCHECKSUM, 25, 8),16,10)),0) AS CHAR) AS ROWSCHECKSUM
FROM
	(%v) subq`, execQueryT)
		}
	case constant.DatabaseTypePostgresql:
		if !strings.EqualFold(r.Dmt.SqlHintT, "") && strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else if !strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointT, `'`, constant.StringSeparatorSemicolon, `SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else if strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointT, `'`, constant.StringSeparatorSemicolon, `SELECT `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		}
		if strings.EqualFold(r.Dmt.CompareMethod, constant.DataCompareMethodDatabaseCheckMD5) {
			execQueryT = fmt.Sprintf(`SELECT
	TO_CHAR(COALESCE(TO_NUMBER(SUM(TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 1, 8),'99999999')+
	TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 9, 8),'99999999')+
	TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 17, 8),'99999999')+ 
	TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 25, 8),'99999999'))),0)) AS ROWSCHECKSUM
FROM
	(%v) subq`, execQueryT)
		}
	default:
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] db_type_t [%s] is not supported, please contact author or reselect", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, dbTypeSli[1])
	}

	logger.Info("data compare task chunk compare starting",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("compare_method", r.Dmt.CompareMethod),
		zap.String("chunk_id", r.Dmt.ChunkID),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
		zap.String("startTime", startTime.String()))
	if strings.EqualFold(r.Dmt.ChunkDetailArgS, "") {
		queryCondArgsS = nil
	} else {
		err = stringutil.UnmarshalJSON([]byte(r.Dmt.ChunkDetailArgS), &queryCondArgsS)
		if err != nil {
			return fmt.Errorf("the database source query args [%v] running failed: [%v]", r.Dmt.ChunkDetailArgS, err)
		}
	}

	if strings.EqualFold(r.Dmt.ChunkDetailArgT, "") {
		queryCondArgsT = nil
	} else {
		err = stringutil.UnmarshalJSON([]byte(r.Dmt.ChunkDetailArgT), &queryCondArgsT)
		if err != nil {
			return fmt.Errorf("the database target query args [%v] running failed: [%v]", r.Dmt.ChunkDetailArgT, err)
		}
	}

	resultSM := make(chan string, 1)
	resultTM := make(chan string, 1)

	g, ctx := errgroup.WithContext(r.Ctx)

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
			streamTime := time.Now()
			_, resultS, err := r.DatabaseS.GetDatabaseTableCompareRow(execQueryS, queryCondArgsS...)
			if err != nil {
				return fmt.Errorf("the database source query sql [%v] args [%v] running failed: [%v]", execQueryS, queryCondArgsS, err)
			}
			resultSM <- resultS[0]["ROWSCHECKSUM"]
			logger.Info("data compare task chunk upstream compare sql",
				zap.String("task_name", r.Dmt.TaskName),
				zap.String("task_mode", r.TaskMode),
				zap.String("task_flow", r.TaskFlow),
				zap.String("schema_name_s", r.Dmt.SchemaNameS),
				zap.String("table_name_s", r.Dmt.TableNameS),
				zap.String("compare_method", r.Dmt.CompareMethod),
				zap.String("chunk_detail_s", desChunkDetailS),
				zap.String("chunk_id", r.Dmt.ChunkID),
				zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
				zap.String("chunk_result_s", resultS[0]["ROWSCHECKSUM"]),
				zap.String("upstream_database_time", time.Now().Sub(streamTime).String()))
			return nil
		}
	})

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
			streamTime := time.Now()
			_, resultT, err := r.DatabaseT.GetDatabaseTableCompareRow(execQueryT, queryCondArgsT...)
			if err != nil {
				return fmt.Errorf("the database target query sql [%v] args [%v] running failed: [%v]", execQueryT, queryCondArgsT, err)
			}
			resultTM <- resultT[0]["ROWSCHECKSUM"]
			logger.Info("data compare task chunk downstream compare sql",
				zap.String("task_name", r.Dmt.TaskName),
				zap.String("task_mode", r.TaskMode),
				zap.String("task_flow", r.TaskFlow),
				zap.String("schema_name_t", r.Dmt.SchemaNameT),
				zap.String("table_name_t", r.Dmt.TableNameT),
				zap.String("compare_method", r.Dmt.CompareMethod),
				zap.String("chunk_id", r.Dmt.ChunkID),
				zap.String("chunk_detail_t", desChunkDetailT),
				zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
				zap.String("chunk_result_s", resultT[0]["ROWSCHECKSUM"]),
				zap.String("downstream_database_time", time.Now().Sub(streamTime).String()))
			return nil
		}
	})

	if err = g.Wait(); err != nil {
		return err
	}

	resultStrS := <-resultSM
	resultStrT := <-resultTM

	endTime := time.Now()

	resultS, err := decimal.NewFromString(resultStrS)
	if err != nil {
		return fmt.Errorf("parse the database source value [%s] target value [%s] failed: %v", resultStrS, resultStrT, err)
	}

	resultT, err := decimal.NewFromString(resultStrT)
	if err != nil {
		return fmt.Errorf("parse the database target value [%s] source value [%s] failed: %v", resultStrT, resultStrS, err)
	}
	if resultS.Equal(resultT) {
		logger.Info("data compare task chunk compare is equaled",
			zap.String("task_name", r.Dmt.TaskName),
			zap.String("task_mode", r.TaskMode),
			zap.String("task_flow", r.TaskFlow),
			zap.String("schema_name_s", r.Dmt.SchemaNameS),
			zap.String("table_name_s", r.Dmt.TableNameS),
			zap.String("compare_method", r.Dmt.CompareMethod),
			zap.String("chunk_id", r.Dmt.ChunkID),
			zap.String("chunk_detail_s", desChunkDetailS),
			zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
			zap.String("schema_name_t", r.Dmt.SchemaNameT),
			zap.String("table_name_t", r.Dmt.TableNameT),
			zap.String("chunk_detail_t", desChunkDetailT),
			zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
			zap.String("chunk_result_s", resultStrS),
			zap.String("chunk_result_t", resultStrT),
			zap.String("cost", endTime.Sub(startTime).String()))

		errW := model.Transaction(r.Ctx, func(txnCtx context.Context) error {
			_, err = model.GetIDataCompareTaskRW().UpdateDataCompareTask(txnCtx,
				&task.DataCompareTask{TaskName: r.Dmt.TaskName, SchemaNameS: r.Dmt.SchemaNameS, TableNameS: r.Dmt.TableNameS, ChunkID: r.Dmt.ChunkID},
				map[string]interface{}{
					"TaskStatus": constant.TaskDatabaseStatusEqual,
					"Duration":   fmt.Sprintf("%f", time.Now().Sub(r.StartTime).Seconds()),
				})
			if err != nil {
				return err
			}
			_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
				TaskName:    r.Dmt.TaskName,
				SchemaNameS: r.Dmt.SchemaNameS,
				TableNameS:  r.Dmt.TableNameS,
				LogDetail: fmt.Sprintf("%v [%v] data compare task [%v] taskflow [%v] source table [%v.%v] chunk_id [%s] chunk [%s] success",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(constant.TaskModeDataCompare),
					r.Dmt.TaskName,
					r.TaskFlow,
					r.Dmt.SchemaNameS,
					r.Dmt.TableNameS,
					r.Dmt.ChunkID,
					r.Dmt.ChunkDetailS),
			})
			if err != nil {
				return err
			}
			return nil
		})
		if errW != nil {
			return errW
		}

		logger.Info("data compare task chunk compare finished",
			zap.String("task_name", r.Dmt.TaskName),
			zap.String("task_mode", r.TaskMode),
			zap.String("task_flow", r.TaskFlow),
			zap.String("schema_name_s", r.Dmt.SchemaNameS),
			zap.String("table_name_s", r.Dmt.TableNameS),
			zap.String("compare_method", r.Dmt.CompareMethod),
			zap.String("chunk_id", r.Dmt.ChunkID),
			zap.String("chunk_detail_s", desChunkDetailS),
			zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
			zap.String("schema_name_t", r.Dmt.SchemaNameT),
			zap.String("table_name_t", r.Dmt.TableNameT),
			zap.String("chunk_detail_t", desChunkDetailT),
			zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
			zap.String("cost", time.Now().Sub(startTime).String()))
		return nil
	}

	logger.Info("data compare task chunk compare isn't equaled",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("compare_method", r.Dmt.CompareMethod),
		zap.String("chunk_id", r.Dmt.ChunkID),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
		zap.String("chunk_result_s", resultStrS),
		zap.String("chunk_result_t", resultStrT),
		zap.String("cost", endTime.Sub(startTime).String()))

	err = r.compareMd5OrCrc32Row()
	if err != nil {
		return err
	}
	return nil
}

func (r *DataCompareRow) CompareCRC32() error {
	startTime := time.Now()

	var (
		execQueryS, execQueryT                                   string
		columnDetailS, columnDetailT, chunkDetailS, chunkDetailT string
		queryCondArgsS, queryCondArgsT                           []interface{}
	)

	desChunkDetailS, err := stringutil.Decrypt(r.Dmt.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
	if err != nil {
		return err
	}
	desChunkDetailS = stringutil.BytesToString(decChunkDetailS)

	desChunkDetailT, err := stringutil.Decrypt(r.Dmt.ChunkDetailT, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	decChunkDetailT, err := snappy.Decode(nil, []byte(desChunkDetailT))
	if err != nil {
		return err
	}
	desChunkDetailT = stringutil.BytesToString(decChunkDetailT)

	convertRaw, err := stringutil.CharsetConvert([]byte(r.Dmt.ColumnDetailSO), constant.CharsetUTF8MB4, r.DBCharsetS)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	columnDetailS = stringutil.BytesToString(convertRaw)
	convertRaw, err = stringutil.CharsetConvert([]byte(desChunkDetailS), constant.CharsetUTF8MB4, r.DBCharsetS)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	chunkDetailS = stringutil.BytesToString(convertRaw)

	convertRaw, err = stringutil.CharsetConvert([]byte(r.Dmt.ColumnDetailTO), constant.CharsetUTF8MB4, r.DBCharsetT)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	columnDetailT = stringutil.BytesToString(convertRaw)
	convertRaw, err = stringutil.CharsetConvert([]byte(desChunkDetailT), constant.CharsetUTF8MB4, r.DBCharsetT)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	chunkDetailT = stringutil.BytesToString(convertRaw)

	dbTypeSli := stringutil.StringSplit(r.TaskFlow, constant.StringSeparatorAite)

	switch stringutil.StringUpper(dbTypeSli[0]) {
	case constant.DatabaseTypeOracle:
		if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
		} else if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
		} else if strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		}
	case constant.DatabaseTypeTiDB:
		if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` AS OF TIMESTAMP '", r.Dmt.SnapshotPointS, "' WHERE ", chunkDetailS)
		} else if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintS, " ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` AS OF TIMESTAMP '", r.Dmt.SnapshotPointS, "' WHERE ", chunkDetailS)
		} else if strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintS, " ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` WHERE ", chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder("SELECT ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` WHERE ", chunkDetailS)
		}
	case constant.DatabaseTypeMySQL:
		if !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintS, " ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` WHERE ", chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder("SELECT ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` WHERE ", chunkDetailS)
		}
	case constant.DatabaseTypePostgresql:
		if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointS, `'`, constant.StringSeparatorSemicolon, `SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointS, `'`, constant.StringSeparatorSemicolon, `SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else if strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		}
	default:
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] db_type_s [%s] is not supported, please contact author or reselect", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, dbTypeSli[0])
	}

	switch stringutil.StringUpper(dbTypeSli[1]) {
	case constant.DatabaseTypeOracle:
		if !strings.EqualFold(r.Dmt.SqlHintT, "") && strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else if !strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" AS OF SCN `, r.Dmt.SnapshotPointT, ` WHERE `, chunkDetailT)
		} else if strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" AS OF SCN `, r.Dmt.SnapshotPointT, ` WHERE `, chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		}
	case constant.DatabaseTypeTiDB:
		if !strings.EqualFold(r.Dmt.SqlHintT, "") && strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintT, " ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` WHERE ", chunkDetailT)
		} else if !strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintT, " ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` AS OF TIMESTAMP '", r.Dmt.SnapshotPointT, "' WHERE ", chunkDetailT)
		} else if strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` AS OF TIMESTAMP '", r.Dmt.SnapshotPointT, "' WHERE ", chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder("SELECT ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` WHERE ", chunkDetailT)
		}
	case constant.DatabaseTypeMySQL:
		if !strings.EqualFold(r.Dmt.SqlHintT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintT, " ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` WHERE ", chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder("SELECT ", columnDetailT, " FROM ", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` WHERE ", chunkDetailT)
		}
	case constant.DatabaseTypePostgresql:
		if !strings.EqualFold(r.Dmt.SqlHintT, "") && strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else if !strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointT, `'`, constant.StringSeparatorSemicolon, `SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else if strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointT, `'`, constant.StringSeparatorSemicolon, `SELECT `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		}
	default:
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] db_type_t [%s] is not supported, please contact author or reselect", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, dbTypeSli[1])
	}

	logger.Info("data compare task chunk compare details starting",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("compare_method", r.Dmt.CompareMethod),
		zap.String("chunk_id", r.Dmt.ChunkID),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
		zap.String("startTime", startTime.String()))

	if strings.EqualFold(r.Dmt.ChunkDetailArgS, "") {
		queryCondArgsS = nil
	} else {
		err = stringutil.UnmarshalJSON([]byte(r.Dmt.ChunkDetailArgS), &queryCondArgsS)
		if err != nil {
			return fmt.Errorf("the database source query args [%v] running failed: [%v]", r.Dmt.ChunkDetailArgS, err)
		}
	}

	if strings.EqualFold(r.Dmt.ChunkDetailArgT, "") {
		queryCondArgsT = nil
	} else {
		err = stringutil.UnmarshalJSON([]byte(r.Dmt.ChunkDetailArgT), &queryCondArgsT)
		if err != nil {
			return fmt.Errorf("the database target query args [%v] running failed: [%v]", r.Dmt.ChunkDetailArgT, err)
		}
	}

	crc32ValSC := make(chan uint32, 1)
	columnDataSMC := make(chan map[string]int64, 1)
	columnDataTMC := make(chan map[string]int64, 1)
	columnNameTC := make(chan []string, 1)
	columnNameSC := make(chan []string, 1)

	crc32ValTC := make(chan uint32, 1)

	g, ctx := errgroup.WithContext(r.Ctx)

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
			streamTime := time.Now()
			columnS, crc32ValS, columnDataS, err := r.DatabaseS.GetDatabaseTableCompareCrc(execQueryS, r.CallTimeout, r.DBCharsetS, constant.CharsetUTF8MB4, r.Separator, queryCondArgsS)
			if err != nil {
				return fmt.Errorf("the database source query sql [%v] args [%v] comparing failed: [%v]", execQueryS, queryCondArgsS, err)
			}
			columnNameSC <- columnS
			crc32ValSC <- crc32ValS
			columnDataSMC <- columnDataS

			logger.Info("data compare task chunk upstream compare sql",
				zap.String("task_name", r.Dmt.TaskName),
				zap.String("task_mode", r.TaskMode),
				zap.String("task_flow", r.TaskFlow),
				zap.String("schema_name_s", r.Dmt.SchemaNameS),
				zap.String("table_name_s", r.Dmt.TableNameS),
				zap.String("compare_method", r.Dmt.CompareMethod),
				zap.String("chunk_id", r.Dmt.ChunkID),
				zap.String("schema_name_t", r.Dmt.SchemaNameT),
				zap.String("table_name_t", r.Dmt.TableNameT),
				zap.String("chunk_detail_s", desChunkDetailS),
				zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
				zap.String("chunk_detail_t", desChunkDetailT),
				zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
				zap.String("upstream_database_time", time.Now().Sub(streamTime).String()))
			return nil
		}
	})

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
			streamTime := time.Now()
			columnT, crc32ValT, columnDataT, err := r.DatabaseT.GetDatabaseTableCompareCrc(execQueryT, r.CallTimeout, r.DBCharsetT, constant.CharsetUTF8MB4, r.Separator, queryCondArgsT)
			if err != nil {
				return fmt.Errorf("the database target query sql [%v] args [%v] comparing failed: [%v]", execQueryT, queryCondArgsT, err)
			}
			columnNameTC <- columnT
			crc32ValTC <- crc32ValT
			columnDataTMC <- columnDataT
			logger.Info("data compare task chunk downstream compare sql",
				zap.String("task_name", r.Dmt.TaskName),
				zap.String("task_mode", r.TaskMode),
				zap.String("task_flow", r.TaskFlow),
				zap.String("schema_name_s", r.Dmt.SchemaNameS),
				zap.String("table_name_s", r.Dmt.TableNameS),
				zap.String("compare_method", r.Dmt.CompareMethod),
				zap.String("chunk_id", r.Dmt.ChunkID),
				zap.String("schema_name_t", r.Dmt.SchemaNameT),
				zap.String("table_name_t", r.Dmt.TableNameT),
				zap.String("chunk_detail_s", desChunkDetailS),
				zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
				zap.String("chunk_detail_t", desChunkDetailT),
				zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
				zap.String("downstream_database_time", time.Now().Sub(streamTime).String()))
			return nil
		}
	})

	if err = g.Wait(); err != nil {
		return err
	}

	columnDataSM := <-columnDataSMC
	crc32VS := <-crc32ValSC
	columnNameS := <-columnNameSC

	columnDataTM := <-columnDataTMC
	crc32VT := <-crc32ValTC
	columnNameT := <-columnNameTC

	endTime := time.Now()

	if crc32VS == crc32VT {
		logger.Info("data compare task chunk compare is equaled",
			zap.String("task_name", r.Dmt.TaskName),
			zap.String("task_mode", r.TaskMode),
			zap.String("task_flow", r.TaskFlow),
			zap.String("schema_name_s", r.Dmt.SchemaNameS),
			zap.String("table_name_s", r.Dmt.TableNameS),
			zap.String("compare_method", r.Dmt.CompareMethod),
			zap.String("chunk_id", r.Dmt.ChunkID),
			zap.String("chunk_detail_s", desChunkDetailS),
			zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
			zap.String("schema_name_t", r.Dmt.SchemaNameT),
			zap.String("table_name_t", r.Dmt.TableNameT),
			zap.String("chunk_detail_t", desChunkDetailT),
			zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
			zap.Uint32("table_crc32_s", crc32VS),
			zap.Uint32("table_crc32_t", crc32VT),
			zap.String("cost", endTime.Sub(startTime).String()))

		errW := model.Transaction(r.Ctx, func(txnCtx context.Context) error {
			_, err = model.GetIDataCompareTaskRW().UpdateDataCompareTask(txnCtx,
				&task.DataCompareTask{TaskName: r.Dmt.TaskName, SchemaNameS: r.Dmt.SchemaNameS, TableNameS: r.Dmt.TableNameS, ChunkID: r.Dmt.ChunkID},
				map[string]interface{}{
					"TaskStatus": constant.TaskDatabaseStatusEqual,
					"Duration":   fmt.Sprintf("%f", time.Now().Sub(r.StartTime).Seconds()),
				})
			if err != nil {
				return err
			}
			_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
				TaskName:    r.Dmt.TaskName,
				SchemaNameS: r.Dmt.SchemaNameS,
				TableNameS:  r.Dmt.TableNameS,
				LogDetail: fmt.Sprintf("%v [%v] data compare task [%v] taskflow [%v] source table [%v.%v] chunk_id [%s] chunk [%s] success",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(constant.TaskModeDataCompare),
					r.Dmt.TaskName,
					r.TaskFlow,
					r.Dmt.SchemaNameS,
					r.Dmt.TableNameS,
					r.Dmt.ChunkID,
					r.Dmt.ChunkDetailS),
			})
			if err != nil {
				return err
			}
			return nil
		})
		if errW != nil {
			return errW
		}

		logger.Info("data compare task chunk compare details finished",
			zap.String("task_name", r.Dmt.TaskName),
			zap.String("task_mode", r.TaskMode),
			zap.String("task_flow", r.TaskFlow),
			zap.String("schema_name_s", r.Dmt.SchemaNameS),
			zap.String("table_name_s", r.Dmt.TableNameS),
			zap.String("compare_method", r.Dmt.CompareMethod),
			zap.String("chunk_id", r.Dmt.ChunkID),
			zap.String("schema_name_t", r.Dmt.SchemaNameT),
			zap.String("table_name_t", r.Dmt.TableNameT),
			zap.String("chunk_detail_s", desChunkDetailS),
			zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
			zap.String("chunk_detail_t", desChunkDetailT),
			zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
			zap.String("cost", time.Now().Sub(startTime).String()))
		return nil
	}

	logger.Info("data compare task chunk compare isn't equaled",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("compare_method", r.Dmt.CompareMethod),
		zap.String("chunk_id", r.Dmt.ChunkID),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
		zap.Uint32("table_crc32_s", crc32VS),
		zap.Uint32("table_crc32_t", crc32VT),
		zap.String("cost", endTime.Sub(startTime).String()))

	compareTime := time.Now()
	addDestSets, delDestSets := Cmp(columnDataSM, columnDataTM)

	logger.Info("data compare task chunk compare rows detail",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("compare_method", r.Dmt.CompareMethod),
		zap.String("chunk_id", r.Dmt.ChunkID),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
		zap.String("compare_row_time", time.Now().Sub(compareTime).String()))

	logger.Debug("data compare task chunk compare rows detail",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("compare_method", r.Dmt.CompareMethod),
		zap.String("chunk_id", r.Dmt.ChunkID),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_id", r.Dmt.ChunkID),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
		zap.Any("addSets", addDestSets),
		zap.Any("deleteSets", delDestSets),
		zap.String("compare_row_time", time.Now().Sub(compareTime).String()))

	var (
		addDetails []string
		delDetails []string
	)
	for dk, dv := range addDestSets {
		switch r.TaskFlow {
		case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB:
			if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowDownstream) {
				addDetails = append(addDetails, GenMYSQLCompatibleDatabaseInsertStmtSQL(
					r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, stringutil.StringSplit(dk, r.Separator), int(dv), false))
			} else if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowUpstream) {
				delDetails = append(delDetails, GenOracleCompatibleDatabaseDeleteStmtSQL(
					r.Dmt.SchemaNameS, r.Dmt.TableNameS, "", columnNameS, stringutil.StringSplit(dk, r.Separator), int(dv)))
			} else {
				return fmt.Errorf("the data compare task [%s] task_flow [%s] param repair-stmt-flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow, r.RepairStmtFlow)
			}
		case constant.TaskFlowTiDBToOracle, constant.TaskFlowMySQLToOracle:
			if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowDownstream) {
				addDetails = append(addDetails, GenOracleCompatibleDatabaseInsertStmtSQL(
					r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, stringutil.StringSplit(dk, r.Separator), int(dv), false))
			} else if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowUpstream) {
				delDetails = append(delDetails, GenMYSQLCompatibleDatabaseDeleteStmtSQL(
					r.Dmt.SchemaNameS, r.Dmt.TableNameS, "", columnNameS, stringutil.StringSplit(dk, r.Separator), int(dv)))
			} else {
				return fmt.Errorf("the data compare task [%s] task_flow [%s] param repair-stmt-flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow, r.RepairStmtFlow)
			}
		case constant.TaskFlowPostgresToMySQL, constant.TaskFlowPostgresToTiDB:
			if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowDownstream) {
				addDetails = append(addDetails, GenMYSQLCompatibleDatabaseInsertStmtSQL(
					r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, stringutil.StringSplit(dk, r.Separator), int(dv), false))
			} else if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowUpstream) {
				delDetails = append(delDetails, GenPostgresCompatibleDatabaseDeleteStmtSQL(
					r.Dmt.SchemaNameS, r.Dmt.TableNameS, "", columnNameS, stringutil.StringSplit(dk, r.Separator), int(dv)))
			} else {
				return fmt.Errorf("the data compare task [%s] task_flow [%s] param repair-stmt-flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow, r.RepairStmtFlow)
			}
		case constant.TaskFlowTiDBToPostgres, constant.TaskFlowMySQLToPostgres:
			if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowDownstream) {
				addDetails = append(addDetails, GenPostgresCompatibleDatabaseInsertStmtSQL(
					r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, stringutil.StringSplit(dk, r.Separator), int(dv), false))
			} else if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowUpstream) {
				delDetails = append(delDetails, GenMYSQLCompatibleDatabaseDeleteStmtSQL(
					r.Dmt.SchemaNameS, r.Dmt.TableNameS, "", columnNameS, stringutil.StringSplit(dk, r.Separator), int(dv)))
			} else {
				return fmt.Errorf("the data compare task [%s] task_flow [%s] param repair-stmt-flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow, r.RepairStmtFlow)
			}
		default:
			return fmt.Errorf("the data compare task [%s] task_flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow)
		}

	}
	for dk, dv := range delDestSets {
		switch r.TaskFlow {
		case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB:
			if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowDownstream) {
				delDetails = append(delDetails, GenMYSQLCompatibleDatabaseDeleteStmtSQL(
					r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, stringutil.StringSplit(dk, r.Separator), int(dv)))
			} else if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowUpstream) {
				addDetails = append(addDetails, GenOracleCompatibleDatabaseInsertStmtSQL(
					r.Dmt.SchemaNameS, r.Dmt.TableNameS, "", columnNameS, stringutil.StringSplit(dk, r.Separator), int(dv), false))
			} else {
				return fmt.Errorf("the data compare task [%s] task_flow [%s] param repair-stmt-flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow, r.RepairStmtFlow)
			}
		case constant.TaskFlowTiDBToOracle, constant.TaskFlowMySQLToOracle:
			if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowDownstream) {
				delDetails = append(delDetails, GenOracleCompatibleDatabaseDeleteStmtSQL(
					r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, stringutil.StringSplit(dk, r.Separator), int(dv)))
			} else if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowUpstream) {
				addDetails = append(addDetails, GenMYSQLCompatibleDatabaseInsertStmtSQL(
					r.Dmt.SchemaNameS, r.Dmt.TableNameS, "", columnNameS, stringutil.StringSplit(dk, r.Separator), int(dv), false))
			} else {
				return fmt.Errorf("the data compare task [%s] task_flow [%s] param repair-stmt-flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow, r.RepairStmtFlow)
			}
		case constant.TaskFlowPostgresToMySQL, constant.TaskFlowPostgresToTiDB:
			if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowDownstream) {
				delDetails = append(delDetails, GenMYSQLCompatibleDatabaseDeleteStmtSQL(
					r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, stringutil.StringSplit(dk, r.Separator), int(dv)))
			} else if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowUpstream) {
				addDetails = append(addDetails, GenPostgresCompatibleDatabaseInsertStmtSQL(
					r.Dmt.SchemaNameS, r.Dmt.TableNameS, "", columnNameS, stringutil.StringSplit(dk, r.Separator), int(dv), false))
			} else {
				return fmt.Errorf("the data compare task [%s] task_flow [%s] param repair-stmt-flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow, r.RepairStmtFlow)
			}
		case constant.TaskFlowTiDBToPostgres, constant.TaskFlowMySQLToPostgres:
			if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowDownstream) {
				delDetails = append(delDetails, GenPostgresCompatibleDatabaseDeleteStmtSQL(
					r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, stringutil.StringSplit(dk, r.Separator), int(dv)))
			} else if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowUpstream) {
				addDetails = append(addDetails, GenMYSQLCompatibleDatabaseInsertStmtSQL(
					r.Dmt.SchemaNameS, r.Dmt.TableNameS, "", columnNameS, stringutil.StringSplit(dk, r.Separator), int(dv), false))
			} else {
				return fmt.Errorf("the data compare task [%s] task_flow [%s] param repair-stmt-flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow, r.RepairStmtFlow)
			}
		default:
			return fmt.Errorf("the data compare task [%s] task_flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow)
		}

	}

	if len(delDetails) > 0 {
		splitCounts := len(delDetails) / r.BatchSize
		if splitCounts == 0 {
			splitCounts = 1
		}
		groupDelDetails := stringutil.StringSliceSplit(delDetails, splitCounts)

		gDel, gDelCtx := errgroup.WithContext(r.Ctx)
		gDel.SetLimit(r.WriteThread)

		for _, gds := range groupDelDetails {
			details := gds
			gDel.Go(func() error {
				select {
				case <-gDelCtx.Done():
					return nil
				default:
					encryptDelDetails, err := stringutil.Encrypt(stringutil.StringJoin(details, "\n"), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					_, err = model.GetIDataCompareResultRW().CreateDataCompareResult(gDelCtx, &task.DataCompareResult{
						TaskName:     r.Dmt.TaskName,
						SchemaNameS:  r.Dmt.SchemaNameS,
						TableNameS:   r.Dmt.TableNameS,
						SchemaNameT:  r.Dmt.SchemaNameT,
						TableNameT:   r.Dmt.TableNameT,
						ChunkID:      r.Dmt.ChunkID,
						ChunkDetailS: r.Dmt.ChunkDetailS,
						FixStmtType:  constant.DataCompareFixStmtTypeDelete,
						FixDetailT:   encryptDelDetails,
					})
					if err != nil {
						return err
					}
					return nil
				}
			})
		}
		if err = gDel.Wait(); err != nil {
			return err
		}
	}

	if len(addDetails) > 0 {
		splitCounts := len(addDetails) / r.BatchSize
		if splitCounts == 0 {
			splitCounts = 1
		}
		groupAddDetails := stringutil.StringSliceSplit(addDetails, splitCounts)

		gAdd, gAddCtx := errgroup.WithContext(r.Ctx)
		gAdd.SetLimit(r.WriteThread)

		for _, gds := range groupAddDetails {
			details := gds
			gAdd.Go(func() error {
				select {
				case <-gAddCtx.Done():
					return nil
				default:
					encryptAddDetails, err := stringutil.Encrypt(stringutil.StringJoin(details, "\n"), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					_, err = model.GetIDataCompareResultRW().CreateDataCompareResult(gAddCtx, &task.DataCompareResult{
						TaskName:     r.Dmt.TaskName,
						SchemaNameS:  r.Dmt.SchemaNameS,
						TableNameS:   r.Dmt.TableNameS,
						SchemaNameT:  r.Dmt.SchemaNameT,
						TableNameT:   r.Dmt.TableNameT,
						ChunkID:      r.Dmt.ChunkID,
						ChunkDetailS: r.Dmt.ChunkDetailS,
						FixStmtType:  constant.DataCompareFixStmtTypeInsert,
						FixDetailT:   encryptAddDetails,
					})
					if err != nil {
						return err
					}
					return nil
				}
			})
		}
		if err = gAdd.Wait(); err != nil {
			return err
		}
	}

	errW := model.Transaction(r.Ctx, func(txnCtx context.Context) error {
		_, err = model.GetIDataCompareTaskRW().UpdateDataCompareTask(txnCtx,
			&task.DataCompareTask{TaskName: r.Dmt.TaskName, SchemaNameS: r.Dmt.SchemaNameS, TableNameS: r.Dmt.TableNameS, ChunkID: r.Dmt.ChunkID},
			map[string]interface{}{
				"TaskStatus": constant.TaskDatabaseStatusNotEqual,
				"Duration":   fmt.Sprintf("%f", time.Now().Sub(r.StartTime).Seconds()),
			})
		if err != nil {
			return err
		}
		_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
			TaskName:    r.Dmt.TaskName,
			SchemaNameS: r.Dmt.SchemaNameS,
			TableNameS:  r.Dmt.TableNameS,
			LogDetail: fmt.Sprintf("%v [%v] data compare task [%v] taskflow [%v] source table [%v.%v] chunk_id [%s] chunk [%s] success",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeDataCompare),
				r.Dmt.TaskName,
				r.TaskFlow,
				r.Dmt.SchemaNameS,
				r.Dmt.TableNameS,
				r.Dmt.ChunkID,
				r.Dmt.ChunkDetailS),
		})
		if err != nil {
			return err
		}
		return nil
	})
	if errW != nil {
		return errW
	}

	logger.Info("data compare task chunk compare details finished",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("compare_method", r.Dmt.CompareMethod),
		zap.String("chunk_id", r.Dmt.ChunkID),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}

func (r *DataCompareRow) compareMd5OrCrc32Row() error {
	startTime := time.Now()

	var (
		execQueryS, execQueryT                                   string
		columnDetailS, columnDetailT, chunkDetailS, chunkDetailT string
		queryCondArgsS, queryCondArgsT                           []interface{}
	)

	desChunkDetailS, err := stringutil.Decrypt(r.Dmt.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
	if err != nil {
		return err
	}
	desChunkDetailS = stringutil.BytesToString(decChunkDetailS)

	desChunkDetailT, err := stringutil.Decrypt(r.Dmt.ChunkDetailT, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	decChunkDetailT, err := snappy.Decode(nil, []byte(desChunkDetailT))
	if err != nil {
		return err
	}
	desChunkDetailT = stringutil.BytesToString(decChunkDetailT)

	convertRaw, err := stringutil.CharsetConvert([]byte(r.Dmt.ColumnDetailSO), constant.CharsetUTF8MB4, r.DBCharsetS)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	columnDetailS = stringutil.BytesToString(convertRaw)
	convertRaw, err = stringutil.CharsetConvert([]byte(desChunkDetailS), constant.CharsetUTF8MB4, r.DBCharsetS)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	chunkDetailS = stringutil.BytesToString(convertRaw)

	convertRaw, err = stringutil.CharsetConvert([]byte(r.Dmt.ColumnDetailTO), constant.CharsetUTF8MB4, r.DBCharsetT)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	columnDetailT = stringutil.BytesToString(convertRaw)
	convertRaw, err = stringutil.CharsetConvert([]byte(desChunkDetailT), constant.CharsetUTF8MB4, r.DBCharsetT)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	chunkDetailT = stringutil.BytesToString(convertRaw)

	dbTypeSli := stringutil.StringSplit(r.TaskFlow, constant.StringSeparatorAite)

	switch stringutil.StringUpper(dbTypeSli[0]) {
	case constant.DatabaseTypeOracle:
		if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
		} else if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
		} else if strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		}
	case constant.DatabaseTypeTiDB:
		if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` AS OF TIMESTAMP '", r.Dmt.SnapshotPointS, "' WHERE ", chunkDetailS)
		} else if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintS, " ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` AS OF TIMESTAMP '", r.Dmt.SnapshotPointS, "' WHERE ", chunkDetailS)
		} else if strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintS, " ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` WHERE ", chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder("SELECT ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` WHERE ", chunkDetailS)
		}
	case constant.DatabaseTypeMySQL:
		if !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintS, " ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` WHERE ", chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder("SELECT ", columnDetailS, " FROM `", r.Dmt.SchemaNameS, "`.`", r.Dmt.TableNameS, "` WHERE ", chunkDetailS)
		}
	case constant.DatabaseTypePostgresql:
		if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointS, `'`, constant.StringSeparatorSemicolon, `SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else if !strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointS, `'`, constant.StringSeparatorSemicolon, `SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else if strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		}
	default:
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] db_type_s [%s] is not supported, please contact author or reselect", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, dbTypeSli[0])
	}

	switch stringutil.StringUpper(dbTypeSli[1]) {
	case constant.DatabaseTypeOracle:
		if !strings.EqualFold(r.Dmt.SqlHintT, "") && strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else if !strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" AS OF SCN `, r.Dmt.SnapshotPointT, ` WHERE `, chunkDetailT)
		} else if strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" AS OF SCN `, r.Dmt.SnapshotPointT, ` WHERE `, chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		}
	case constant.DatabaseTypeTiDB:
		if !strings.EqualFold(r.Dmt.SqlHintT, "") && strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintT, " ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` WHERE ", chunkDetailT)
		} else if !strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintT, " ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` AS OF TIMESTAMP '", r.Dmt.SnapshotPointT, "' WHERE ", chunkDetailT)
		} else if strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` AS OF TIMESTAMP '", r.Dmt.SnapshotPointT, "' WHERE ", chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder("SELECT ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` WHERE ", chunkDetailT)
		}
	case constant.DatabaseTypeMySQL:
		if !strings.EqualFold(r.Dmt.SqlHintT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", r.Dmt.SqlHintT, " ", columnDetailT, " FROM `", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` WHERE ", chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder("SELECT ", columnDetailT, " FROM ", r.Dmt.SchemaNameT, "`.`", r.Dmt.TableNameT, "` WHERE ", chunkDetailT)
		}
	case constant.DatabaseTypePostgresql:
		if !strings.EqualFold(r.Dmt.SqlHintT, "") && strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else if !strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointT, `'`, constant.StringSeparatorSemicolon,
				`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else if strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, r.Dmt.SnapshotPointT, `'`, constant.StringSeparatorSemicolon,
				`SELECT `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM "`, r.Dmt.SchemaNameT, `"."`, r.Dmt.TableNameT, `" WHERE `, chunkDetailT)
		}
	default:
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] db_type_t [%s] is not supported, please contact author or reselect", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, dbTypeSli[1])
	}

	logger.Info("data compare task chunk compare details starting",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("compare_method", r.Dmt.CompareMethod),
		zap.String("chunk_id", r.Dmt.ChunkID),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
		zap.String("startTime", startTime.String()))

	if strings.EqualFold(r.Dmt.ChunkDetailArgS, "") {
		queryCondArgsS = nil
	} else {
		err = stringutil.UnmarshalJSON([]byte(r.Dmt.ChunkDetailArgS), &queryCondArgsS)
		if err != nil {
			return fmt.Errorf("the database source query args [%v] running failed: [%v]", r.Dmt.ChunkDetailArgS, err)
		}
	}

	if strings.EqualFold(r.Dmt.ChunkDetailArgT, "") {
		queryCondArgsT = nil
	} else {
		err = stringutil.UnmarshalJSON([]byte(r.Dmt.ChunkDetailArgT), &queryCondArgsT)
		if err != nil {
			return fmt.Errorf("the database target query args [%v] running failed: [%v]", r.Dmt.ChunkDetailArgT, err)
		}
	}

	columnDataSMC := make(chan map[string]int64, 1)
	columnNameSC := make(chan []string, 1)

	columnDataTMC := make(chan map[string]int64, 1)
	columnNameTC := make(chan []string, 1)

	g, ctx := errgroup.WithContext(r.Ctx)

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
			streamTime := time.Now()
			columnS, _, columnDataS, err := r.DatabaseS.GetDatabaseTableCompareCrc(execQueryS, r.CallTimeout, r.DBCharsetS, constant.CharsetUTF8MB4, r.Separator, queryCondArgsS)
			if err != nil {
				return fmt.Errorf("the database source query sql [%v] args [%v] comparing failed: [%v]", execQueryS, queryCondArgsS, err)
			}
			columnNameSC <- columnS
			columnDataSMC <- columnDataS
			logger.Info("data compare task chunk upstream compare rows",
				zap.String("task_name", r.Dmt.TaskName),
				zap.String("task_mode", r.TaskMode),
				zap.String("task_flow", r.TaskFlow),
				zap.String("schema_name_s", r.Dmt.SchemaNameS),
				zap.String("table_name_s", r.Dmt.TableNameS),
				zap.String("compare_method", r.Dmt.CompareMethod),
				zap.String("chunk_id", r.Dmt.ChunkID),
				zap.String("schema_name_t", r.Dmt.SchemaNameT),
				zap.String("table_name_t", r.Dmt.TableNameT),
				zap.String("chunk_detail_s", desChunkDetailS),
				zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
				zap.String("chunk_detail_t", desChunkDetailT),
				zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
				zap.String("upstream_database_time", time.Now().Sub(streamTime).String()))
			return nil
		}
	})

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
			streamTime := time.Now()
			columnT, _, columnDataT, err := r.DatabaseT.GetDatabaseTableCompareCrc(execQueryT, r.CallTimeout, r.DBCharsetT, constant.CharsetUTF8MB4, r.Separator, queryCondArgsT)
			if err != nil {
				return fmt.Errorf("the database target query sql [%v] args [%v] comparing failed: [%v]", execQueryT, queryCondArgsT, err)
			}
			columnNameTC <- columnT
			columnDataTMC <- columnDataT
			logger.Info("data compare task chunk downstream compare rows",
				zap.String("task_name", r.Dmt.TaskName),
				zap.String("task_mode", r.TaskMode),
				zap.String("task_flow", r.TaskFlow),
				zap.String("schema_name_s", r.Dmt.SchemaNameS),
				zap.String("table_name_s", r.Dmt.TableNameS),
				zap.String("compare_method", r.Dmt.CompareMethod),
				zap.String("chunk_id", r.Dmt.ChunkID),
				zap.String("schema_name_t", r.Dmt.SchemaNameT),
				zap.String("table_name_t", r.Dmt.TableNameT),
				zap.String("chunk_detail_s", desChunkDetailS),
				zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
				zap.String("chunk_detail_t", desChunkDetailT),
				zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
				zap.String("downstream_database_time", time.Now().Sub(streamTime).String()))
			return nil
		}
	})

	if err = g.Wait(); err != nil {
		return err
	}

	columnDataSM := <-columnDataSMC
	columnNameS := <-columnNameSC

	columnDataTM := <-columnDataTMC
	columnNameT := <-columnNameTC

	compareTime := time.Now()
	addDestSets, delDestSets := Cmp(columnDataSM, columnDataTM)
	logger.Info("data compare task chunk compare rows detail",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("compare_method", r.Dmt.CompareMethod),
		zap.String("chunk_id", r.Dmt.ChunkID),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
		zap.String("compare_row_time", time.Now().Sub(compareTime).String()))

	logger.Debug("data compare task chunk compare rows detail",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("compare_method", r.Dmt.CompareMethod),
		zap.String("chunk_id", r.Dmt.ChunkID),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_id", r.Dmt.ChunkID),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
		zap.Any("addSets", addDestSets),
		zap.Any("deleteSets", delDestSets),
		zap.String("compare_row_time", time.Now().Sub(compareTime).String()))
	var (
		addDetails []string
		delDetails []string
	)
	for dk, dv := range addDestSets {
		switch r.TaskFlow {
		case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB:
			if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowDownstream) {
				addDetails = append(addDetails, GenMYSQLCompatibleDatabaseInsertStmtSQL(
					r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, stringutil.StringSplit(dk, r.Separator), int(dv), false))
			} else if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowUpstream) {
				delDetails = append(delDetails, GenOracleCompatibleDatabaseDeleteStmtSQL(
					r.Dmt.SchemaNameS, r.Dmt.TableNameS, "", columnNameS, stringutil.StringSplit(dk, r.Separator), int(dv)))
			} else {
				return fmt.Errorf("the data compare task [%s] task_flow [%s] param repair-stmt-flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow, r.RepairStmtFlow)
			}
		case constant.TaskFlowTiDBToOracle, constant.TaskFlowMySQLToOracle:
			if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowDownstream) {
				addDetails = append(addDetails, GenOracleCompatibleDatabaseInsertStmtSQL(
					r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, stringutil.StringSplit(dk, r.Separator), int(dv), false))
			} else if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowUpstream) {
				delDetails = append(delDetails, GenMYSQLCompatibleDatabaseDeleteStmtSQL(
					r.Dmt.SchemaNameS, r.Dmt.TableNameS, "", columnNameS, stringutil.StringSplit(dk, r.Separator), int(dv)))
			} else {
				return fmt.Errorf("the data compare task [%s] task_flow [%s] param repair-stmt-flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow, r.RepairStmtFlow)
			}
		case constant.TaskFlowPostgresToMySQL, constant.TaskFlowPostgresToTiDB:
			if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowDownstream) {
				addDetails = append(addDetails, GenMYSQLCompatibleDatabaseInsertStmtSQL(
					r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, stringutil.StringSplit(dk, r.Separator), int(dv), false))
			} else if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowUpstream) {
				delDetails = append(delDetails, GenPostgresCompatibleDatabaseDeleteStmtSQL(
					r.Dmt.SchemaNameS, r.Dmt.TableNameS, "", columnNameS, stringutil.StringSplit(dk, r.Separator), int(dv)))
			} else {
				return fmt.Errorf("the data compare task [%s] task_flow [%s] param repair-stmt-flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow, r.RepairStmtFlow)
			}
		case constant.TaskFlowTiDBToPostgres, constant.TaskFlowMySQLToPostgres:
			if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowDownstream) {
				addDetails = append(addDetails, GenPostgresCompatibleDatabaseInsertStmtSQL(
					r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, stringutil.StringSplit(dk, r.Separator), int(dv), false))
			} else if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowUpstream) {
				delDetails = append(delDetails, GenMYSQLCompatibleDatabaseDeleteStmtSQL(
					r.Dmt.SchemaNameS, r.Dmt.TableNameS, "", columnNameS, stringutil.StringSplit(dk, r.Separator), int(dv)))
			} else {
				return fmt.Errorf("the data compare task [%s] task_flow [%s] param repair-stmt-flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow, r.RepairStmtFlow)
			}
		default:
			return fmt.Errorf("the data compare task [%s] task_flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow)
		}

	}

	for dk, dv := range delDestSets {
		switch r.TaskFlow {
		case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB:
			if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowDownstream) {
				delDetails = append(delDetails, GenMYSQLCompatibleDatabaseDeleteStmtSQL(
					r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, stringutil.StringSplit(dk, r.Separator), int(dv)))
			} else if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowUpstream) {
				addDetails = append(addDetails, GenOracleCompatibleDatabaseInsertStmtSQL(
					r.Dmt.SchemaNameS, r.Dmt.TableNameS, "", columnNameS, stringutil.StringSplit(dk, r.Separator), int(dv), false))
			} else {
				return fmt.Errorf("the data compare task [%s] task_flow [%s] param repair-stmt-flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow, r.RepairStmtFlow)
			}
		case constant.TaskFlowTiDBToOracle, constant.TaskFlowMySQLToOracle:
			if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowDownstream) {
				delDetails = append(delDetails, GenOracleCompatibleDatabaseDeleteStmtSQL(
					r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, stringutil.StringSplit(dk, r.Separator), int(dv)))
			} else if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowUpstream) {
				addDetails = append(addDetails, GenMYSQLCompatibleDatabaseInsertStmtSQL(
					r.Dmt.SchemaNameS, r.Dmt.TableNameS, "", columnNameS, stringutil.StringSplit(dk, r.Separator), int(dv), false))
			} else {
				return fmt.Errorf("the data compare task [%s] task_flow [%s] param repair-stmt-flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow, r.RepairStmtFlow)
			}
		case constant.TaskFlowPostgresToMySQL, constant.TaskFlowPostgresToTiDB:
			if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowDownstream) {
				delDetails = append(delDetails, GenMYSQLCompatibleDatabaseDeleteStmtSQL(
					r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, stringutil.StringSplit(dk, r.Separator), int(dv)))
			} else if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowUpstream) {
				addDetails = append(addDetails, GenPostgresCompatibleDatabaseInsertStmtSQL(
					r.Dmt.SchemaNameS, r.Dmt.TableNameS, "", columnNameS, stringutil.StringSplit(dk, r.Separator), int(dv), false))
			} else {
				return fmt.Errorf("the data compare task [%s] task_flow [%s] param repair-stmt-flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow, r.RepairStmtFlow)
			}
		case constant.TaskFlowTiDBToPostgres, constant.TaskFlowMySQLToPostgres:
			if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowDownstream) {
				delDetails = append(delDetails, GenPostgresCompatibleDatabaseDeleteStmtSQL(
					r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, stringutil.StringSplit(dk, r.Separator), int(dv)))
			} else if strings.EqualFold(r.RepairStmtFlow, constant.DataCompareRepairStmtFlowUpstream) {
				addDetails = append(addDetails, GenMYSQLCompatibleDatabaseInsertStmtSQL(
					r.Dmt.SchemaNameS, r.Dmt.TableNameS, "", columnNameS, stringutil.StringSplit(dk, r.Separator), int(dv), false))
			} else {
				return fmt.Errorf("the data compare task [%s] task_flow [%s] param repair-stmt-flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow, r.RepairStmtFlow)
			}
		default:
			return fmt.Errorf("the data compare task [%s] task_flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow)
		}

	}

	if len(delDetails) > 0 {
		splitCounts := len(delDetails) / r.BatchSize
		if splitCounts == 0 {
			splitCounts = 1
		}
		groupDelDetails := stringutil.StringSliceSplit(delDetails, splitCounts)

		gDel, gDelCtx := errgroup.WithContext(r.Ctx)
		gDel.SetLimit(r.WriteThread)

		for _, gds := range groupDelDetails {
			details := gds
			gDel.Go(func() error {
				select {
				case <-gDelCtx.Done():
					return nil
				default:
					encryptDelDetails, err := stringutil.Encrypt(stringutil.StringJoin(details, "\n"), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					_, err = model.GetIDataCompareResultRW().CreateDataCompareResult(gDelCtx, &task.DataCompareResult{
						TaskName:     r.Dmt.TaskName,
						SchemaNameS:  r.Dmt.SchemaNameS,
						TableNameS:   r.Dmt.TableNameS,
						SchemaNameT:  r.Dmt.SchemaNameT,
						TableNameT:   r.Dmt.TableNameT,
						ChunkID:      r.Dmt.ChunkID,
						ChunkDetailS: r.Dmt.ChunkDetailS,
						FixStmtType:  constant.DataCompareFixStmtTypeDelete,
						FixDetailT:   encryptDelDetails,
					})
					if err != nil {
						return err
					}
					return nil
				}
			})
		}
		if err = gDel.Wait(); err != nil {
			return err
		}
	}

	if len(addDetails) > 0 {
		splitCounts := len(addDetails) / r.BatchSize
		if splitCounts == 0 {
			splitCounts = 1
		}
		groupAddDetails := stringutil.StringSliceSplit(addDetails, splitCounts)

		gAdd, gAddCtx := errgroup.WithContext(r.Ctx)
		gAdd.SetLimit(r.WriteThread)

		for _, gds := range groupAddDetails {
			details := gds
			gAdd.Go(func() error {
				select {
				case <-gAddCtx.Done():
					return nil
				default:
					encryptAddDetails, err := stringutil.Encrypt(stringutil.StringJoin(details, "\n"), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					_, err = model.GetIDataCompareResultRW().CreateDataCompareResult(gAddCtx, &task.DataCompareResult{
						TaskName:     r.Dmt.TaskName,
						SchemaNameS:  r.Dmt.SchemaNameS,
						TableNameS:   r.Dmt.TableNameS,
						SchemaNameT:  r.Dmt.SchemaNameT,
						TableNameT:   r.Dmt.TableNameT,
						ChunkID:      r.Dmt.ChunkID,
						ChunkDetailS: r.Dmt.ChunkDetailS,
						FixStmtType:  constant.DataCompareFixStmtTypeInsert,
						FixDetailT:   encryptAddDetails,
					})
					if err != nil {
						return err
					}
					return nil
				}
			})
		}
		if err = gAdd.Wait(); err != nil {
			return err
		}
	}

	errW := model.Transaction(r.Ctx, func(txnCtx context.Context) error {
		_, err = model.GetIDataCompareTaskRW().UpdateDataCompareTask(txnCtx,
			&task.DataCompareTask{TaskName: r.Dmt.TaskName, SchemaNameS: r.Dmt.SchemaNameS, TableNameS: r.Dmt.TableNameS, ChunkID: r.Dmt.ChunkID},
			map[string]interface{}{
				"TaskStatus": constant.TaskDatabaseStatusNotEqual,
				"Duration":   fmt.Sprintf("%f", time.Now().Sub(r.StartTime).Seconds()),
			})
		if err != nil {
			return err
		}
		_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
			TaskName:    r.Dmt.TaskName,
			SchemaNameS: r.Dmt.SchemaNameS,
			TableNameS:  r.Dmt.TableNameS,
			LogDetail: fmt.Sprintf("%v [%v] data compare task [%v] taskflow [%v] source table [%v.%v] chunk_id [%s] chunk [%s] success",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeDataCompare),
				r.Dmt.TaskName,
				r.TaskFlow,
				r.Dmt.SchemaNameS,
				r.Dmt.TableNameS,
				r.Dmt.ChunkID,
				r.Dmt.ChunkDetailS),
		})
		if err != nil {
			return err
		}
		return nil
	})
	if errW != nil {
		return errW
	}

	logger.Info("data compare task chunk compare details finished",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("compare_method", r.Dmt.CompareMethod),
		zap.String("chunk_id", r.Dmt.ChunkID),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.Any("chunk_detail_args_s", r.Dmt.ChunkDetailArgS),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.Any("chunk_detail_args_t", r.Dmt.ChunkDetailArgT),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}

// Cmp used for the src and dest store key-value pair , and key data row and value data row counts
func Cmp(src map[string]int64, dest map[string]int64) (map[string]int64, map[string]int64) {
	addedSrcSets := make(map[string]int64)
	delSrcSets := make(map[string]int64)

	// Iterate over source keys and calculate differences.
	for sk, sv := range src {
		if dv, ok := dest[sk]; ok {
			if sv != dv {
				if sv > dv {
					// src has more records than dest.
					addedSrcSets[sk] = sv - dv
				} else {
					// src has fewer records than dest.
					delSrcSets[sk] = dv - sv
				}
			}
		} else {
			// Key exists only in src.
			addedSrcSets[sk] = sv
		}
	}

	// Iterate over destination keys and add those not present in src.
	for dk, dv := range dest {
		if _, ok := src[dk]; !ok {
			// Key exists only in dest.
			delSrcSets[dk] = dv
		}
	}

	return addedSrcSets, delSrcSets
}
