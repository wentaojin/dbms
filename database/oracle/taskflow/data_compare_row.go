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
	"fmt"
	"github.com/shopspring/decimal"
	"maps"
	"strings"
	"time"

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
	Ctx         context.Context
	StartTime   time.Time
	TaskMode    string
	TaskFlow    string
	Dmt         *task.DataCompareTask
	DatabaseS   database.IDatabase
	DatabaseT   database.IDatabase
	BatchSize   int
	WriteThread int
	CallTimeout int
	DBCharsetS  string
	DBCharsetT  string
}

func (r *DataCompareRow) CompareMethod() string {
	return r.Dmt.CompareMethod
}

func (r *DataCompareRow) CompareRows() error {
	startTime := time.Now()

	var (
		execQueryS, execQueryT                                   string
		columnDetailS, columnDetailT, chunkDetailS, chunkDetailT string
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

	switch {
	case !strings.EqualFold(r.Dmt.SnapshotPointS, "") && strings.EqualFold(r.Dmt.SqlHintS, ""):
		execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
	case !strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, ""):
		execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
	case strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, ""):
		execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
	default:
		execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
	}

	switch {
	case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB):
		if !strings.EqualFold(r.Dmt.SqlHintT, "") && strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
		} else if !strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` AS OF TIMESTAMP `, r.Dmt.SnapshotPointT, ` WHERE `, chunkDetailT)
		} else if strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` AS OF TIMESTAMP `, r.Dmt.SnapshotPointT, ` WHERE `, chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
		}
	case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL):
		if !strings.EqualFold(r.Dmt.SqlHintT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
		}
	default:
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] is not supported, please contact author or reselect", r.Dmt.TaskName, r.TaskMode, r.TaskFlow)
	}

	logger.Info("data compare task chunk rows compare starting",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.String("startTime", startTime.String()))

	resultSM := make(chan string, 1)
	resultTM := make(chan string, 1)

	g, ctx := errgroup.WithContext(r.Ctx)

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
			_, resultS, err := r.DatabaseS.GeneralQuery(execQueryS)
			if err != nil {
				return fmt.Errorf("the database source query sql [%v] running failed: [%v]", execQueryS, err)
			}
			resultSM <- resultS[0]["ROWSCOUNT"]
			return nil
		}
	})

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
			_, resultT, err := r.DatabaseT.GeneralQuery(execQueryT)
			if err != nil {
				return fmt.Errorf("the database source target sql [%v] running failed: [%v]", execQueryT, err)
			}
			resultTM <- resultT[0]["ROWSCOUNT"]
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
		return fmt.Errorf("parse the database source rowcounts failed: %v", err)
	}

	resultT, err := decimal.NewFromString(resultStrT)
	if err != nil {
		return fmt.Errorf("parse the database target rowcounts failed: %v", err)
	}

	if resultS.Equal(resultT) {
		logger.Info("data compare task chunk rows compare is equaled",
			zap.String("task_name", r.Dmt.TaskName),
			zap.String("task_mode", r.TaskMode),
			zap.String("task_flow", r.TaskFlow),
			zap.String("schema_name_s", r.Dmt.SchemaNameS),
			zap.String("table_name_s", r.Dmt.TableNameS),
			zap.String("chunk_detail_s", desChunkDetailS),
			zap.String("schema_name_t", r.Dmt.SchemaNameT),
			zap.String("table_name_t", r.Dmt.TableNameT),
			zap.String("chunk_detail_t", desChunkDetailT),
			zap.String("compare_method", r.Dmt.CompareMethod),
			zap.String("table_rows_s", resultStrS),
			zap.String("table_rows_t", resultStrT),
			zap.String("cost", endTime.Sub(startTime).String()))

		errW := model.Transaction(r.Ctx, func(txnCtx context.Context) error {
			_, err = model.GetIDataCompareTaskRW().UpdateDataCompareTask(txnCtx,
				&task.DataCompareTask{TaskName: r.Dmt.TaskName, SchemaNameS: r.Dmt.SchemaNameS, TableNameS: r.Dmt.TableNameS, ChunkDetailS: r.Dmt.ChunkDetailS},
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
				LogDetail: fmt.Sprintf("%v [%v] data compare task [%v] taskflow [%v] source table [%v.%v] chunk [%s] success",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(constant.TaskModeDataCompare),
					r.Dmt.TaskName,
					r.TaskFlow,
					r.Dmt.SchemaNameS,
					r.Dmt.TableNameS,
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
		return nil
	}

	logger.Info("data compare task chunk rows compare isn't equaled",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.String("compare_method", r.Dmt.CompareMethod),
		zap.String("table_rows_s", resultStrS),
		zap.String("table_rows_t", resultStrT),
		zap.String("cost", endTime.Sub(startTime).String()))

	errW := model.Transaction(r.Ctx, func(txnCtx context.Context) error {
		_, err = model.GetIDataCompareTaskRW().UpdateDataCompareTask(txnCtx,
			&task.DataCompareTask{TaskName: r.Dmt.TaskName, SchemaNameS: r.Dmt.SchemaNameS, TableNameS: r.Dmt.TableNameS, ChunkDetailS: r.Dmt.ChunkDetailS},
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
			LogDetail: fmt.Sprintf("%v [%v] data compare task [%v] taskflow [%v] source table [%v.%v] chunk [%s] success",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeDataCompare),
				r.Dmt.TaskName,
				r.TaskFlow,
				r.Dmt.SchemaNameS,
				r.Dmt.TableNameS,
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
	return nil
}

func (r *DataCompareRow) CompareMD5() error {
	startTime := time.Now()

	var (
		execQueryS, execQueryT                                   string
		columnDetailS, columnDetailT, chunkDetailS, chunkDetailT string
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

	switch {
	case !strings.EqualFold(r.Dmt.SnapshotPointS, "") && strings.EqualFold(r.Dmt.SqlHintS, ""):
		execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
	case !strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, ""):
		execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
	case strings.EqualFold(r.Dmt.SnapshotPointS, "NO") && !strings.EqualFold(r.Dmt.SqlHintS, ""):
		execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
	default:
		execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
	}

	switch {
	case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB):
		if !strings.EqualFold(r.Dmt.SqlHintT, "") && strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
		} else if !strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` AS OF TIMESTAMP `, r.Dmt.SnapshotPointT, ` WHERE `, chunkDetailT)
		} else if strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` AS OF TIMESTAMP `, r.Dmt.SnapshotPointT, ` WHERE `, chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
		}
	case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL):
		if !strings.EqualFold(r.Dmt.SqlHintT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
		}
	default:
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] is not supported, please contact author or reselect", r.Dmt.TaskName, r.TaskMode, r.TaskFlow)
	}

	if strings.EqualFold(r.Dmt.CompareMethod, constant.DataCompareMethodCheckMD5) {
		execQueryS = fmt.Sprintf(`SELECT
	TO_CHAR(NVL(TO_NUMBER(SUM(TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 1, 8),'xxxxxxxx')+
	TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 9, 8),'xxxxxxxx')+
	TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 17, 8),'xxxxxxxx')+ 
	TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 25, 8),'xxxxxxxx'))),0)) AS ROWSCHECKSUM
FROM
	(%v) subq`, execQueryS)
		execQueryT = fmt.Sprintf(`
SELECT
	 CAST(IFNULL(SUM(CONV(SUBSTRING(subq.ROWSCHECKSUM, 1, 8),16,10)+ 
	 CONV(SUBSTRING(subq.ROWSCHECKSUM, 9, 8),16,10)+
	 CONV(SUBSTRING(subq.ROWSCHECKSUM, 17, 8),16,10)+
	 CONV(SUBSTRING(subq.ROWSCHECKSUM, 25, 8),16,10)),0) AS CHAR) AS ROWSCHECKSUM
FROM
	(%v) subq`, execQueryT)
	}

	logger.Info("data compare task chunk md5 compare starting",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.String("startTime", startTime.String()))

	resultSM := make(chan string, 1)
	resultTM := make(chan string, 1)

	g, ctx := errgroup.WithContext(r.Ctx)

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
			_, resultS, err := r.DatabaseS.GeneralQuery(execQueryS)
			if err != nil {
				return fmt.Errorf("the database source query sql [%v] running failed: [%v]", execQueryS, err)
			}
			resultSM <- resultS[0]["ROWSCHECKSUM"]
			return nil
		}
	})

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
			_, resultT, err := r.DatabaseT.GeneralQuery(execQueryT)
			if err != nil {
				return fmt.Errorf("the database source target sql [%v] running failed: [%v]", execQueryT, err)
			}
			resultTM <- resultT[0]["ROWSCHECKSUM"]
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
		return fmt.Errorf("parse the database source failed: %v", err)
	}

	resultT, err := decimal.NewFromString(resultStrT)
	if err != nil {
		return fmt.Errorf("parse the database target failed: %v", err)
	}
	if resultS.Equal(resultT) {
		logger.Info("data compare task chunk md5 compare is equaled",
			zap.String("task_name", r.Dmt.TaskName),
			zap.String("task_mode", r.TaskMode),
			zap.String("task_flow", r.TaskFlow),
			zap.String("schema_name_s", r.Dmt.SchemaNameS),
			zap.String("table_name_s", r.Dmt.TableNameS),
			zap.String("chunk_detail_s", desChunkDetailS),
			zap.String("schema_name_t", r.Dmt.SchemaNameT),
			zap.String("table_name_t", r.Dmt.TableNameT),
			zap.String("chunk_detail_t", desChunkDetailT),
			zap.String("compare_method", r.Dmt.CompareMethod),
			zap.String("table_md5_s", resultStrS),
			zap.String("table_md5_t", resultStrT),
			zap.String("cost", endTime.Sub(startTime).String()))

		errW := model.Transaction(r.Ctx, func(txnCtx context.Context) error {
			_, err = model.GetIDataCompareTaskRW().UpdateDataCompareTask(txnCtx,
				&task.DataCompareTask{TaskName: r.Dmt.TaskName, SchemaNameS: r.Dmt.SchemaNameS, TableNameS: r.Dmt.TableNameS, ChunkDetailS: r.Dmt.ChunkDetailS},
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
				LogDetail: fmt.Sprintf("%v [%v] data compare task [%v] taskflow [%v] source table [%v.%v] chunk [%s] success",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(constant.TaskModeDataCompare),
					r.Dmt.TaskName,
					r.TaskFlow,
					r.Dmt.SchemaNameS,
					r.Dmt.TableNameS,
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
		return nil
	}

	logger.Info("data compare task chunk md5 compare isn't equaled",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.String("compare_method", r.Dmt.CompareMethod),
		zap.String("table_md5_s", resultStrS),
		zap.String("table_md5_t", resultStrT),
		zap.String("cost", endTime.Sub(startTime).String()))

	err = r.compareMd5Row()
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

	switch {
	case !strings.EqualFold(r.Dmt.SnapshotPointS, "") && strings.EqualFold(r.Dmt.SqlHintS, ""):
		execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
	case !strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, ""):
		execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
	case strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, ""):
		execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
	default:
		execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
	}

	switch {
	case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB):
		if !strings.EqualFold(r.Dmt.SqlHintT, "") && strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
		} else if !strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` AS OF TIMESTAMP `, r.Dmt.SnapshotPointT, ` WHERE `, chunkDetailT)
		} else if strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` AS OF TIMESTAMP `, r.Dmt.SnapshotPointT, ` WHERE `, chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
		}
	case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL):
		if !strings.EqualFold(r.Dmt.SqlHintT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
		}
	default:
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] is not supported, please contact author or reselect", r.Dmt.TaskName, r.TaskMode, r.TaskFlow)
	}

	logger.Info("data compare task chunk crc32 compare details starting",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.String("startTime", startTime.String()))

	crc32ValSC := make(chan uint32, 1)
	columnDataSMC := make(chan map[string]int64, 1)
	columnDataTMC := make(chan map[string]int64, 1)
	columnNameTC := make(chan []string, 1)
	crc32ValTC := make(chan uint32, 1)

	g, ctx := errgroup.WithContext(r.Ctx)

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
			_, crc32ValS, columnDataS, err := r.DatabaseS.GetDatabaseTableCompareData(execQueryS, r.CallTimeout, r.DBCharsetS, constant.CharsetUTF8MB4)
			if err != nil {
				return fmt.Errorf("the database source query sql [%v] comparing failed: [%v]", execQueryS, err)
			}
			crc32ValSC <- crc32ValS
			columnDataSMC <- columnDataS
			return nil
		}
	})

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
			columnT, crc32ValT, columnDataT, err := r.DatabaseT.GetDatabaseTableCompareData(execQueryT, r.CallTimeout, r.DBCharsetT, constant.CharsetUTF8MB4)
			if err != nil {
				return fmt.Errorf("the database target query sql [%v] comparing failed: [%v]", execQueryT, err)
			}
			columnNameTC <- columnT
			crc32ValTC <- crc32ValT
			columnDataTMC <- columnDataT
			return nil
		}
	})

	if err = g.Wait(); err != nil {
		return err
	}

	columnDataSM := <-columnDataSMC
	crc32VS := <-crc32ValSC

	columnDataTM := <-columnDataTMC
	crc32VT := <-crc32ValTC
	columnNameT := <-columnNameTC

	endTime := time.Now()

	if crc32VS == crc32VT {
		logger.Info("data compare task chunk crc32 compare is equaled",
			zap.String("task_name", r.Dmt.TaskName),
			zap.String("task_mode", r.TaskMode),
			zap.String("task_flow", r.TaskFlow),
			zap.String("schema_name_s", r.Dmt.SchemaNameS),
			zap.String("table_name_s", r.Dmt.TableNameS),
			zap.String("chunk_detail_s", desChunkDetailS),
			zap.String("schema_name_t", r.Dmt.SchemaNameT),
			zap.String("table_name_t", r.Dmt.TableNameT),
			zap.String("chunk_detail_t", desChunkDetailT),
			zap.String("compare_method", r.Dmt.CompareMethod),
			zap.Uint32("table_crc32_s", crc32VS),
			zap.Uint32("table_crc32_t", crc32VT),
			zap.String("cost", endTime.Sub(startTime).String()))

		errW := model.Transaction(r.Ctx, func(txnCtx context.Context) error {
			_, err = model.GetIDataCompareTaskRW().UpdateDataCompareTask(txnCtx,
				&task.DataCompareTask{TaskName: r.Dmt.TaskName, SchemaNameS: r.Dmt.SchemaNameS, TableNameS: r.Dmt.TableNameS, ChunkDetailS: r.Dmt.ChunkDetailS},
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
				LogDetail: fmt.Sprintf("%v [%v] data compare task [%v] taskflow [%v] source table [%v.%v] chunk [%s] success",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(constant.TaskModeDataCompare),
					r.Dmt.TaskName,
					r.TaskFlow,
					r.Dmt.SchemaNameS,
					r.Dmt.TableNameS,
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
		return nil
	}

	logger.Info("data compare task chunk crc32 compare isn't equaled",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.String("compare_method", r.Dmt.CompareMethod),
		zap.Uint32("table_crc32_s", crc32VS),
		zap.Uint32("table_crc32_t", crc32VT),
		zap.String("cost", endTime.Sub(startTime).String()))

	addDestSets, delDestSets := Cmp(columnDataTM, columnDataSM)

	var (
		addDetails []string
		delDetails []string
	)
	for dk, dv := range addDestSets {
		switch {
		case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB):
			addDetails = append(addDetails, GenMYSQLCompatibleDatabaseInsertStmtSQL(
				r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, dk, int(dv), false))
		default:
			return fmt.Errorf("the data compare task [%s] task_flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow)
		}

	}
	for dk, dv := range delDestSets {
		switch {
		case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB):
			delDetails = append(delDetails, GenMYSQLCompatibleDatabaseDeleteStmtSQL(
				r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, stringutil.StringSplit(dk, constant.StringSeparatorComma), int(dv)))
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
					encryptDelDetails, err := stringutil.Encrypt(stringutil.StringJoin(details, constant.StringSeparatorSemicolon+"\n"), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					_, err = model.GetIDataCompareResultRW().CreateDataCompareResult(gDelCtx, &task.DataCompareResult{
						TaskName:     r.Dmt.TaskName,
						SchemaNameS:  r.Dmt.SchemaNameS,
						TableNameS:   r.Dmt.TableNameS,
						SchemaNameT:  r.Dmt.SchemaNameT,
						TableNameT:   r.Dmt.TableNameT,
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
		splitCounts := len(delDetails) / r.BatchSize
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
					encryptAddDetails, err := stringutil.Encrypt(stringutil.StringJoin(details, constant.StringSeparatorSemicolon+"\n"), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					_, err = model.GetIDataCompareResultRW().CreateDataCompareResult(gAddCtx, &task.DataCompareResult{
						TaskName:     r.Dmt.TaskName,
						SchemaNameS:  r.Dmt.SchemaNameS,
						TableNameS:   r.Dmt.TableNameS,
						SchemaNameT:  r.Dmt.SchemaNameT,
						TableNameT:   r.Dmt.TableNameT,
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
			&task.DataCompareTask{TaskName: r.Dmt.TaskName, SchemaNameS: r.Dmt.SchemaNameS, TableNameS: r.Dmt.TableNameS, ChunkDetailS: r.Dmt.ChunkDetailS},
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
			LogDetail: fmt.Sprintf("%v [%v] data compare task [%v] taskflow [%v] source table [%v.%v] chunk [%s] success",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeDataCompare),
				r.Dmt.TaskName,
				r.TaskFlow,
				r.Dmt.SchemaNameS,
				r.Dmt.TableNameS,
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

	logger.Info("data compare task chunk crc32 compare details finished",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}

func (r *DataCompareRow) compareMd5Row() error {
	startTime := time.Now()

	var (
		execQueryS, execQueryT                                   string
		columnDetailS, columnDetailT, chunkDetailS, chunkDetailT string
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

	switch {
	case !strings.EqualFold(r.Dmt.SnapshotPointS, "") && strings.EqualFold(r.Dmt.SqlHintS, ""):
		execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
	case !strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, ""):
		execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
	case strings.EqualFold(r.Dmt.SnapshotPointS, "") && !strings.EqualFold(r.Dmt.SqlHintS, ""):
		execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
	default:
		execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
	}

	switch {
	case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB):
		if !strings.EqualFold(r.Dmt.SqlHintT, "") && strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
		} else if !strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` AS OF TIMESTAMP `, r.Dmt.SnapshotPointT, ` WHERE `, chunkDetailT)
		} else if strings.EqualFold(r.Dmt.SqlHintT, "") && !strings.EqualFold(r.Dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` AS OF TIMESTAMP `, r.Dmt.SnapshotPointT, ` WHERE `, chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
		}
	case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL):
		if !strings.EqualFold(r.Dmt.SqlHintT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
		}
	default:
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] is not supported, please contact author or reselect", r.Dmt.TaskName, r.TaskMode, r.TaskFlow)
	}

	logger.Info("data compare task chunk md5 compare details starting",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.String("startTime", startTime.String()))

	columnDataSMC := make(chan map[string]int64, 1)
	columnDataTMC := make(chan map[string]int64, 1)
	columnNameTC := make(chan []string, 1)

	g, ctx := errgroup.WithContext(r.Ctx)

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
			_, _, columnDataS, err := r.DatabaseS.GetDatabaseTableCompareData(execQueryS, r.CallTimeout, r.DBCharsetS, constant.CharsetUTF8MB4)
			if err != nil {
				return fmt.Errorf("the database source query sql [%v] comparing failed: [%v]", execQueryS, err)
			}
			columnDataSMC <- columnDataS
			return nil
		}
	})

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
			columnT, _, columnDataT, err := r.DatabaseT.GetDatabaseTableCompareData(execQueryT, r.CallTimeout, r.DBCharsetT, constant.CharsetUTF8MB4)
			if err != nil {
				return fmt.Errorf("the database target query sql [%v] comparing failed: [%v]", execQueryT, err)
			}
			columnNameTC <- columnT
			columnDataTMC <- columnDataT
			return nil
		}
	})

	if err = g.Wait(); err != nil {
		return err
	}

	columnDataSM := <-columnDataSMC

	columnDataTM := <-columnDataTMC
	columnNameT := <-columnNameTC

	addDestSets, delDestSets := Cmp(columnDataTM, columnDataSM)

	var (
		addDetails []string
		delDetails []string
	)
	for dk, dv := range addDestSets {
		switch {
		case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB):
			addDetails = append(addDetails, GenMYSQLCompatibleDatabaseInsertStmtSQL(
				r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, dk, int(dv), false))
		default:
			return fmt.Errorf("the data compare task [%s] task_flow [%s] isn't support, please contact author or reselect", r.Dmt.TaskName, r.TaskFlow)
		}

	}

	for dk, dv := range delDestSets {
		switch {
		case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB):
			delDetails = append(delDetails, GenMYSQLCompatibleDatabaseDeleteStmtSQL(
				r.Dmt.SchemaNameT, r.Dmt.TableNameT, "", columnNameT, stringutil.StringSplit(dk, constant.StringSeparatorComma), int(dv)))
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
					encryptDelDetails, err := stringutil.Encrypt(stringutil.StringJoin(details, constant.StringSeparatorSemicolon+"\n"), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					_, err = model.GetIDataCompareResultRW().CreateDataCompareResult(gDelCtx, &task.DataCompareResult{
						TaskName:     r.Dmt.TaskName,
						SchemaNameS:  r.Dmt.SchemaNameS,
						TableNameS:   r.Dmt.TableNameS,
						SchemaNameT:  r.Dmt.SchemaNameT,
						TableNameT:   r.Dmt.TableNameT,
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
		splitCounts := len(delDetails) / r.BatchSize
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
					encryptAddDetails, err := stringutil.Encrypt(stringutil.StringJoin(details, constant.StringSeparatorSemicolon+"\n"), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					_, err = model.GetIDataCompareResultRW().CreateDataCompareResult(gAddCtx, &task.DataCompareResult{
						TaskName:     r.Dmt.TaskName,
						SchemaNameS:  r.Dmt.SchemaNameS,
						TableNameS:   r.Dmt.TableNameS,
						SchemaNameT:  r.Dmt.SchemaNameT,
						TableNameT:   r.Dmt.TableNameT,
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
			&task.DataCompareTask{TaskName: r.Dmt.TaskName, SchemaNameS: r.Dmt.SchemaNameS, TableNameS: r.Dmt.TableNameS, ChunkDetailS: r.Dmt.ChunkDetailS},
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
			LogDetail: fmt.Sprintf("%v [%v] data compare task [%v] taskflow [%v] source table [%v.%v] chunk [%s] success",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeDataCompare),
				r.Dmt.TaskName,
				r.TaskFlow,
				r.Dmt.SchemaNameS,
				r.Dmt.TableNameS,
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

	logger.Info("data compare task chunk md5 compare details finished",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}

// Cmp used for the src and dest store key-value pair , and key data row and value data row counts
func Cmp(src map[string]int64, dest map[string]int64) (map[string]int64, map[string]int64) {
	// source element map mall
	columnDataMall := maps.Clone(src) // source and target element map temp malls

	addedSrcSets := make(map[string]int64)
	delSrcSets := make(map[string]int64)

	// data intersection
	intersectionSets := make(map[string]int64)

	// data comparison through set operations
	for dk, dv := range dest {
		if val, exist := columnDataMall[dk]; exist {
			if dv == val {
				intersectionSets[dk] = dv
			} else if dv < val {
				// target records is less than source records
				delSrcSets[dk] = val - dv
				delete(src, dk)
				delete(columnDataMall, dk)
			} else {
				// target records is more than source records
				addedSrcSets[dk] = dv - val
				delete(src, dk)
				delete(columnDataMall, dk)
			}
		} else {
			columnDataMall[dk] = dv
		}
	}

	for dk, _ := range intersectionSets {
		delete(columnDataMall, dk)
	}

	for dk, dv := range columnDataMall {
		if _, exist := src[dk]; exist {
			delSrcSets[dk] = dv
		} else {
			addedSrcSets[dk] = dv
		}
	}
	return addedSrcSets, delSrcSets
}
