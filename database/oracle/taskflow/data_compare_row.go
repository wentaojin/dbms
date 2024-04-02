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
	"maps"
	"strconv"
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
	CallTimeout int
	DBCharsetS  string
	DBCharsetT  string
}

func (r *DataCompareRow) CompareRows() (bool, error) {
	startTime := time.Now()

	var (
		execQueryS, execQueryT                                   string
		columnDetailS, columnDetailT, chunkDetailS, chunkDetailT string
	)

	desChunkDetailS, err := stringutil.Decrypt(r.Dmt.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return false, err
	}
	decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
	if err != nil {
		return false, err
	}
	desChunkDetailS = stringutil.BytesToString(decChunkDetailS)

	desChunkDetailT, err := stringutil.Decrypt(r.Dmt.ChunkDetailT, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return false, err
	}
	decChunkDetailT, err := snappy.Decode(nil, []byte(desChunkDetailT))
	if err != nil {
		return false, err
	}

	desChunkDetailT = stringutil.BytesToString(decChunkDetailT)

	convertRaw, err := stringutil.CharsetConvert([]byte(r.Dmt.ColumnDetailS), constant.CharsetUTF8MB4, r.DBCharsetS)
	if err != nil {
		return false, fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	columnDetailS = stringutil.BytesToString(convertRaw)
	convertRaw, err = stringutil.CharsetConvert([]byte(desChunkDetailS), constant.CharsetUTF8MB4, r.DBCharsetS)
	if err != nil {
		return false, fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	chunkDetailS = stringutil.BytesToString(convertRaw)

	convertRaw, err = stringutil.CharsetConvert([]byte(r.Dmt.ColumnDetailT), constant.CharsetUTF8MB4, r.DBCharsetT)
	if err != nil {
		return false, fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	columnDetailT = stringutil.BytesToString(convertRaw)
	convertRaw, err = stringutil.CharsetConvert([]byte(desChunkDetailT), constant.CharsetUTF8MB4, r.DBCharsetT)
	if err != nil {
		return false, fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, r.Dmt.SchemaNameS, r.Dmt.TableNameS, r.Dmt.ColumnDetailS, err)
	}
	chunkDetailT = stringutil.BytesToString(convertRaw)

	switch {
	case strings.EqualFold(r.Dmt.ConsistentReadS, "YES") && strings.EqualFold(r.Dmt.SqlHintS, ""):
		execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, strconv.FormatUint(r.Dmt.GlobalScnS, 10), ` WHERE `, chunkDetailS)
	case strings.EqualFold(r.Dmt.ConsistentReadS, "YES") && !strings.EqualFold(r.Dmt.SqlHintS, ""):
		execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, strconv.FormatUint(r.Dmt.GlobalScnS, 10), ` WHERE `, chunkDetailS)
	case strings.EqualFold(r.Dmt.ConsistentReadS, "NO") && !strings.EqualFold(r.Dmt.SqlHintS, ""):
		execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
	default:
		execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
	}

	if !strings.EqualFold(r.Dmt.SqlHintT, "") {
		execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
	} else {
		execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
	}

	if strings.EqualFold(r.Dmt.CompareMethod, constant.DataCompareMethodCheckSum) {
		execQueryS = fmt.Sprintf(`SELECT
	NVL(SUM(TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 1, 8),'xxxxxxxx')+
	TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 9, 8),'xxxxxxxx')+
	TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 17, 8),'xxxxxxxx')+ 
	TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 25, 8),'xxxxxxxx')),0) AS ROWSCHECKSUM
FROM
	(%v) subq`, execQueryS)
		execQueryT = fmt.Sprintf(`
SELECT
	IFNULL(SUM(CONV(SUBSTRING(subq.ROWSCHECKSUM, 1, 8),16,10)+ 
	 CONV(SUBSTRING(subq.ROWSCHECKSUM, 9, 8),16,10)+
	 CONV(SUBSTRING(subq.ROWSCHECKSUM, 17, 8),16,10)+
	 CONV(SUBSTRING(subq.ROWSCHECKSUM, 25, 8),16,10)),0) AS ROWSCHECKSUM
FROM
	(%v) subq`, execQueryT)
	}

	logger.Info("data compare task chunk rows compare starting",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("column_name_s", r.Dmt.ColumnDetailS),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("column_name_t", r.Dmt.ColumnDetailT),
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
			if strings.EqualFold(r.Dmt.CompareMethod, constant.DataCompareMethodCheckRows) {
				resultSM <- resultS[0]["ROWSCOUNT"]
			}
			if strings.EqualFold(r.Dmt.CompareMethod, constant.DataCompareMethodCheckSum) {
				resultSM <- resultS[0]["ROWSCHECKSUM"]
			}
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
			if strings.EqualFold(r.Dmt.CompareMethod, constant.DataCompareMethodCheckRows) {
				resultTM <- resultT[0]["ROWSCOUNT"]
			}
			if strings.EqualFold(r.Dmt.CompareMethod, constant.DataCompareMethodCheckSum) {
				resultTM <- resultT[0]["ROWSCHECKSUM"]
			}
			return nil
		}
	})

	if err = g.Wait(); err != nil {
		return false, err
	}

	resultStrS := <-resultSM
	resultStrT := <-resultTM

	endTime := time.Now()

	if resultStrS == resultStrT {
		logger.Info("data compare task chunk rows compare is equaled",
			zap.String("task_name", r.Dmt.TaskName),
			zap.String("task_mode", r.TaskMode),
			zap.String("task_flow", r.TaskFlow),
			zap.String("schema_name_s", r.Dmt.SchemaNameS),
			zap.String("table_name_s", r.Dmt.TableNameS),
			zap.String("column_name_s", r.Dmt.ColumnDetailS),
			zap.String("chunk_detail_s", desChunkDetailS),
			zap.String("schema_name_t", r.Dmt.SchemaNameT),
			zap.String("table_name_t", r.Dmt.TableNameT),
			zap.String("column_name_t", r.Dmt.ColumnDetailT),
			zap.String("chunk_detail_t", desChunkDetailT),
			zap.String("compare_method", r.Dmt.CompareMethod),
			zap.String("count_checksum_source", resultStrS),
			zap.String("count_checksum_target", resultStrT),
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
			return false, errW
		}
		return true, nil
	}

	logger.Info("data compare task chunk rows compare isn't equaled",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("column_name_s", r.Dmt.ColumnDetailS),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("column_name_t", r.Dmt.ColumnDetailT),
		zap.String("chunk_detail_t", desChunkDetailT),
		zap.String("compare_method", r.Dmt.CompareMethod),
		zap.String("count_checksum_source", resultStrS),
		zap.String("count_checksum_target", resultStrT),
		zap.String("cost", endTime.Sub(startTime).String()))
	return false, nil
}

func (r *DataCompareRow) CompareDiff() error {
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

	if !strings.EqualFold(r.Dmt.SqlHintS, "") {
		execQueryS = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
	} else {
		execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
	}

	if !strings.EqualFold(r.Dmt.SqlHintT, "") {
		execQueryT = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintT, ` `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
	} else {
		execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM `, r.Dmt.SchemaNameT, `.`, r.Dmt.TableNameT, ` WHERE `, chunkDetailT)
	}

	logger.Info("data compare task chunk rows compare details starting",
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

	columnNameSC := make(chan []string, 1)
	columnDataSMC := make(chan map[string]int64, 1)
	columnDataTMC := make(chan map[string]int64, 1)
	columnNameTC := make(chan []string, 1)

	g, ctx := errgroup.WithContext(r.Ctx)

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
			columnS, columnDataS, err := r.DatabaseS.GetDatabaseTableColumnDataCompare(execQueryS, r.CallTimeout, r.DBCharsetS, constant.CharsetUTF8MB4)
			if err != nil {
				return fmt.Errorf("the database source query sql [%v] comparing failed: [%v]", execQueryS, err)
			}
			columnNameSC <- columnS
			columnDataSMC <- columnDataS
			return nil
		}
	})

	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
			columnT, columnDataT, err := r.DatabaseT.GetDatabaseTableColumnDataCompare(execQueryT, r.CallTimeout, r.DBCharsetT, constant.CharsetUTF8MB4)
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
	columnNameS := <-columnNameSC

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

	if len(addDetails) > 0 {
		encryptAddDetails, err := stringutil.Encrypt(stringutil.StringJoin(addDetails, constant.StringSeparatorSemicolon+"\n"), []byte(constant.DefaultDataEncryptDecryptKey))
		if err != nil {
			return err
		}
		_, err = model.GetIDataCompareTaskRW().UpdateDataCompareTask(r.Ctx, &task.DataCompareTask{
			TaskName:     r.Dmt.TaskName,
			SchemaNameS:  r.Dmt.SchemaNameS,
			TableNameS:   r.Dmt.TableNameS,
			ChunkDetailS: r.Dmt.ChunkDetailS,
		}, map[string]interface{}{
			"FixDetailAddT": encryptAddDetails,
		})
		if err != nil {
			return err
		}
	}

	if len(delDetails) > 0 {
		encryptDelDetails, err := stringutil.Encrypt(stringutil.StringJoin(delDetails, constant.StringSeparatorSemicolon+"\n"), []byte(constant.DefaultDataEncryptDecryptKey))
		if err != nil {
			return err
		}
		_, err = model.GetIDataCompareTaskRW().UpdateDataCompareTask(r.Ctx, &task.DataCompareTask{
			TaskName:     r.Dmt.TaskName,
			SchemaNameS:  r.Dmt.SchemaNameS,
			TableNameS:   r.Dmt.TableNameS,
			ChunkDetailS: r.Dmt.ChunkDetailS,
		}, map[string]interface{}{
			"FixDetailDelT": encryptDelDetails,
		})
		if err != nil {
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

	logger.Info("data compare task chunk rows compare details finished",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("column_name_s", stringutil.StringJoin(columnNameS, constant.StringSeparatorComma)),
		zap.String("schema_name_t", r.Dmt.SchemaNameT),
		zap.String("table_name_t", r.Dmt.TableNameT),
		zap.String("column_name_t", stringutil.StringJoin(columnNameT, constant.StringSeparatorComma)),
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
