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
	"strings"
	"time"

	"github.com/golang/snappy"

	"github.com/wentaojin/dbms/model"

	"github.com/wentaojin/dbms/database"

	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/utils/stringutil"

	"go.uber.org/zap"

	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"
)

type DataScanRow struct {
	Ctx        context.Context
	StartTime  time.Time
	TaskName   string
	TaskMode   string
	TaskFlow   string
	Dst        *task.DataScanTask
	DatabaseS  database.IDatabase
	DBCharsetS string
}

func (r *DataScanRow) ScanRows() error {
	startTime := time.Now()

	var (
		execQueryS                  string
		columnDetailS, chunkDetailS string
	)

	desChunkDetailS, err := stringutil.Decrypt(r.Dst.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
	if err != nil {
		return err
	}
	desChunkDetailS = stringutil.BytesToString(decChunkDetailS)

	convertRaw, err := stringutil.CharsetConvert([]byte(r.Dst.ColumnDetailS), constant.CharsetUTF8MB4, constant.MigrateOracleCharsetStringConvertMapping[r.DBCharsetS])
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dst.TaskName, r.TaskMode, r.TaskFlow, r.Dst.SchemaNameS, r.Dst.TableNameS, r.Dst.ColumnDetailS, err)
	}
	columnDetailS = stringutil.BytesToString(convertRaw)
	convertRaw, err = stringutil.CharsetConvert([]byte(desChunkDetailS), constant.CharsetUTF8MB4, constant.MigrateOracleCharsetStringConvertMapping[r.DBCharsetS])
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] schema [%s] table [%s] column [%s] charset convert failed, %v", r.Dst.TaskName, r.TaskMode, r.TaskFlow, r.Dst.SchemaNameS, r.Dst.TableNameS, r.Dst.ColumnDetailS, err)
	}
	chunkDetailS = stringutil.BytesToString(convertRaw)

	// sample scan
	if strings.EqualFold(chunkDetailS, "sample_scan") && !strings.EqualFold(r.Dst.Samplerate, "") {
		switch {
		case strings.EqualFold(r.Dst.ConsistentReadS, "YES") && strings.EqualFold(r.Dst.SqlHintS, ""):
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dst.SchemaNameS, `"."`, r.Dst.TableNameS, `" SAMPLE(`, r.Dst.Samplerate, `) AS OF SCN `, r.Dst.SnapshotPointS)
		case strings.EqualFold(r.Dst.ConsistentReadS, "YES") && !strings.EqualFold(r.Dst.SqlHintS, ""):
			execQueryS = stringutil.StringBuilder(`SELECT `, r.Dst.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dst.SchemaNameS, `"."`, r.Dst.TableNameS, `" SAMPLE(`, r.Dst.Samplerate, `) AS OF SCN `, r.Dst.SnapshotPointS)
		case strings.EqualFold(r.Dst.ConsistentReadS, "NO") && !strings.EqualFold(r.Dst.SqlHintS, ""):
			execQueryS = stringutil.StringBuilder(`SELECT `, r.Dst.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dst.SchemaNameS, `"."`, r.Dst.TableNameS, `" SAMPLE(`, r.Dst.Samplerate, `)`)
		default:
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dst.SchemaNameS, `"."`, r.Dst.TableNameS, `" SAMPLE(`, r.Dst.Samplerate, `)`)
		}
	} else {
		switch {
		case strings.EqualFold(r.Dst.ConsistentReadS, "YES") && strings.EqualFold(r.Dst.SqlHintS, ""):
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dst.SchemaNameS, `"."`, r.Dst.TableNameS, `" AS OF SCN `, r.Dst.SnapshotPointS, ` WHERE `, chunkDetailS)
		case strings.EqualFold(r.Dst.ConsistentReadS, "YES") && !strings.EqualFold(r.Dst.SqlHintS, ""):
			execQueryS = stringutil.StringBuilder(`SELECT `, r.Dst.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dst.SchemaNameS, `"."`, r.Dst.TableNameS, `" AS OF SCN `, r.Dst.SnapshotPointS, ` WHERE `, chunkDetailS)
		case strings.EqualFold(r.Dst.ConsistentReadS, "NO") && !strings.EqualFold(r.Dst.SqlHintS, ""):
			execQueryS = stringutil.StringBuilder(`SELECT `, r.Dst.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dst.SchemaNameS, `"."`, r.Dst.TableNameS, `" WHERE `, chunkDetailS)
		default:
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dst.SchemaNameS, `"."`, r.Dst.TableNameS, `" WHERE `, chunkDetailS)
		}
	}

	var (
		groupColumnSli []string
		groupingSetSli []string
		sumColumnSli   []string
	)
	groupColumns := stringutil.StringSplit(r.Dst.GroupColumnS, constant.StringSeparatorComma)
	for _, c := range groupColumns {
		groupingSetSli = append(groupingSetSli, fmt.Sprintf("(%s)", c))
		groupColumnSli = append(groupColumnSli, c)
		columnCategorySli := stringutil.StringSplit(c, constant.StringSeparatorUnderline)
		sumColumnSli = append(sumColumnSli, fmt.Sprintf("SUM(CASE WHEN %s IS NULL THEN 0 ELSE COUNTS END) AS %s", c, columnCategorySli[0]))
	}

	execQueryS = fmt.Sprintf(`WITH CV AS (
%v
)
SELECT 
	%s AS CATEGORY,
	%s
FROM
    (SELECT
		%s,
		COUNT(*) COUNTS
	FROM
		CV
	GROUP BY
		GROUPING SETS(%s)) V GROUP BY %s`, execQueryS, stringutil.StringJoin(groupColumnSli, constant.StringSplicingSymbol), stringutil.StringJoin(sumColumnSli, constant.StringSeparatorComma),
		stringutil.StringJoin(groupColumnSli, constant.StringSeparatorComma), stringutil.StringJoin(groupingSetSli, constant.StringSeparatorComma), stringutil.StringJoin(groupColumnSli, constant.StringSplicingSymbol))

	logger.Info("data scan task chunk rows scan starting",
		zap.String("task_name", r.Dst.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dst.SchemaNameS),
		zap.String("table_name_s", r.Dst.TableNameS),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.String("startTime", startTime.String()))

	cols, resultS, err := r.DatabaseS.GeneralQuery(execQueryS)
	if err != nil {
		return fmt.Errorf("the database source query sql [%v] running failed: [%v]", execQueryS, err)
	}

	var jsonStr string

	switch {
	case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB):
		excludeCategory := cols[1:]
		var scanResults []*ScanResultMYSQLCompatible

		for _, col := range excludeCategory {
			srm := &ScanResultMYSQLCompatible{}
			srm.ColumnName = col
			for _, res := range resultS {
				switch stringutil.StringUpper(res["CATEGORY"]) {
				case "BIGINT":
					srm.Bigint = res[col]
				case "BIGINT_UNSIGNED":
					srm.BigintUnsigned = res[col]
				case "DECIMAL_INT":
					srm.DecimalInt = res[col]
				case "DECIMAL_POINT":
					srm.DecimalPoint = res[col]
				case "UNKNOWN":
					srm.Unknown = res[col]
				}
			}
			scanResults = append(scanResults, srm)
		}

		jsonStr, err = stringutil.MarshalJSON(scanResults)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] isn't support, please contact author or reselect", r.TaskName, r.TaskMode, r.TaskFlow)
	}

	endTime := time.Now()
	logger.Info("data scan task chunk rows scan finished",
		zap.String("task_name", r.Dst.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dst.SchemaNameS),
		zap.String("table_name_s", r.Dst.TableNameS),
		zap.String("chunk_detail_s", desChunkDetailS),
		zap.String("cost", endTime.Sub(startTime).String()))

	errW := model.Transaction(r.Ctx, func(txnCtx context.Context) error {
		_, err = model.GetIDataScanTaskRW().UpdateDataScanTask(txnCtx,
			&task.DataScanTask{TaskName: r.TaskName, SchemaNameS: r.Dst.SchemaNameS, TableNameS: r.Dst.TableNameS, ChunkDetailS: r.Dst.ChunkDetailS},
			map[string]interface{}{
				"TaskStatus": constant.TaskDatabaseStatusSuccess,
				"ScanResult": jsonStr,
				"Duration":   fmt.Sprintf("%f", time.Now().Sub(r.StartTime).Seconds()),
			})
		if err != nil {
			return err
		}
		_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
			TaskName:    r.TaskName,
			SchemaNameS: r.Dst.SchemaNameS,
			TableNameS:  r.Dst.TableNameS,
			LogDetail: fmt.Sprintf("%v [%v] data scan task [%v] taskflow [%v] source table [%v.%v] chunk [%s] success",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeDataScan),
				r.TaskName,
				r.TaskMode,
				r.Dst.SchemaNameS,
				r.Dst.TableNameS,
				r.Dst.ChunkDetailS),
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

type ScanResultMYSQLCompatible struct {
	ColumnName     string
	Bigint         string
	BigintUnsigned string
	DecimalInt     string
	DecimalPoint   string
	Unknown        string
}
