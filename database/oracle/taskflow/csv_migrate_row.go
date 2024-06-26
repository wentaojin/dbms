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
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang/snappy"

	"github.com/wentaojin/dbms/proto/pb"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
)

type CsvMigrateRow struct {
	Ctx        context.Context
	TaskMode   string
	TaskFlow   string
	BufioSize  int
	Dmt        *task.DataMigrateTask
	DatabaseS  database.IDatabase
	DBCharsetS string
	DBCharsetT string
	TaskParams *pb.CsvMigrateParam
	ReadChan   chan []string
	WriteChan  chan string
}

func (r *CsvMigrateRow) MigrateRead() error {
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
		originQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
		execQuerySQL = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
	case strings.EqualFold(r.Dmt.ConsistentReadS, "YES") && !strings.EqualFold(r.Dmt.SqlHintS, ""):
		originQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
		execQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" AS OF SCN `, r.Dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
	case strings.EqualFold(r.Dmt.ConsistentReadS, "NO") && !strings.EqualFold(r.Dmt.SqlHintS, ""):
		originQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		execQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
	default:
		originQuerySQL = stringutil.StringBuilder(`SELECT `, r.Dmt.ColumnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
		execQuerySQL = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, r.Dmt.SchemaNameS, `"."`, r.Dmt.TableNameS, `" WHERE `, chunkDetailS)
	}

	logger.Info("csv migrate task chunk rows extractor starting",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", chunkDetailS),
		zap.String("sql_query_s", execQuerySQL),
		zap.String("origin_sql_s", originQuerySQL),
		zap.String("startTime", startTime.String()))

	err = r.DatabaseS.GetDatabaseTableCsvData(execQuerySQL, int(r.TaskParams.CallTimeout), r.TaskFlow, r.DBCharsetS, r.DBCharsetT, r.Dmt.ColumnDetailO, r.TaskParams.EscapeBackslash, r.TaskParams.NullValue, r.TaskParams.Separator, r.TaskParams.Delimiter, r.ReadChan)
	if err != nil {
		return fmt.Errorf("the task [%s] task_mode [%s] task_flow [%v] source sql [%v] execute failed: %v", r.Dmt.TaskName, r.TaskMode, r.TaskFlow, execQuerySQL, err)
	}

	endTime := time.Now()
	logger.Info("csv migrate task chunk rows extractor finished",
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

func (r *CsvMigrateRow) MigrateProcess() error {
	defer close(r.WriteChan)
	for rows := range r.ReadChan {
		r.WriteChan <- stringutil.StringBuilder(stringutil.StringJoin(rows, r.TaskParams.Separator), r.TaskParams.Terminator)
	}
	return nil
}

func (r *CsvMigrateRow) MigrateApply() error {
	startTime := time.Now()

	logger.Info("csv migrate task chunk rows applier starting",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", r.Dmt.ChunkDetailS),
		zap.String("startTime", startTime.String()))

	fileW, err := os.OpenFile(r.Dmt.CsvFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer fileW.Close()

	writer := bufio.NewWriterSize(fileW, r.BufioSize)
	defer writer.Flush()

	if r.TaskParams.Header {
		if _, err = writer.WriteString(stringutil.StringBuilder(
			stringutil.StringJoin(
				stringutil.StringSplit(
					r.Dmt.ColumnDetailT, constant.StringSeparatorComma),
				r.TaskParams.Separator),
			r.TaskParams.Terminator)); err != nil {
			return fmt.Errorf("failed to write csv column header: %v", err)
		}
	}

	for dataC := range r.WriteChan {
		if _, err = writer.WriteString(dataC); err != nil {
			return fmt.Errorf("failed to write data row to csv: %v", err)
		}
	}
	logger.Info("csv migrate task chunk rows applier finished",
		zap.String("task_name", r.Dmt.TaskName),
		zap.String("task_mode", r.TaskMode),
		zap.String("task_flow", r.TaskFlow),
		zap.String("schema_name_s", r.Dmt.SchemaNameS),
		zap.String("table_name_s", r.Dmt.TableNameS),
		zap.String("chunk_detail_s", r.Dmt.ChunkDetailS),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}
