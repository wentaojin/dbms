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
	"github.com/jedib0t/go-pretty/v6/table"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"
)

type DataScanFile struct {
	Ctx        context.Context `json:"-"`
	Mutex      *sync.Mutex     `json:"-"`
	CompFile   *os.File        `json:"-"`
	CompWriter *bufio.Writer   `json:"-"`
	TaskName   string          `json:"taskName"`
	TaskFlow   string          `json:"taskFlow"`
	OutputDir  string          `json:"outputDir"`
}

func NewDataScanFile(ctx context.Context,
	taskName, taskFlow, outputDir string) *DataScanFile {
	return &DataScanFile{
		Ctx:       ctx,
		TaskName:  taskName,
		TaskFlow:  taskFlow,
		OutputDir: outputDir,
		Mutex:     &sync.Mutex{},
	}
}

func (s *DataScanFile) InitFile() error {
	err := s.initOutputScanFile()
	if err != nil {
		return err
	}
	return nil
}

func (s *DataScanFile) SyncFile() error {
	var (
		err          error
		migrateTasks []*task.DataScanTask
	)
	// get migrate task tables
	migrateTasks, err = model.GetIDataScanTaskRW().QueryDataScanTaskByTaskStatus(s.Ctx, &task.DataScanTask{
		TaskName: s.TaskName, TaskStatus: constant.TaskDatabaseStatusFailed})
	if err != nil {
		return err
	}
	if len(migrateTasks) > 0 {
		fmt.Printf("the data scan task are existed failed records, the failed records counts [%d], please see the [data_scan_task] detail\n", len(migrateTasks))
	}

	migrateTasks, err = model.GetIDataScanTaskRW().QueryDataScanTaskByTaskStatus(s.Ctx, &task.DataScanTask{
		TaskName: s.TaskName, TaskStatus: constant.TaskDatabaseStatusSuccess})
	if err != nil {
		return err
	}

	schemaTableColsM := make(map[string]string)
	for _, mt := range migrateTasks {
		schemaTableColsM[stringutil.StringBuilder(mt.SchemaNameS, constant.StringSeparatorAite, mt.TableNameS)] = mt.GroupColumnS
	}

	var sqlComp strings.Builder

	if len(schemaTableColsM) > 0 {
		sqlComp.WriteString("/*\n")
		sqlComp.WriteString(" the database schema table struct number datatype scan detail.\n")
		sqlComp.WriteString(fmt.Sprintf(" 1, the data scan task_name [%s] task_flow [%s] scan tables [%d] output dir [%s].\n", s.TaskName, s.TaskFlow, len(schemaTableColsM), filepath.Join(s.OutputDir, fmt.Sprintf("scan_%s.sql", stringutil.StringLower(s.TaskName)))))
		sqlComp.WriteString(" 2, before running structure migration, set the number data type mapping rules appropriately in advance based on the data scan results.\n")
		sqlComp.WriteString("*/\n")

		for st, groupColumn := range schemaTableColsM {
			keySli := stringutil.StringSplit(st, constant.StringSeparatorAite)
			schemaName := keySli[0]
			tableName := keySli[1]

			var columns []string
			groupColumns := stringutil.StringSplit(groupColumn, constant.StringSeparatorComma)
			for _, c := range groupColumns {
				columnCategorySli := stringutil.StringSplit(c, constant.StringSeparatorUnderline)
				columns = append(columns, columnCategorySli[0])
			}

			wt := table.NewWriter()
			wt.SetStyle(table.StyleLight)
			wt.SetTitle(fmt.Sprintf("TABLE_NAME:	%s.%s", schemaName, tableName))
			wt.AppendHeader(table.Row{"COLUMN_NAME", "BIGINT", "BIGINT_UNSIGNED", "DECIMAL_INT", "DECIMAL_POINT", "UNKNOWN"})

			var tableScans []*ScanResultMYSQLCompatible

			for _, mt := range migrateTasks {
				if strings.EqualFold(schemaName, mt.SchemaNameS) && strings.EqualFold(tableName, mt.TableNameS) {
					var scanResults []*ScanResultMYSQLCompatible
					err = stringutil.UnmarshalJSON([]byte(mt.ScanResult), &scanResults)
					if err != nil {
						return err
					}
					tableScans = append(tableScans, scanResults...)
				}
			}

			for _, c := range columns {
				var (
					bigint         int64
					bigintUnsigned int64
					decimalInt     int64
					decimalPoint   int64
					unknown        int64
				)

				for _, ts := range tableScans {
					if strings.EqualFold(c, ts.ColumnName) {
						bigintS, err := stringutil.StrconvIntBitSize(ts.Bigint, 64)
						if err != nil {
							return err
						}
						bigint += bigintS

						bigintUS, err := stringutil.StrconvIntBitSize(ts.BigintUnsigned, 64)
						if err != nil {
							return err
						}
						bigintUnsigned += bigintUS

						decimalI, err := stringutil.StrconvIntBitSize(ts.DecimalInt, 64)
						if err != nil {
							return err
						}
						decimalInt += decimalI

						decimalIP, err := stringutil.StrconvIntBitSize(ts.DecimalPoint, 64)
						if err != nil {
							return err
						}
						decimalPoint += decimalIP

						unknownS, err := stringutil.StrconvIntBitSize(ts.Unknown, 64)
						if err != nil {
							return err
						}
						unknown += unknownS
					}
				}

				wt.AppendRow(table.Row{c, bigint, bigintUnsigned, decimalInt, decimalPoint, unknown})
			}

			wt.SetCaption(fmt.Sprintf("%d rows in set (scan detail)", len(columns)))

			sqlComp.WriteString(wt.Render() + "\n\n")
		}

	}

	if strings.EqualFold(sqlComp.String(), "") {
		_, err = s.writeScanFile(sqlComp.String())
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *DataScanFile) writeScanFile(str string) (int, error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.CompWriter.WriteString(str)
}

func (s *DataScanFile) initOutputScanFile() error {
	outCompFile, err := os.OpenFile(filepath.Join(s.OutputDir, fmt.Sprintf("data_scan_%s.sql", stringutil.StringLower(s.TaskName))), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	s.CompWriter, s.CompFile = bufio.NewWriter(outCompFile), outCompFile
	return nil
}

func (s *DataScanFile) Close() error {
	if s.CompFile != nil {
		err := s.CompWriter.Flush()
		if err != nil {
			return err
		}
		err = s.CompFile.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
