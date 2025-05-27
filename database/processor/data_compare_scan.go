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
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/golang/snappy"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/utils/stringutil"
	"golang.org/x/sync/errgroup"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"
)

type DataCompareScan struct {
	Ctx               context.Context    `json:"-"`
	Mutex             *sync.Mutex        `json:"-"`
	CompFile          *os.File           `json:"-"`
	CompWriter        *bufio.Writer      `json:"-"`
	DBTypeS           string             `json:"dbTypeS"`
	DBTypeT           string             `json:"dbTypeT"`
	TaskName          string             `json:"taskName"`
	TaskFlow          string             `json:"taskFlow"`
	SchemaNameS       string             `json:"schemaNameS"`
	TableNameS        string             `json:"tableNameS"`
	DatabaseS         database.IDatabase `json:"-"`
	DatabaseT         database.IDatabase `json:"-"`
	ConnCharsetS      string             `json:"connCharsetS"`
	ConnCharsetT      string             `json:"connCharsetT"`
	DisableCompareMd5 bool               `json:"disableCompareMd5"`
	Stream            string             `json:"stream"`
	ChunkIds          []string           `json:"chunkIds"`
	ChunkThreads      int                `json:"chunkThreads"`
	OutputDir         string             `json:"outputDir"`
	CallTimeout       int                `json:"callTimeout"`
}

func NewDataCompareScan(ctx context.Context,
	taskName, taskFlow, dbTypeS, dbTypeT, schemaName,
	tableName, outputDir, connCharsetS, connCharsetT string, chunkIds []string,
	databaseS, databaseT database.IDatabase,
	chunkThreads int,
	disableCompareMd5 bool, callTimeout int, stream string) *DataCompareScan {
	return &DataCompareScan{
		Ctx:               ctx,
		TaskName:          taskName,
		DBTypeS:           dbTypeS,
		DBTypeT:           dbTypeT,
		TaskFlow:          taskFlow,
		SchemaNameS:       schemaName,
		TableNameS:        tableName,
		OutputDir:         outputDir,
		ChunkIds:          chunkIds,
		ConnCharsetS:      connCharsetS,
		ConnCharsetT:      connCharsetT,
		DatabaseS:         databaseS,
		DatabaseT:         databaseT,
		ChunkThreads:      chunkThreads,
		Mutex:             &sync.Mutex{},
		DisableCompareMd5: disableCompareMd5,
		Stream:            stream,
		CallTimeout:       callTimeout,
	}
}

func (s *DataCompareScan) SyncFile() error {
	var (
		err          error
		migrateTasks []*task.DataCompareTask
	)
	fmt.Printf("Log:          ...\n")
	fmt.Printf("query the data compare task schema table abnormal not_equal chunk data...\n")

	// get migrate task tables
	if !strings.EqualFold(s.SchemaNameS, "") && !strings.EqualFold(s.TableNameS, "") && len(s.ChunkIds) == 0 {
		migrateTasks, err = model.GetIDataCompareTaskRW().FindDataCompareTask(s.Ctx, &task.DataCompareTask{
			TaskName:    s.TaskName,
			SchemaNameS: s.SchemaNameS,
			TableNameS:  s.TableNameS,
		},
			[]string{constant.TaskDatabaseStatusNotEqual})
		if err != nil {
			return err
		}
	} else if !strings.EqualFold(s.SchemaNameS, "") && !strings.EqualFold(s.TableNameS, "") && len(s.ChunkIds) > 0 {
		migrateTasks, err = model.GetIDataCompareTaskRW().QueryDataCompareTaskChunk(s.Ctx, &task.DataCompareTask{
			TaskName:    s.TaskName,
			SchemaNameS: s.SchemaNameS,
			TableNameS:  s.TableNameS,
			TaskStatus:  constant.TaskDatabaseStatusNotEqual,
		}, s.ChunkIds)
		if err != nil {
			return err
		}
	} else {
		migrateTasks, err = model.GetIDataCompareTaskRW().QueryDataCompareTaskByTaskStatus(s.Ctx, &task.DataCompareTask{
			TaskName:   s.TaskName,
			TaskStatus: constant.TaskDatabaseStatusNotEqual,
		})
		if err != nil {
			return err
		}
	}

	if len(migrateTasks) == 0 {
		// fmt.Printf("the data compare task all of the table records are equal, current not exist not equal table records.\n")
		fmt.Printf("the data compare task schema table not found abnormal not_equal chunk data...\n")
		return errors.New(constant.TaskDatabaseStatusEqual)
	}

	schemaTableTaskM := make(map[string][]*task.DataCompareTask)

	schemaTableNameRule := make(map[string]string)
	for _, mt := range migrateTasks {
		// ignore row counts data compare
		if mt.CompareMethod == constant.DataCompareMethodDatabaseCheckRows {
			continue
		}
		schemaTableNameRule[stringutil.StringBuilder(mt.SchemaNameS, constant.StringSeparatorAite, mt.TableNameS)] = stringutil.StringBuilder(mt.SchemaNameT, constant.StringSeparatorAite, mt.TableNameT)

	}

	if len(schemaTableNameRule) == 0 {
		return fmt.Errorf("the verify scan only supports scanning of chunk records that are unequal due to crc32 or md5 data verification methods. it does not support scanning of chunks with unequal number of rows. currently, no chunk record data table that meets the conditions is found, so it is skipped.")
	}

	for st, _ := range schemaTableNameRule {
		var compareTasks []*task.DataCompareTask
		for _, mt := range migrateTasks {
			if st == stringutil.StringBuilder(mt.SchemaNameS, constant.StringSeparatorAite, mt.TableNameS) {
				compareTasks = append(compareTasks, mt)
			}
		}

		if len(compareTasks) > 0 {
			schemaTableTaskM[st] = compareTasks
		}
	}

	dbCharsetS, dbCharsetT, err := s.genDatabaseConvertCharset()
	if err != nil {
		return err
	}

	// binary search traversal locates the problem field
	// tableName[chunkID][problemFields]
	for st, ts := range schemaTableTaskM {
		schemaTabSli := strings.Split(st, constant.StringSeparatorAite)

		tt, ok := schemaTableNameRule[st]
		if !ok {
			return fmt.Errorf("the task_name [%s] schema_name table name mapping rule not found, please contact author or retry", s.TableNameS)
		}
		schemaTabSliT := strings.Split(tt, constant.StringSeparatorAite)

		err := s.initFile(schemaTabSli[1])
		if err != nil {
			return err
		}

		if _, err := s.writeFile(
			fmt.Sprintf(" the task_name [%s] problem field where the database schema_name_s [%s] table_name_s [%s] generates inconsistent data\n", s.TaskName, schemaTabSli[0], schemaTabSli[1])); err != nil {
			return err
		}

		startTime := time.Now()
		if strings.EqualFold(s.Stream, "UPSTREAM") {
			fmt.Printf("scan abnormal schema_name_s [%s] table_name_s [%s] started at [%s]\n",
				color.RedString(schemaTabSli[0]), color.RedString(schemaTabSli[1]), color.RedString(startTime.Format("2006-01-02 15:04:05")))
		} else {
			fmt.Printf("scan abnormal schema_name_t [%s] table_name_t [%s] started at [%s]\n",
				color.RedString(schemaTabSliT[0]), color.RedString(schemaTabSliT[1]), color.RedString(startTime.Format("2006-01-02 15:04:05")))
		}

		g, gCtx := errgroup.WithContext(s.Ctx)
		g.SetLimit(s.ChunkThreads)

		for _, ds := range ts {
			dmts := ds
			g.Go(func() error {
				chunkTime := time.Now()
				fmt.Printf("- scan abnormal chunk_id [%s]...\n", color.GreenString(dmts.ChunkID))

				var chunkStr strings.Builder

				chunkStr.WriteString(fmt.Sprintf(" the abnormal schema table chunk_id [%s] problem details:\n", dmts.ChunkID))

				wt := table.NewWriter()
				wt.SetStyle(table.StyleLight)
				wt.AppendHeader(table.Row{"TABLE_NAME_S", "TABLE_NAME_T", "ISSUE_COLUMN_S", "ISSUE_COLUMN_T", "ISSUE_CHUNK_S", "ISSUE_CHUNK_T", "REASON", "SUGGEST"})

				seeks, err := s.SeekDataCompare(gCtx, dmts)
				if err != nil {
					return err
				}

				var (
					chunkColumnValues [][]string
					abnormalDatas     []map[string]string
				)
				if strings.EqualFold(s.Stream, "UPSTREAM") {
					var (
						asciiColumns []string
						queryStr     strings.Builder
					)
					for _, c := range seeks.columnNameS {
						asciiColumns = append(asciiColumns, fmt.Sprintf(`ASCII(%s)`, c))
					}

					queryStr.WriteString(fmt.Sprintf(`SELECT %s`, strings.Join(seeks.columnNameS, constant.StringSeparatorComma)))
					if len(seeks.chunkColumnS) > 0 {
						// when seeks.chunkColumnS == 0, chunk detail where 1 = 1, not found chunk column row data
						queryStr.WriteString(fmt.Sprintf(`,%s`, strings.Join(seeks.chunkColumnS, constant.StringSeparatorComma)))
					}
					queryStr.WriteString(fmt.Sprintf(`,%s FROM %s.%s WHERE %s`, strings.Join(asciiColumns, constant.StringSeparatorComma), dmts.SchemaNameS,
						dmts.TableNameS,
						seeks.chunkDetailS))

					chunkColumnValues, abnormalDatas, err = s.DatabaseS.GetDatabaseTableSeekAbnormalData(queryStr.String(), seeks.queryCondArgsS, s.CallTimeout, dbCharsetS, dbCharsetT, seeks.chunkColumnS)
					if err != nil {
						return err
					}
				} else {
					var (
						asciiColumns []string
						queryStr     strings.Builder
					)
					for _, c := range seeks.columnNameT {
						asciiColumns = append(asciiColumns, fmt.Sprintf(`ASCII(%s)`, c))
					}

					queryStr.WriteString(fmt.Sprintf(`SELECT %s`, strings.Join(seeks.columnNameT, constant.StringSeparatorComma)))
					if len(seeks.chunkColumnT) > 0 {
						// when seeks.chunkColumnT == 0, chunk detail where 1 = 1, not found chunk
						queryStr.WriteString(fmt.Sprintf(`,%s`, strings.Join(seeks.chunkColumnT, constant.StringSeparatorComma)))
					}
					queryStr.WriteString(fmt.Sprintf(`,%s FROM %s.%s WHERE %s`, strings.Join(asciiColumns, constant.StringSeparatorComma), dmts.SchemaNameT,
						dmts.TableNameT,
						seeks.chunkDetailT))

					chunkColumnValues, abnormalDatas, err = s.DatabaseT.GetDatabaseTableSeekAbnormalData(queryStr.String(), seeks.queryCondArgsT, s.CallTimeout, dbCharsetT, dbCharsetS, seeks.chunkColumnT)
					if err != nil {
						return err
					}
				}

				if len(abnormalDatas) == 0 {
					wt.AppendRow(table.Row{
						fmt.Sprintf("%s.%s", dmts.SchemaNameS, dmts.TableNameS),
						fmt.Sprintf("%s.%s", dmts.SchemaNameT, dmts.TableNameT),
						strings.Join(seeks.columnNameS, constant.StringSeparatorComma),
						strings.Join(seeks.columnNameT, constant.StringSeparatorComma),
						"",
						"",
						"Not Found Garbled Or UncommonWords Data",
						"Please Manual Double Check",
					})
					chunkStr.WriteString(wt.Render())
					if _, err := s.writeFile(chunkStr.String()); err != nil {
						return err
					}
					fmt.Printf("   scan abnormal chunk_id [%s] finished, duration: [%s]\n", color.GreenString(dmts.ChunkID), time.Now().Sub(chunkTime))
					return nil
				}

				for i, v := range chunkColumnValues {
					var (
						abnormalStr    []string
						chunkS, chunkT string
						err            error
					)
					for k, reason := range abnormalDatas[i] {
						abnormalStr = append(abnormalStr, fmt.Sprintf("%s[%s]", k, reason))
					}
					// when seeks.chunkColumnS == 0, chunk detail where 1 = 1, not found chunk column row data
					if len(v) == 0 {
						chunkS = `1 = 1`
						chunkT = `1 = 1`
					} else {
						chunkS, chunkT, err = s.genDatabaseChunkRange(seeks.chunkColumnS, seeks.chunkColumnT, v)
						if err != nil {
							return err
						}
					}
					wt.AppendRow(table.Row{
						fmt.Sprintf("%s.%s", dmts.SchemaNameS, dmts.TableNameS),
						fmt.Sprintf("%s.%s", dmts.SchemaNameT, dmts.TableNameT),
						strings.Join(seeks.columnNameS, constant.StringSeparatorComma),
						strings.Join(seeks.columnNameT, constant.StringSeparatorComma),
						chunkS,
						chunkT,
						strings.Join(abnormalStr, constant.StringSeparatorComma),
						"Double Check Abnormal Column Data",
					})
				}

				chunkStr.WriteString(wt.Render())
				if _, err := s.writeFile(chunkStr.String() + "\n\n"); err != nil {
					return err
				}

				fmt.Printf("   scan abnormal chunk_id [%s] finished, duration: [%s]\n", color.GreenString(dmts.ChunkID), time.Now().Sub(chunkTime))
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return err
		}

		if err := s.closeFile(); err != nil {
			return err
		}

		if strings.EqualFold(s.Stream, "UPSTREAM") {
			fmt.Printf("scan abnormal schema_name_s [%s] table_name_s [%s] finished, duration: [%s]\n---\n", color.RedString(schemaTabSli[0]), color.RedString(schemaTabSli[1]), time.Now().Sub(startTime))
		} else {
			fmt.Printf("scan abnormal schema_name_t [%s] table_name_t [%s] finished, duration: [%s]\n---\n", color.RedString(schemaTabSliT[0]), color.RedString(schemaTabSliT[1]), time.Now().Sub(startTime))
		}
	}

	return nil
}

func (s *DataCompareScan) writeFile(str string) (int, error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.CompWriter.WriteString(str)
}

func (s *DataCompareScan) initFile(tableName string) error {
	outCompFile, err := os.OpenFile(filepath.Join(s.OutputDir, fmt.Sprintf("data_compare_seek_%s_%s.sql", stringutil.StringLower(s.TaskName), stringutil.StringLower(tableName))), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	s.CompWriter, s.CompFile = bufio.NewWriter(outCompFile), outCompFile
	return nil
}

func (s *DataCompareScan) closeFile() error {
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

type Seek struct {
	columnNameS    []string
	chunkDetailS   string
	chunkColumnS   []string
	queryCondArgsS []interface{}
	columnNameT    []string
	chunkDetailT   string
	chunkColumnT   []string
	queryCondArgsT []interface{}
}

func (s *DataCompareScan) SeekDataCompare(ctx context.Context, dmt *task.DataCompareTask) (*Seek, error) {
	var (
		chunkDetailS, chunkDetailT     string
		queryCondArgsS, queryCondArgsT []interface{}

		columnDetailS, columnDetailT string
	)

	dbCharsetS, dbCharsetT, err := s.genDatabaseConvertCharset()
	if err != nil {
		return nil, err
	}
	desChunkDetailS, err := stringutil.Decrypt(dmt.ChunkDetailS, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return nil, err
	}
	decChunkDetailS, err := snappy.Decode(nil, []byte(desChunkDetailS))
	if err != nil {
		return nil, err
	}
	desChunkDetailS = stringutil.BytesToString(decChunkDetailS)

	desChunkDetailT, err := stringutil.Decrypt(dmt.ChunkDetailT, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return nil, err
	}
	decChunkDetailT, err := snappy.Decode(nil, []byte(desChunkDetailT))
	if err != nil {
		return nil, err
	}
	desChunkDetailT = stringutil.BytesToString(decChunkDetailT)

	convertRaw, err := stringutil.CharsetConvert([]byte(desChunkDetailS), constant.CharsetUTF8MB4, dbCharsetS)
	if err != nil {
		return nil, fmt.Errorf("the task [%s] schema [%s] table [%s] column [%s] charset convert failed, %v", dmt.TaskName, dmt.SchemaNameS, dmt.TableNameS, dmt.ColumnDetailSS, err)
	}
	chunkDetailS = stringutil.BytesToString(convertRaw)

	convertRaw, err = stringutil.CharsetConvert([]byte(desChunkDetailT), constant.CharsetUTF8MB4, dbCharsetT)
	if err != nil {
		return nil, fmt.Errorf("the task [%s] schema [%s] table [%s] column [%s] charset convert failed, %v", dmt.TaskName, dmt.SchemaNameS, dmt.TableNameS, dmt.ColumnDetailSS, err)
	}
	chunkDetailT = stringutil.BytesToString(convertRaw)

	if strings.EqualFold(dmt.ChunkDetailArgS, "") {
		queryCondArgsS = nil
	} else {
		err = stringutil.UnmarshalJSON([]byte(dmt.ChunkDetailArgS), &queryCondArgsS)
		if err != nil {
			return nil, fmt.Errorf("the database source query args [%v] running failed: [%v]", dmt.ChunkDetailArgS, err)
		}
	}

	if strings.EqualFold(dmt.ChunkDetailArgT, "") {
		queryCondArgsT = nil
	} else {
		err = stringutil.UnmarshalJSON([]byte(dmt.ChunkDetailArgT), &queryCondArgsT)
		if err != nil {
			return nil, fmt.Errorf("the database target query args [%v] running failed: [%v]", dmt.ChunkDetailArgT, err)
		}
	}

	convertRaw, err = stringutil.CharsetConvert([]byte(dmt.ColumnDetailSS), constant.CharsetUTF8MB4, dbCharsetS)
	if err != nil {
		return nil, fmt.Errorf("the task [%s] schema [%s] table [%s] column [%s] charset convert failed: [%v]", dmt.TaskName, dmt.SchemaNameS, dmt.TableNameS, dmt.ColumnDetailSS, err)
	}
	columnDetailS = stringutil.BytesToString(convertRaw)

	convertRaw, err = stringutil.CharsetConvert([]byte(dmt.ColumnDetailTS), constant.CharsetUTF8MB4, dbCharsetT)
	if err != nil {
		return nil, fmt.Errorf("the task [%s] schema [%s] table [%s] column [%s] charset convert failed: [%v]", dmt.TaskName, dmt.SchemaNameS, dmt.TableNameS, dmt.ColumnDetailTS, err)
	}
	columnDetailT = stringutil.BytesToString(convertRaw)

	dbTypeSli := stringutil.StringSplit(s.TaskFlow, constant.StringSeparatorAite)

	var (
		columnDetailSilS, columnDetailSilT             []string
		columnStringSeparatorS, columnStringSeparatorT string
	)
	columnDetailSilS = stringutil.StringSplit(columnDetailS, constant.StringSeparatorComplexSymbol)
	columnDetailSilT = stringutil.StringSplit(columnDetailT, constant.StringSeparatorComplexSymbol)

	switch dbTypeSli[0] {
	case constant.DatabaseTypeOracle, constant.DatabaseTypePostgresql:
		columnStringSeparatorS = constant.StringSeparatorDoubleQuotes
	case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
		columnStringSeparatorS = constant.StringSeparatorBacktick
	default:
		return nil, fmt.Errorf("the task_name [%s] taskflow [%s] source database type [%s] isn't support, please contact author", s.TaskName, s.TaskFlow, dbTypeSli[0])
	}
	switch dbTypeSli[1] {
	case constant.DatabaseTypeOracle, constant.DatabaseTypePostgresql:
		columnStringSeparatorT = constant.StringSeparatorDoubleQuotes
	case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
		columnStringSeparatorT = constant.StringSeparatorBacktick
	default:
		return nil, fmt.Errorf("the task_name [%s] taskflow [%s] source database type [%s] isn't support, please contact author", s.TaskName, s.TaskFlow, dbTypeSli[0])
	}
	seek := NewDataCompareSeek(
		s.TaskName,
		s.TaskFlow,
		dbTypeSli[0],
		dbTypeSli[1],
		s.ConnCharsetS,
		s.ConnCharsetT,
		s.DatabaseS,
		s.DatabaseT,
		columnDetailSilS,
		columnDetailSilT,
		s.DisableCompareMd5)

	differences, err := seek.collectDifferences(
		ctx,
		chunkDetailS,
		chunkDetailT,
		queryCondArgsS,
		queryCondArgsT,
		dmt,
	)
	if err != nil {
		return nil, fmt.Errorf("the task [%s] schema [%s] table [%s] chunk [%s] seek collecting differences failed: [%v]", dmt.TaskName, dmt.SchemaNameS, dmt.TableNameS, dmt.ChunkID, err)
	}

	var (
		upstreams, downstreams     []string
		chunkColumnS, chunkColumnT []string
	)
	if len(differences) > 0 {
		for _, diff := range differences {
			upstreams = append(upstreams, diff[0])
			downstreams = append(downstreams, diff[1])
		}
	}

	// find chunk columns
	re := regexp.MustCompile(fmt.Sprintf(`%s([^%s]*)%s`, columnStringSeparatorS, columnStringSeparatorS, columnStringSeparatorS))
	matcheColumnS := re.FindAllStringSubmatch(chunkDetailS, -1)
	originColumnSliS := make(map[string]struct{})
	for _, match := range matcheColumnS {
		if len(match) > 1 {
			originColumnSliS[match[1]] = struct{}{}
		}
	}
	for c, _ := range originColumnSliS {
		chunkColumnS = append(chunkColumnS, c)
	}

	re = regexp.MustCompile(fmt.Sprintf(`%s([^%s]*)%s`, columnStringSeparatorT, columnStringSeparatorT, columnStringSeparatorT))
	matcheColumnT := re.FindAllStringSubmatch(chunkDetailT, -1)
	originColumnSliT := make(map[string]struct{})
	for _, match := range matcheColumnT {
		if len(match) > 1 {
			originColumnSliT[match[1]] = struct{}{}
		}
	}
	for c, _ := range originColumnSliT {
		chunkColumnT = append(chunkColumnT, c)
	}

	return &Seek{
		columnNameS:    upstreams,
		chunkDetailS:   chunkDetailS,
		chunkColumnS:   chunkColumnS,
		queryCondArgsS: queryCondArgsS,
		columnNameT:    downstreams,
		chunkDetailT:   chunkDetailT,
		chunkColumnT:   chunkColumnT,
		queryCondArgsT: queryCondArgsT,
	}, nil
}

func (s *DataCompareScan) genDatabaseConvertCharset() (string, string, error) {
	var dbCharsetS, dbCharsetT string
	switch s.TaskFlow {
	case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL:
		dbCharsetS = constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(s.ConnCharsetS)]
		dbCharsetT = constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(s.ConnCharsetT)]
	case constant.TaskFlowTiDBToOracle, constant.TaskFlowMySQLToOracle:
		dbCharsetS = constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(s.ConnCharsetS)]
		dbCharsetT = constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(s.ConnCharsetT)]
	case constant.TaskFlowPostgresToTiDB, constant.TaskFlowPostgresToMySQL:
		dbCharsetS = constant.MigratePostgreSQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(s.ConnCharsetS)]
		dbCharsetT = constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(s.ConnCharsetT)]
	case constant.TaskFlowTiDBToPostgres, constant.TaskFlowMySQLToPostgres:
		dbCharsetS = constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(s.ConnCharsetS)]
		dbCharsetT = constant.MigratePostgreSQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(s.ConnCharsetT)]
	default:
		return "", "", fmt.Errorf("the task [%s] taskflow [%s] column rule isn't support, please contact author", s.TaskName, s.TaskFlow)
	}
	return dbCharsetS, dbCharsetT, nil
}

func (s *DataCompareScan) genDatabaseChunkRange(chunkColumnS, chunkColumnT, chunkValues []string) (string, string, error) {
	if strings.EqualFold(s.Stream, "UPSTREAM") {
		return s.genDatabaseChunkRangeByDBType(s.DBTypeS, chunkColumnS, chunkColumnT, chunkValues)
	} else {
		return s.genDatabaseChunkRangeByDBType(s.DBTypeT, chunkColumnS, chunkColumnT, chunkValues)
	}
}

func (s *DataCompareScan) genDatabaseChunkRangeByDBType(baselineDBType string, chunkColumnS, chunkColumnT, chunkValues []string) (string, string, error) {
	switch strings.ToUpper(baselineDBType) {
	case constant.DatabaseTypeOracle:
		chunkRangeS, chunkRangeT := s.genDatabaseChunkRangeSeekOracleStream(chunkColumnS, chunkColumnT, chunkValues)
		return chunkRangeS, chunkRangeT, nil
	case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
		chunkRangeS, chunkRangeT := s.genDatabaseChunkRangeSeekMySQLStream(chunkColumnS, chunkColumnT, chunkValues)
		return chunkRangeS, chunkRangeT, nil
	case constant.DatabaseTypePostgresql:
		chunkRangeS, chunkRangeT := s.genDatabaseChunkRangeSeekPostgreStream(chunkColumnS, chunkColumnT, chunkValues)
		return chunkRangeS, chunkRangeT, nil
	default:
		return "", "", fmt.Errorf("the database type [%s] gen chunk range is not support,please contact author or retry", baselineDBType)
	}
}

func (s *DataCompareScan) genDatabaseChunkRangeSeekOracleStream(chunkColumnS, chunkColumnT, chunkValues []string) (string, string) {
	var chunkRangeS, chunkRangeT []string
	for i, v := range chunkValues {
		if strings.EqualFold(v, `NULL`) {
			chunkRangeS = append(chunkRangeS, fmt.Sprintf("%s IS NULL", chunkColumnS[i]))
			chunkRangeT = append(chunkRangeT, fmt.Sprintf("%s IS NULL", chunkColumnT[i]))
		} else if strings.EqualFold(v, "") {
			chunkRangeS = append(chunkRangeS, fmt.Sprintf("%s IS NULL", chunkColumnS[i]))
			chunkRangeT = append(chunkRangeT, fmt.Sprintf("%s IS NULL", chunkColumnT[i]))
		} else {
			chunkRangeS = append(chunkRangeS, fmt.Sprintf("%s = %s", chunkColumnS[i], v))
			chunkRangeT = append(chunkRangeT, fmt.Sprintf("%s = %s", chunkColumnT[i], v))
		}
	}
	return strings.Join(chunkRangeS, " AND "), strings.Join(chunkRangeT, " AND ")
}

func (s *DataCompareScan) genDatabaseChunkRangeSeekPostgreStream(chunkColumnS, chunkColumnT, chunkValues []string) (string, string) {
	var chunkRangeS, chunkRangeT []string
	for i, v := range chunkValues {
		if strings.EqualFold(v, `NULL`) {
			chunkRangeS = append(chunkRangeS, fmt.Sprintf("%s IS NULL", chunkColumnS[i]))
			chunkRangeT = append(chunkRangeT, fmt.Sprintf("%s IS NULL", chunkColumnT[i]))
		} else if strings.EqualFold(v, "") {
			chunkRangeS = append(chunkRangeS, fmt.Sprintf("%s = ''", chunkColumnS[i]))
			chunkRangeT = append(chunkRangeT, fmt.Sprintf("%s = ''", chunkColumnT[i]))
		} else {
			chunkRangeS = append(chunkRangeS, fmt.Sprintf("%s = %s", chunkColumnS[i], v))
			chunkRangeT = append(chunkRangeT, fmt.Sprintf("%s = %s", chunkColumnT[i], v))

		}
	}
	return strings.Join(chunkRangeS, " AND "), strings.Join(chunkRangeT, " AND ")
}

func (s *DataCompareScan) genDatabaseChunkRangeSeekMySQLStream(chunkColumnS, chunkColumnT, chunkValues []string) (string, string) {
	var chunkRangeS, chunkRangeT []string
	for i, v := range chunkValues {
		if strings.EqualFold(v, `NULL`) {
			chunkRangeS = append(chunkRangeS, fmt.Sprintf("%s IS NULL", chunkColumnS[i]))
			chunkRangeT = append(chunkRangeT, fmt.Sprintf("%s IS NULL", chunkColumnT[i]))
		} else if strings.EqualFold(v, "") {
			chunkRangeS = append(chunkRangeS, fmt.Sprintf("%s = ''", chunkColumnS[i]))
			chunkRangeT = append(chunkRangeT, fmt.Sprintf("%s = ''", chunkColumnT[i]))
		} else {
			chunkRangeS = append(chunkRangeS, fmt.Sprintf("%s = %s", chunkColumnS[i], v))
			chunkRangeT = append(chunkRangeT, fmt.Sprintf("%s = %s", chunkColumnT[i], v))

		}
	}
	return strings.Join(chunkRangeS, " AND "), strings.Join(chunkRangeT, " AND ")
}
