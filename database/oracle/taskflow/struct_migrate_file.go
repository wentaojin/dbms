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
	"path/filepath"
	"strings"
	"sync"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type StructMigrateFile struct {
	Ctx              context.Context `json:"-"`
	Mutex            *sync.Mutex     `json:"-"`
	CompFile         *os.File        `json:"-"`
	InCompFile       *os.File        `json:"-"`
	CompWriter       *bufio.Writer   `json:"-"`
	InCompWriter     *bufio.Writer   `json:"-"`
	TaskName         string          `json:"taskName"`
	TaskFlow         string          `json:"taskFlow"`
	DBCharsetS       string          `json:"dbCharsetS"`
	DBCollationS     bool            `json:"dbCollationS"`
	SchemaCollationS string          `json:"schemaCollationS"`
	DBNlsComp        string          `json:"DBNlsComp"`
	SchemaNameS      string          `json:"schemaNameS"`
	SchemaNameT      string          `json:"schemaNameT"`
	OutputDir        string          `json:"outputDir"`
}

func NewStructMigrateFile(ctx context.Context,
	taskName, taskFlow, schemaNameS string, outputDir string) *StructMigrateFile {
	return &StructMigrateFile{
		Ctx:         ctx,
		TaskName:    taskName,
		TaskFlow:    taskFlow,
		SchemaNameS: schemaNameS,
		OutputDir:   outputDir,
		Mutex:       &sync.Mutex{},
	}
}

func (s *StructMigrateFile) InitOutputFile() error {
	err := s.initOutputCompatibleFile()
	if err != nil {
		return err
	}
	err = s.initOutputInCompatibleFile()
	if err != nil {
		return err
	}
	return nil
}

func (s *StructMigrateFile) SyncStructFile() error {
	taskFlowDBs := strings.Split(s.TaskFlow, constant.StringSeparatorAite)
	sourceDBType := taskFlowDBs[0]
	targetDBType := taskFlowDBs[1]

	var (
		sqlComp   strings.Builder
		sqlInComp strings.Builder
	)
	// schema
	migrateSchema, err := model.GetIStructMigrateTaskRW().GetStructMigrateTask(s.Ctx, &task.StructMigrateTask{
		TaskName:    s.TaskName,
		SchemaNameS: s.SchemaNameS,
		TaskStatus:  constant.TaskDatabaseStatusSuccess,
		Category:    constant.DatabaseStructMigrateSqlSchemaCategory,
	})
	if err != nil {
		return err
	}
	sqlComp.WriteString("/*\n")
	sqlComp.WriteString(" database schema migrate create sql\n")
	wt := table.NewWriter()
	wt.SetStyle(table.StyleLight)
	wt.AppendHeader(table.Row{"#", sourceDBType, targetDBType, "SUGGEST"})
	wt.AppendRows([]table.Row{
		{"Schema", migrateSchema[0].SchemaNameS, migrateSchema[0].SchemaNameT, "Create Schema"},
	})
	sqlComp.WriteString(wt.Render() + "\n")
	sqlComp.WriteString("*/\n")

	schemaDigest, err := stringutil.Decrypt(migrateSchema[0].TargetSqlDigest, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	sqlComp.WriteString(schemaDigest + "\n")

	// tables
	migrateTables, err := model.GetIStructMigrateTaskRW().GetStructMigrateTask(s.Ctx, &task.StructMigrateTask{
		TaskName:    s.TaskName,
		SchemaNameS: s.SchemaNameS,
		TaskStatus:  constant.TaskDatabaseStatusSuccess,
		Category:    constant.DatabaseStructMigrateSqlTableCategory,
	})
	if err != nil {
		return err
	}

	// incompatible table
	switch {
	case strings.EqualFold(s.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(s.TaskFlow, constant.TaskFlowOracleToMySQL):
		var (
			tableRows, partitionTables, sessionTemporaryTables, transactionTemporaryTables, clusteredTables, materializedViews []table.Row
		)
		for _, t := range migrateTables {
			if strings.EqualFold(t.TableTypeS, constant.OracleDatabaseTableTypeHeapTable) {
				continue
			}
			if strings.EqualFold(t.TableTypeS, constant.OracleDatabaseTableTypePartitionTable) {
				partitionTables = append(partitionTables, table.Row{"TABLE", t.TableTypeS, fmt.Sprintf("%s.%s", t.SchemaNameS, t.TableNameS), fmt.Sprintf("%s.%s", t.SchemaNameT, t.TableNameT), "If Need, Please Manual Process Table"})
			}
			if strings.EqualFold(t.TableTypeS, constant.OracleDatabaseTableTypeSessionTemporaryTable) {
				sessionTemporaryTables = append(sessionTemporaryTables, table.Row{"TABLE", t.TableTypeS, fmt.Sprintf("%s.%s", t.SchemaNameS, t.TableNameS), fmt.Sprintf("%s.%s", t.SchemaNameT, t.TableNameT), "If Need, Please Manual Process Table"})
			}
			if strings.EqualFold(t.TableTypeS, constant.OracleDatabaseTableTypeTransactionTemporaryTable) {
				transactionTemporaryTables = append(transactionTemporaryTables, table.Row{"TABLE", t.TableTypeS, fmt.Sprintf("%s.%s", t.SchemaNameS, t.TableNameS), fmt.Sprintf("%s.%s", t.SchemaNameT, t.TableNameT), "If Need, Please Manual Process Table"})
			}
			if strings.EqualFold(t.TableTypeS, constant.OracleDatabaseTableTypeClusteredTable) {
				clusteredTables = append(clusteredTables, table.Row{"TABLE", t.TableTypeS, fmt.Sprintf("%s.%s", t.SchemaNameS, t.TableNameS), fmt.Sprintf("%s.%s", t.SchemaNameT, t.TableNameT), "If Need, Please Manual Process Table"})
			}
			if strings.EqualFold(t.TableTypeS, constant.OracleDatabaseTableTypeMaterializedView) {
				materializedViews = append(materializedViews, table.Row{"TABLE", t.TableTypeS, fmt.Sprintf("%s.%s", t.SchemaNameS, t.TableNameS), fmt.Sprintf("%s.%s", t.SchemaNameT, t.TableNameT), "If Need, Please Manual Process Table"})
			}
		}

		tableRows = append(tableRows, partitionTables...)
		tableRows = append(tableRows, sessionTemporaryTables...)
		tableRows = append(tableRows, transactionTemporaryTables...)
		tableRows = append(tableRows, clusteredTables...)
		tableRows = append(tableRows, materializedViews...)

		sqlInComp.WriteString("/*\n")
		sqlInComp.WriteString(" database table maybe has incompatibility, will convert to normal table, if need, please manual process\n")
		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"#", "TABLE TYPE", sourceDBType, targetDBType, "SUGGEST"})
		tw.AppendRows(tableRows)
		sqlInComp.WriteString(tw.Render() + "\n")
		sqlInComp.WriteString("*/\n\n")
	default:
		return fmt.Errorf("current taskflow [%s] isn't support, please contact author or reselect", s.TaskFlow)
	}

	for _, t := range migrateTables {
		sqlComp.WriteString("/*\n")
		sqlComp.WriteString(" database table struct migrate sql \n")

		w := table.NewWriter()
		w.SetStyle(table.StyleLight)
		w.AppendHeader(table.Row{"#", "TABLE TYPE", sourceDBType, targetDBType, "SUGGEST"})
		w.AppendRows([]table.Row{
			{"TABLE", t.TableTypeS, fmt.Sprintf("%s.%s", t.SchemaNameS, t.TableNameS), fmt.Sprintf("%s.%s", t.SchemaNameT, t.TableNameT), "Create Table"},
		})

		sqlComp.WriteString(fmt.Sprintf("%v\n", w.Render()))

		originSql, err := stringutil.Decrypt(t.SourceSqlDigest, []byte(constant.DefaultDataEncryptDecryptKey))
		if err != nil {
			return err
		}
		sqlComp.WriteString(fmt.Sprintf("Origin DDL:%v\n", originSql))
		sqlComp.WriteString("*/\n")

		targetSqls, err := stringutil.Decrypt(t.TargetSqlDigest, []byte(constant.DefaultDataEncryptDecryptKey))
		if err != nil {
			return err
		}
		sqlComp.WriteString(targetSqls + "\n")

		// incompatible
		incompSqls, err := stringutil.Decrypt(t.IncompSqlDigest, []byte(constant.DefaultDataEncryptDecryptKey))
		if err != nil {
			return err
		}
		if !strings.EqualFold(incompSqls, "") {
			sqlInComp.WriteString("/*\n")
			sqlInComp.WriteString(" database table index or constraint maybe has incompatibility, skip, If need, please manual process\n")
			w = table.NewWriter()
			w.SetStyle(table.StyleLight)
			w.AppendHeader(table.Row{"#", "TABLE TYPE", sourceDBType, targetDBType, "SUGGEST"})
			w.AppendRows([]table.Row{
				{"TABLE", t.TableTypeS, fmt.Sprintf("%s.%s", t.SchemaNameS, t.TableNameS), fmt.Sprintf("%s.%s", t.SchemaNameT, t.TableNameT), "Create Index Or Constraints"}})
			sqlInComp.WriteString(fmt.Sprintf("%v\n", w.Render()))
			sqlInComp.WriteString("*/\n")

			sqlInComp.WriteString(incompSqls + "\n")
		}
	}

	if !strings.EqualFold(sqlComp.String(), "") {
		_, err = s.writeStructCompatibleFile(sqlComp.String())
		if err != nil {
			return err
		}
	}
	if !strings.EqualFold(sqlInComp.String(), "") {
		_, err = s.writeStructIncompatibleFile(sqlInComp.String())
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *StructMigrateFile) SyncSequenceFile() error {
	// sequence
	migrateTables, err := model.GetIStructMigrateTaskRW().GetStructMigrateTask(s.Ctx, &task.StructMigrateTask{
		TaskName:    s.TaskName,
		SchemaNameS: s.SchemaNameS,
		TaskStatus:  constant.TaskDatabaseStatusSuccess,
		Category:    constant.DatabaseStructMigrateSqlSequenceCategory,
	})
	if err != nil {
		return err
	}

	var (
		seqCreates       []string
		seqIncompCreates []string
	)
	for _, t := range migrateTables {
		if !strings.EqualFold(t.TargetSqlDigest, "") {
			targetSqls, err := stringutil.Decrypt(t.TargetSqlDigest, []byte(constant.DefaultDataEncryptDecryptKey))
			if err != nil {
				return err
			}
			seqCreates = append(seqCreates, targetSqls)
		}

		// incompatible
		if !strings.EqualFold(t.IncompSqlDigest, "") {
			incompSqls, err := stringutil.Decrypt(t.IncompSqlDigest, []byte(constant.DefaultDataEncryptDecryptKey))
			if err != nil {
				return err
			}
			seqIncompCreates = append(seqIncompCreates, incompSqls)
		}

	}

	if len(seqCreates) > 0 {
		var sqlComp strings.Builder

		sqlComp.WriteString("/*\n")
		sqlComp.WriteString(" database table sequence migrate sql \n")

		w := table.NewWriter()
		w.SetStyle(table.StyleLight)
		w.AppendHeader(table.Row{"#", "TABLE TYPE", "SCHEMA_NAME_S", "SCHEMA_NAME_T", "SUGGEST"})
		w.AppendRows([]table.Row{
			{"TABLE", "SEQUENCE", s.SchemaNameS, s.SchemaNameT, "Create Sequence"},
		})

		sqlComp.WriteString(fmt.Sprintf("%v\n", w.Render()))

		sqlComp.WriteString(stringutil.StringJoin(seqCreates, "\n"))
		_, err = s.writeStructCompatibleFile(sqlComp.String())
		if err != nil {
			return err
		}
	}

	if len(seqIncompCreates) > 0 {
		var sqlIncomp strings.Builder

		sqlIncomp.WriteString("/*\n")
		sqlIncomp.WriteString(" database table sequence migrate sql incompatible \n")

		w := table.NewWriter()
		w.SetStyle(table.StyleLight)
		w.AppendHeader(table.Row{"#", "TABLE TYPE", "SCHEMA_NAME_S", "SCHEMA_NAME_T", "SUGGEST"})
		w.AppendRows([]table.Row{
			{"TABLE", "SEQUENCE", s.SchemaNameS, s.SchemaNameT, "Ignore Sequence"},
		})

		sqlIncomp.WriteString(fmt.Sprintf("%v\n", w.Render()))

		sqlIncomp.WriteString(stringutil.StringJoin(seqIncompCreates, "\n"))
		_, err = s.writeStructIncompatibleFile(sqlIncomp.String())
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *StructMigrateFile) writeStructCompatibleFile(str string) (int, error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.CompWriter.WriteString(str)
}

func (s *StructMigrateFile) writeStructIncompatibleFile(str string) (int, error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.InCompWriter.WriteString(str)
}

func (s *StructMigrateFile) initOutputCompatibleFile() error {
	outCompFile, err := os.OpenFile(filepath.Join(s.OutputDir, fmt.Sprintf("struct_migrate_compatible_%s.sql", s.SchemaNameS)), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	s.CompWriter, s.CompFile = bufio.NewWriter(outCompFile), outCompFile
	return nil
}

func (s *StructMigrateFile) initOutputInCompatibleFile() error {
	outInCompFile, err := os.OpenFile(filepath.Join(s.OutputDir, fmt.Sprintf("struct_migrate_incompatible_%s.sql", s.SchemaNameS)), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	s.InCompWriter, s.InCompFile = bufio.NewWriter(outInCompFile), outInCompFile
	return nil
}

func (s *StructMigrateFile) Close() error {
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
	if s.InCompFile != nil {
		err := s.InCompWriter.Flush()
		if err != nil {
			return err
		}
		err = s.InCompFile.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
