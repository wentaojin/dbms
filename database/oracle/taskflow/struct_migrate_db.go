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

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/database/mysql"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
)

type StructMigrateDatabase struct {
	Ctx           context.Context       `json:"-"`
	TaskName      string                `json:"taskName"`
	SubTaskName   string                `json:"subTaskName"`
	TaskFlow      string                `json:"taskFlow"`
	TaskStartTime time.Time             `json:"-"`
	DatasourceT   database.IDatabase    `json:"-"`
	TableStruct   *database.TableStruct `json:"tableStruct"`
}

func NewStructMigrateDatabase(ctx context.Context,
	taskName, subTaskName, taskFlow string, datasourceT database.IDatabase,
	taskStartTime time.Time, tableStruct *database.TableStruct) *StructMigrateDatabase {
	return &StructMigrateDatabase{
		Ctx:           ctx,
		TaskName:      taskName,
		SubTaskName:   subTaskName,
		TaskFlow:      taskFlow,
		TaskStartTime: taskStartTime,
		DatasourceT:   datasourceT,
		TableStruct:   tableStruct,
	}
}

// WriteStructDatabase used for sync file, current only write database, but not sync target database
func (s *StructMigrateDatabase) WriteStructDatabase() error {
	originSqlDigest, compDigest, incompDigest, err := s.GenTableStructDigest()
	if err != nil {
		return err
	}
	_, err = model.GetIStructMigrateTaskRW().UpdateStructMigrateTask(s.Ctx, &task.StructMigrateTask{
		SubTaskName: s.SubTaskName,
		SchemaNameS: s.TableStruct.SchemaNameS,
		TableNameS:  s.TableStruct.TableNameT,
	}, map[string]string{
		"TaskStatus":      constant.TaskStatusFinished,
		"SourceSqlDigest": originSqlDigest,
		"TargetSqlDigest": compDigest,
		"IncompSqlDigest": incompDigest,
		"Duration":        fmt.Sprintf("%f", time.Now().Sub(s.TaskStartTime).Seconds()),
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *StructMigrateDatabase) SyncStructDatabase() error {
	originSqlDigest, compDigest, incompDigest, err := s.GenTableStructDigest()
	if err != nil {
		return err
	}

	ddlSql, err := stringutil.Decrypt(compDigest, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}

	for _, ddl := range strings.Split(ddlSql, "\n") {
		_, err = s.DatasourceT.ExecContext(ddl)
		if err != nil {
			return err
		}
	}
	_, err = model.GetIStructMigrateTaskRW().UpdateStructMigrateTask(s.Ctx, &task.StructMigrateTask{
		SubTaskName: s.SubTaskName,
		SchemaNameS: s.TableStruct.SchemaNameS,
		TableNameS:  s.TableStruct.TableNameT,
	}, map[string]string{
		"TaskStatus":      constant.TaskStatusFinished,
		"SourceSqlDigest": originSqlDigest,
		"TargetSqlDigest": compDigest,
		"IncompSqlDigest": incompDigest,
		"Duration":        fmt.Sprintf("%f", time.Now().Sub(s.TaskStartTime).Seconds()),
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *StructMigrateDatabase) GenTableStructDigest() (string, string, string, error) {
	originSqlDigest, err := stringutil.Encrypt(s.TableStruct.OriginDdlS, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return "", "", "", err
	}
	compSql, incompSql, err := s.GenTableStructDDL()
	if err != nil {
		return "", "", "", err
	}
	compDigest, err := stringutil.Encrypt(strings.Join(compSql, "\n"), []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return "", "", "", err
	}
	incompDigest, err := stringutil.Encrypt(strings.Join(incompSql, "\n"), []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return "", "", "", err
	}
	return originSqlDigest, compDigest, incompDigest, nil
}

func (s *StructMigrateDatabase) GenTableStructDDL() ([]string, []string, error) {
	var (
		bf              strings.Builder
		incompatibleSql []string
		compatibleSql   []string
	)

	bf.WriteString(fmt.Sprintf("CREATE TABLE %s.%s (\n", s.TableStruct.SchemaNameT, s.TableStruct.TableNameT))
	bf.WriteString(strings.Join(s.TableStruct.TableColumns, ",\n"))

	switch {
	case strings.EqualFold(s.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(s.TaskFlow, constant.TaskFlowOracleToMySQL):
		var tableKeys []string
		if !strings.EqualFold(s.TableStruct.TablePrimaryKey, "") {
			tableKeys = append(tableKeys, s.TableStruct.TablePrimaryKey)
		}
		if len(s.TableStruct.TableUniqueKeys) > 0 {
			tableKeys = append(tableKeys, s.TableStruct.TableUniqueKeys...)
		}

		if len(s.TableStruct.TableUniqueIndexes) > 0 {
			tableKeys = append(tableKeys, s.TableStruct.TableUniqueIndexes...)
		}

		if len(s.TableStruct.TableNormalIndexes) > 0 {
			tableKeys = append(tableKeys, s.TableStruct.TableNormalIndexes...)
		}

		if len(tableKeys) > 0 {
			bf.WriteString(strings.Join(tableKeys, ",\n"))
		}

		if strings.EqualFold(s.TableStruct.TableComment, "") {
			bf.WriteString(fmt.Sprintf(") %s;", s.TableStruct.TableSuffix))
		} else {
			bf.WriteString(fmt.Sprintf(") %s %s;", s.TableStruct.TableSuffix, s.TableStruct.TableComment))
		}

		// foreign and check key sql ddl
		var (
			foreignKeySql []string
			checkKeySql   []string
		)

		compatibleSql = append(compatibleSql, bf.String())

		zap.L().Info("migrate oracle table struct",
			zap.String("schema", s.TableStruct.SchemaNameT),
			zap.String("table", s.TableStruct.TableNameT),
			zap.String("struct sql", bf.String()))

		// not support ddl sql
		if len(s.TableStruct.TableIncompatibleDDL) > 0 {
			incompatibleSql = append(incompatibleSql, s.TableStruct.TableIncompatibleDDL...)
		}

		if len(s.TableStruct.TableForeignKeys) > 0 {
			for _, fk := range s.TableStruct.TableForeignKeys {
				fkSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;",
					s.TableStruct.SchemaNameT, s.TableStruct.TableNameT, fk)
				zap.L().Info("migrate oracle table foreign key",
					zap.String("schema", s.TableStruct.SchemaNameT),
					zap.String("table", s.TableStruct.TableNameT),
					zap.String("fk sql", fkSQL))
				foreignKeySql = append(foreignKeySql, fkSQL)
			}
		}
		if len(s.TableStruct.TableCheckKeys) > 0 {
			for _, ck := range s.TableStruct.TableCheckKeys {
				ckSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;",
					s.TableStruct.SchemaNameT, s.TableStruct.SchemaNameT, ck)
				zap.L().Info("migrate oracle table check key",
					zap.String("schema", s.TableStruct.SchemaNameT),
					zap.String("table", s.TableStruct.TableNameT),
					zap.String("ck sql", ckSQL))
				checkKeySql = append(checkKeySql, ckSQL)
			}
		}

		// database tidb isn't support check and foreign constraint, ignore
		if strings.EqualFold(s.TaskFlow, constant.TaskFlowOracleToTiDB) {
			if len(foreignKeySql) > 0 {
				incompatibleSql = append(incompatibleSql, foreignKeySql...)
			}
			if len(checkKeySql) > 0 {
				incompatibleSql = append(incompatibleSql, checkKeySql...)
			}
			return compatibleSql, incompatibleSql, nil
		}

		// get target database version
		version, err := s.DatasourceT.(*mysql.Database).GetDatabaseVersion(constant.DatabaseTypeMySQL)
		if err != nil {
			return compatibleSql, incompatibleSql, err
		}
		if len(foreignKeySql) > 0 {
			compatibleSql = append(compatibleSql, foreignKeySql...)
		}
		if stringutil.VersionOrdinal(version) > stringutil.VersionOrdinal(constant.MYSQLDatabaseCheckConstraintSupportVersion) {
			if len(checkKeySql) > 0 {
				compatibleSql = append(compatibleSql, checkKeySql...)
			}
		} else {
			// not support
			if len(checkKeySql) > 0 {
				incompatibleSql = append(incompatibleSql, checkKeySql...)
			}
		}
		return compatibleSql, incompatibleSql, nil
	default:
		return compatibleSql, incompatibleSql, fmt.Errorf("oracle current task [%s] subtask [%s] taskflow [%s] isn't support, please contact author or reselect", s.TaskName, s.SubTaskName, s.TaskFlow)
	}
}
