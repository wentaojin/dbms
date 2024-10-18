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

	"github.com/wentaojin/dbms/logger"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
)

type StructMigrateDatabase struct {
	Ctx           context.Context       `json:"-"`
	TaskName      string                `json:"taskName"`
	TaskFlow      string                `json:"taskFlow"`
	TaskStartTime time.Time             `json:"-"`
	DatasourceT   database.IDatabase    `json:"-"`
	TableStruct   *database.TableStruct `json:"tableStruct"`
}

func NewStructMigrateDatabase(ctx context.Context,
	taskName, taskFlow string, datasourceT database.IDatabase,
	taskStartTime time.Time, tableStruct *database.TableStruct) *StructMigrateDatabase {
	return &StructMigrateDatabase{
		Ctx:           ctx,
		TaskName:      taskName,
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
	err = model.Transaction(s.Ctx, func(txnCtx context.Context) error {
		duration := fmt.Sprintf("%f", time.Now().Sub(s.TaskStartTime).Seconds())
		_, err = model.GetIStructMigrateTaskRW().UpdateStructMigrateTask(txnCtx, &task.StructMigrateTask{
			TaskName:    s.TaskName,
			SchemaNameS: s.TableStruct.SchemaNameS,
			TableNameS:  s.TableStruct.TableNameS,
		}, map[string]interface{}{
			"TaskStatus":      constant.TaskDatabaseStatusSuccess,
			"SourceSqlDigest": originSqlDigest,
			"TargetSqlDigest": compDigest,
			"IncompSqlDigest": incompDigest,
			"Duration":        duration,
			"ErrorDetail":     "",
		})
		if err != nil {
			return err
		}
		_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
			TaskName:    s.TaskName,
			SchemaNameS: s.TableStruct.SchemaNameS,
			TableNameS:  s.TableStruct.TableNameS,
			LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] schema_name_s [%s] table_name_s [%s] migrate write database success, cost [%v]",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeStructMigrate),
				s.TaskName,
				s.TaskFlow,
				s.TableStruct.SchemaNameS,
				s.TableStruct.TableNameS,
				duration,
			),
		})
		if err != nil {
			return err
		}
		return nil
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

	err = model.Transaction(s.Ctx, func(txnCtx context.Context) error {
		for _, d := range strings.Split(ddlSql, "\t\n") {
			ddl := strings.ReplaceAll(d, "\n", "")
			_, err = s.DatasourceT.ExecContext(txnCtx, ddl)
			if err != nil {
				return fmt.Errorf("the datasource sync ddl sql [%v] failed: [%v]", ddl, err)
			}
		}
		duration := fmt.Sprintf("%f", time.Now().Sub(s.TaskStartTime).Seconds())
		_, err = model.GetIStructMigrateTaskRW().UpdateStructMigrateTask(txnCtx, &task.StructMigrateTask{
			TaskName:    s.TaskName,
			SchemaNameS: s.TableStruct.SchemaNameS,
			TableNameS:  s.TableStruct.TableNameS,
		}, map[string]interface{}{
			"TaskStatus":      constant.TaskDatabaseStatusSuccess,
			"SourceSqlDigest": originSqlDigest,
			"TargetSqlDigest": compDigest,
			"IncompSqlDigest": incompDigest,
			"Duration":        duration,
			"ErrorDetail":     "",
		})
		if err != nil {
			return err
		}
		_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
			TaskName:    s.TaskName,
			SchemaNameS: s.TableStruct.SchemaNameS,
			TableNameS:  s.TableStruct.TableNameS,
			LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] schema_name_s [%s] table_name_s [%s] migrate write database success, cost [%v]",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeStructMigrate),
				s.TaskName,
				s.TaskFlow,
				s.TableStruct.SchemaNameS,
				s.TableStruct.TableNameS,
				duration,
			),
		})
		if err != nil {
			return err
		}
		return nil
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

	switch s.TaskFlow {
	case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL, constant.TaskFlowPostgresToTiDB, constant.TaskFlowPostgresToMySQL:
		bf.WriteString(fmt.Sprintf("%s `%s`.`%s` (\n", s.TableStruct.TableCreatePrefixT, s.TableStruct.SchemaNameT, s.TableStruct.TableNameT))
		bf.WriteString(strings.Join(s.TableStruct.TableColumns, ",\n"))

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
			bf.WriteString(",\n" + strings.Join(tableKeys, ",\n"))
		}

		if strings.EqualFold(s.TableStruct.TableComment, "") {
			bf.WriteString(fmt.Sprintf("\n) %s;", s.TableStruct.TableSuffix))
		} else {
			bf.WriteString(fmt.Sprintf("\n) %s %s;", s.TableStruct.TableSuffix, s.TableStruct.TableComment))
		}

		// foreign and check key sql ddl
		var (
			foreignKeySql []string
			checkKeySql   []string
		)

		compatibleSql = append(compatibleSql, bf.String())

		logger.Info("struct migrate task processor",
			zap.String("task_name", s.TaskName),
			zap.String("task_flow", s.TaskFlow),
			zap.String("schema_name_s", s.TableStruct.SchemaNameS),
			zap.String("table_name_s", s.TableStruct.TableNameS),
			zap.String("schema_name_t", s.TableStruct.SchemaNameT),
			zap.String("table_name_t", s.TableStruct.TableNameT),
			zap.String("task_stage", "struct ddl gen"),
			zap.String("table_ddl_sql", strings.ReplaceAll(bf.String(), "\n", "")))

		// not support ddl sql
		if len(s.TableStruct.TableIncompatibleDDL) > 0 {
			incompatibleSql = append(incompatibleSql, s.TableStruct.TableIncompatibleDDL...)
		}

		if len(s.TableStruct.TableForeignKeys) > 0 {
			for _, fk := range s.TableStruct.TableForeignKeys {
				fkSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;",
					s.TableStruct.SchemaNameT, s.TableStruct.TableNameT, fk)

				logger.Warn("struct migrate task processor",
					zap.String("task_name", s.TaskName),
					zap.String("task_flow", s.TaskFlow),
					zap.String("schema_name_s", s.TableStruct.SchemaNameS),
					zap.String("table_name_s", s.TableStruct.TableNameS),
					zap.String("schema_name_t", s.TableStruct.SchemaNameT),
					zap.String("table_name_t", s.TableStruct.TableNameT),
					zap.String("task_stage", "struct foregin key gen"),
					zap.String("foreign_key_sql", fkSQL))

				foreignKeySql = append(foreignKeySql, fkSQL)
			}
		}
		if len(s.TableStruct.TableCheckKeys) > 0 {
			for _, ck := range s.TableStruct.TableCheckKeys {
				ckSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s;",
					s.TableStruct.SchemaNameT, s.TableStruct.SchemaNameT, ck)

				logger.Warn("struct migrate task processor",
					zap.String("task_name", s.TaskName),
					zap.String("task_flow", s.TaskFlow),
					zap.String("schema_name_s", s.TableStruct.SchemaNameS),
					zap.String("table_name_s", s.TableStruct.TableNameS),
					zap.String("schema_name_t", s.TableStruct.SchemaNameT),
					zap.String("table_name_t", s.TableStruct.TableNameT),
					zap.String("task_stage", "struct check key gen"),
					zap.String("check_key_sql", ckSQL))

				checkKeySql = append(checkKeySql, ckSQL)
			}
		}

		// database tidb isn't support check and foreign constraint, ignore
		if strings.EqualFold(s.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(s.TaskFlow, constant.TaskFlowPostgresToTiDB) {
			if len(foreignKeySql) > 0 {
				incompatibleSql = append(incompatibleSql, foreignKeySql...)
			}
			if len(checkKeySql) > 0 {
				incompatibleSql = append(incompatibleSql, checkKeySql...)
			}
			return compatibleSql, incompatibleSql, nil
		}

		// get target database version
		version, err := s.DatasourceT.GetDatabaseVersion()
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
		return compatibleSql, incompatibleSql, fmt.Errorf("oracle current task [%s] taskflow [%s] isn't support, please contact author or reselect", s.TaskName, s.TaskFlow)
	}
}

type SequenceMigrateDatabase struct {
	Ctx            context.Context           `json:"-"`
	TaskName       string                    `json:"taskName"`
	TaskMode       string                    `json:"taskMode"`
	TaskFlow       string                    `json:"taskFlow"`
	TaskStartTime  time.Time                 `json:"-"`
	DatasourceT    database.IDatabase        `json:"-"`
	SeqMigrateTask *task.SequenceMigrateTask `json:"seqMigrateTask"`
}

func NewSequenceMigrateDatabase(ctx context.Context,
	taskName, taskMode, taskFlow string, datasourceT database.IDatabase,
	taskStartTime time.Time, seqs *task.SequenceMigrateTask) *SequenceMigrateDatabase {
	return &SequenceMigrateDatabase{
		Ctx:            ctx,
		TaskName:       taskName,
		TaskMode:       taskMode,
		TaskFlow:       taskFlow,
		TaskStartTime:  taskStartTime,
		DatasourceT:    datasourceT,
		SeqMigrateTask: seqs,
	}
}

func (s *SequenceMigrateDatabase) WriteSequenceDatabase() error {
	seqDigestS, err := stringutil.Encrypt(s.SeqMigrateTask.SourceSqlDigest, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	seqDigestT, err := stringutil.Encrypt(s.SeqMigrateTask.TargetSqlDigest, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	switch s.TaskFlow {
	case constant.TaskFlowOracleToTiDB, constant.TaskFlowPostgresToTiDB:
		version, err := s.DatasourceT.GetDatabaseVersion()
		if err != nil {
			return err
		}
		if stringutil.VersionOrdinal(version) < stringutil.VersionOrdinal(constant.TIDBDatabaseSequenceSupportVersion) {
			err = model.Transaction(s.Ctx, func(txnCtx context.Context) error {
				duration := fmt.Sprintf("%f", time.Now().Sub(s.TaskStartTime).Seconds())
				_, err = model.GetISequenceMigrateTaskRW().UpdateSequenceMigrateTask(txnCtx, &task.SequenceMigrateTask{
					TaskName:      s.TaskName,
					SchemaNameS:   s.SeqMigrateTask.SchemaNameS,
					SequenceNameS: s.SeqMigrateTask.SequenceNameS,
				}, map[string]interface{}{
					"TaskStatus":      constant.TaskDatabaseStatusSuccess,
					"SourceSqlDigest": seqDigestS,
					"TargetSqlDigest": seqDigestT,
					"IsCompatible":    constant.DatabaseMigrateSequenceNotCompatible,
					"Duration":        time.Now().Sub(s.TaskStartTime).Seconds(),
					"ErrorDetail":     "",
				})
				if err != nil {
					return err
				}
				_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
					TaskName:    s.TaskName,
					SchemaNameS: s.SeqMigrateTask.SchemaNameS,
					LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] schema_name_s [%s] sequence_name_s [%s] migrate write database success, cost [%v]",
						stringutil.CurrentTimeFormatString(),
						stringutil.StringLower(constant.TaskModeSequenceMigrate),
						s.TaskName,
						s.TaskFlow,
						s.SeqMigrateTask.SchemaNameS,
						s.SeqMigrateTask.SequenceNameS,
						duration,
					),
				})
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return err
			}

			return nil
		}
		err = model.Transaction(s.Ctx, func(txnCtx context.Context) error {
			duration := fmt.Sprintf("%f", time.Now().Sub(s.TaskStartTime).Seconds())
			_, err = model.GetISequenceMigrateTaskRW().UpdateSequenceMigrateTask(txnCtx, &task.SequenceMigrateTask{
				TaskName:      s.TaskName,
				SchemaNameS:   s.SeqMigrateTask.SchemaNameS,
				SequenceNameS: s.SeqMigrateTask.SequenceNameS,
			}, map[string]interface{}{
				"TaskStatus":      constant.TaskDatabaseStatusSuccess,
				"SourceSqlDigest": seqDigestS,
				"TargetSqlDigest": seqDigestT,
				"IsCompatible":    constant.DatabaseMigrateSequenceCompatible,
				"Duration":        time.Now().Sub(s.TaskStartTime).Seconds(),
				"ErrorDetail":     "",
			})
			if err != nil {
				return err
			}
			_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
				TaskName:    s.TaskName,
				SchemaNameS: s.SeqMigrateTask.SchemaNameS,
				LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] schema_name_s [%s] sequence_name_s [%s] migrate write database success, cost [%v]",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(constant.TaskModeSequenceMigrate),
					s.TaskName,
					s.TaskFlow,
					s.SeqMigrateTask.SchemaNameS,
					s.SeqMigrateTask.SequenceNameS,
					duration,
				),
			})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	case constant.TaskFlowOracleToMySQL, constant.TaskFlowPostgresToMySQL:
		version, err := s.DatasourceT.GetDatabaseVersion()
		if err != nil {
			return err
		}
		if stringutil.VersionOrdinal(version) < stringutil.VersionOrdinal(constant.MYSQLDatabaseSequenceSupportVersion) {
			err = model.Transaction(s.Ctx, func(txnCtx context.Context) error {
				duration := fmt.Sprintf("%f", time.Now().Sub(s.TaskStartTime).Seconds())
				_, err = model.GetISequenceMigrateTaskRW().UpdateSequenceMigrateTask(txnCtx, &task.SequenceMigrateTask{
					TaskName:      s.TaskName,
					SchemaNameS:   s.SeqMigrateTask.SchemaNameS,
					SequenceNameS: s.SeqMigrateTask.SequenceNameS,
				}, map[string]interface{}{
					"TaskStatus":      constant.TaskDatabaseStatusSuccess,
					"SourceSqlDigest": seqDigestS,
					"TargetSqlDigest": seqDigestT,
					"IsCompatible":    constant.DatabaseMigrateSequenceNotCompatible,
					"Duration":        time.Now().Sub(s.TaskStartTime).Seconds(),
					"ErrorDetail":     "",
				})
				if err != nil {
					return err
				}
				_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
					TaskName:    s.TaskName,
					SchemaNameS: s.SeqMigrateTask.SchemaNameS,
					LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] schema_name_s [%s] sequence_name_s [%s] migrate write database success, cost [%v]",
						stringutil.CurrentTimeFormatString(),
						stringutil.StringLower(constant.TaskModeSequenceMigrate),
						s.TaskName,
						s.TaskFlow,
						s.SeqMigrateTask.SchemaNameS,
						s.SeqMigrateTask.SequenceNameS,
						duration,
					),
				})
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return err
			}
			return nil
		}
		err = model.Transaction(s.Ctx, func(txnCtx context.Context) error {
			duration := fmt.Sprintf("%f", time.Now().Sub(s.TaskStartTime).Seconds())
			_, err = model.GetISequenceMigrateTaskRW().UpdateSequenceMigrateTask(txnCtx, &task.SequenceMigrateTask{
				TaskName:      s.TaskName,
				SchemaNameS:   s.SeqMigrateTask.SchemaNameS,
				SequenceNameS: s.SeqMigrateTask.SequenceNameS,
			}, map[string]interface{}{
				"TaskStatus":      constant.TaskDatabaseStatusSuccess,
				"SourceSqlDigest": seqDigestS,
				"TargetSqlDigest": seqDigestT,
				"IsCompatible":    constant.DatabaseMigrateSequenceCompatible,
				"Duration":        time.Now().Sub(s.TaskStartTime).Seconds(),
				"ErrorDetail":     "",
			})
			if err != nil {
				return err
			}
			_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
				TaskName:    s.TaskName,
				SchemaNameS: s.SeqMigrateTask.SchemaNameS,
				LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] schema_name_s [%s] sequence_name_s [%s] migrate write database success, cost [%v]",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(constant.TaskModeSequenceMigrate),
					s.TaskName,
					s.TaskFlow,
					s.SeqMigrateTask.SchemaNameS,
					s.SeqMigrateTask.SequenceNameS,
					duration,
				),
			})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("the task [%v] task_flow [%v] isn't support, please contact author or reselect", s.TaskName, s.TaskFlow)
	}
}

func (s *SequenceMigrateDatabase) SyncSequenceDatabase() error {
	seqDigestS, err := stringutil.Encrypt(s.SeqMigrateTask.SourceSqlDigest, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	seqDigestT, err := stringutil.Encrypt(s.SeqMigrateTask.TargetSqlDigest, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	switch s.TaskFlow {
	case constant.TaskFlowOracleToTiDB, constant.TaskFlowPostgresToTiDB:
		version, err := s.DatasourceT.GetDatabaseVersion()
		if err != nil {
			return err
		}
		if stringutil.VersionOrdinal(version) < stringutil.VersionOrdinal(constant.TIDBDatabaseSequenceSupportVersion) {
			err = model.Transaction(s.Ctx, func(txnCtx context.Context) error {
				duration := fmt.Sprintf("%f", time.Now().Sub(s.TaskStartTime).Seconds())
				errMsg := fmt.Sprintf("the downstream database version [%s] is not support, please checking", version)
				_, err = model.GetISequenceMigrateTaskRW().UpdateSequenceMigrateTask(txnCtx, &task.SequenceMigrateTask{
					TaskName:      s.TaskName,
					SchemaNameS:   s.SeqMigrateTask.SchemaNameS,
					SequenceNameS: s.SeqMigrateTask.SequenceNameS,
				}, map[string]interface{}{
					"TaskStatus":      constant.TaskDatabaseStatusFailed,
					"SourceSqlDigest": seqDigestS,
					"TargetSqlDigest": seqDigestT,
					"IsCompatible":    constant.DatabaseMigrateSequenceNotCompatible,
					"Duration":        duration,
					"ErrorDetail":     errMsg,
				})
				if err != nil {
					return err
				}
				_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
					TaskName:    s.TaskName,
					SchemaNameS: s.SeqMigrateTask.SchemaNameS,
					LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] schema_name_s [%s] sequence_name_s [%s] migrate sync database failed, cost [%v]",
						stringutil.CurrentTimeFormatString(),
						stringutil.StringLower(constant.TaskModeSequenceMigrate),
						s.TaskName,
						s.TaskFlow,
						s.SeqMigrateTask.SchemaNameS,
						s.SeqMigrateTask.SequenceNameS,
						duration,
					),
				})
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return err
			}

			return nil
		}

		_, err = s.DatasourceT.ExecContext(s.Ctx, s.SeqMigrateTask.TargetSqlDigest)
		if err != nil {
			errMsg := fmt.Errorf("the datasource exec sync sequence [%v] failed: [%v]", s.SeqMigrateTask.SequenceNameS, err)

			logger.Error("sequence migrate task processor",
				zap.String("task_name", s.TaskName),
				zap.String("task_mode", s.TaskMode),
				zap.String("task_flow", s.TaskFlow),
				zap.String("schema_name_s", s.SeqMigrateTask.SchemaNameS),
				zap.String("sequence_name_s", s.SeqMigrateTask.SequenceNameS),
				zap.String("schema_name_t", s.SeqMigrateTask.SchemaNameT),
				zap.String("sequence_sql_t", s.SeqMigrateTask.TargetSqlDigest),
				zap.String("task_stage", "sequence create failed"),
				zap.Error(errMsg))

			err = model.Transaction(s.Ctx, func(txnCtx context.Context) error {
				duration := fmt.Sprintf("%f", time.Now().Sub(s.TaskStartTime).Seconds())
				_, err = model.GetISequenceMigrateTaskRW().UpdateSequenceMigrateTask(txnCtx, &task.SequenceMigrateTask{
					TaskName:      s.TaskName,
					SchemaNameS:   s.SeqMigrateTask.SchemaNameS,
					SequenceNameS: s.SeqMigrateTask.SequenceNameS,
				}, map[string]interface{}{
					"TaskStatus":      constant.TaskDatabaseStatusFailed,
					"ErrorDetail":     errMsg,
					"SourceSqlDigest": seqDigestS,
					"TargetSqlDigest": seqDigestT,
					"IsCompatible":    constant.DatabaseMigrateSequenceCompatible,
					"Duration":        duration,
				})
				if err != nil {
					return err
				}
				_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
					TaskName:    s.TaskName,
					SchemaNameS: s.SeqMigrateTask.SchemaNameS,
					LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] schema_name_s [%s] sequence_name_s [%s] migrate sync database failed, cost [%v]",
						stringutil.CurrentTimeFormatString(),
						stringutil.StringLower(constant.TaskModeSequenceMigrate),
						s.TaskName,
						s.TaskFlow,
						s.SeqMigrateTask.SchemaNameS,
						s.SeqMigrateTask.SequenceNameS,
						duration,
					),
				})
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return err
			}

			return nil
		}
		err = model.Transaction(s.Ctx, func(txnCtx context.Context) error {
			duration := fmt.Sprintf("%f", time.Now().Sub(s.TaskStartTime).Seconds())
			_, err = model.GetISequenceMigrateTaskRW().UpdateSequenceMigrateTask(txnCtx, &task.SequenceMigrateTask{
				TaskName:      s.TaskName,
				SchemaNameS:   s.SeqMigrateTask.SchemaNameS,
				SequenceNameS: s.SeqMigrateTask.SequenceNameS,
			}, map[string]interface{}{
				"TaskStatus":      constant.TaskDatabaseStatusSuccess,
				"SourceSqlDigest": seqDigestS,
				"TargetSqlDigest": seqDigestT,
				"IsCompatible":    constant.DatabaseMigrateSequenceCompatible,
				"Duration":        time.Now().Sub(s.TaskStartTime).Seconds(),
				"ErrorDetail":     "",
			})
			if err != nil {
				return err
			}
			_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
				TaskName:    s.TaskName,
				SchemaNameS: s.SeqMigrateTask.SchemaNameS,
				LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] schema_name_s [%s] sequence_name_s [%s] migrate sync database success, cost [%v]",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(constant.TaskModeSequenceMigrate),
					s.TaskName,
					s.TaskFlow,
					s.SeqMigrateTask.SchemaNameS,
					s.SeqMigrateTask.SequenceNameS,
					duration,
				),
			})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	case constant.TaskFlowOracleToMySQL, constant.TaskFlowPostgresToMySQL:
		version, err := s.DatasourceT.GetDatabaseVersion()
		if err != nil {
			return err
		}
		if stringutil.VersionOrdinal(version) < stringutil.VersionOrdinal(constant.MYSQLDatabaseSequenceSupportVersion) {
			err = model.Transaction(s.Ctx, func(txnCtx context.Context) error {
				duration := fmt.Sprintf("%f", time.Now().Sub(s.TaskStartTime).Seconds())
				errMsg := fmt.Errorf("the downstream database version [%s] is not support, please checking", version)

				logger.Error("sequence migrate task processor",
					zap.String("task_name", s.TaskName),
					zap.String("task_mode", s.TaskMode),
					zap.String("task_flow", s.TaskFlow),
					zap.String("schema_name_s", s.SeqMigrateTask.SchemaNameS),
					zap.String("sequence_name_s", s.SeqMigrateTask.SequenceNameS),
					zap.String("schema_name_t", s.SeqMigrateTask.SchemaNameT),
					zap.String("sequence_sql_t", s.SeqMigrateTask.TargetSqlDigest),
					zap.String("task_stage", "sequence create failed"),
					zap.Error(errMsg))

				_, err = model.GetISequenceMigrateTaskRW().UpdateSequenceMigrateTask(txnCtx, &task.SequenceMigrateTask{
					TaskName:      s.TaskName,
					SchemaNameS:   s.SeqMigrateTask.SchemaNameS,
					SequenceNameS: s.SeqMigrateTask.SequenceNameS,
				}, map[string]interface{}{
					"TaskStatus":      constant.TaskDatabaseStatusFailed,
					"SourceSqlDigest": seqDigestS,
					"TargetSqlDigest": seqDigestT,
					"IsCompatible":    constant.DatabaseMigrateSequenceNotCompatible,
					"Duration":        duration,
					"ErrorDetail":     errMsg,
				})
				if err != nil {
					return err
				}
				_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
					TaskName:    s.TaskName,
					SchemaNameS: s.SeqMigrateTask.SchemaNameS,
					LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] schema_name_s [%s] sequence_name_s [%s] migrate sync database failed, cost [%v]",
						stringutil.CurrentTimeFormatString(),
						stringutil.StringLower(constant.TaskModeSequenceMigrate),
						s.TaskName,
						s.TaskFlow,
						s.SeqMigrateTask.SchemaNameS,
						s.SeqMigrateTask.SequenceNameS,
						duration,
					),
				})
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return err
			}
			return nil
		}

		_, err = s.DatasourceT.ExecContext(s.Ctx, s.SeqMigrateTask.TargetSqlDigest)
		if err != nil {
			errMsg := fmt.Errorf("the datasource exec sync sequence [%v] failed: [%v]", s.SeqMigrateTask.SequenceNameS, err)

			logger.Error("sequence migrate task processor",
				zap.String("task_name", s.TaskName),
				zap.String("task_mode", s.TaskMode),
				zap.String("task_flow", s.TaskFlow),
				zap.String("schema_name_s", s.SeqMigrateTask.SchemaNameS),
				zap.String("sequence_name_s", s.SeqMigrateTask.SequenceNameS),
				zap.String("schema_name_t", s.SeqMigrateTask.SchemaNameT),
				zap.String("sequence_sql_t", s.SeqMigrateTask.TargetSqlDigest),
				zap.String("task_stage", "sequence create failed"),
				zap.Error(errMsg))

			err = model.Transaction(s.Ctx, func(txnCtx context.Context) error {
				duration := fmt.Sprintf("%f", time.Now().Sub(s.TaskStartTime).Seconds())
				_, err = model.GetISequenceMigrateTaskRW().UpdateSequenceMigrateTask(txnCtx, &task.SequenceMigrateTask{
					TaskName:      s.TaskName,
					SchemaNameS:   s.SeqMigrateTask.SchemaNameS,
					SequenceNameS: s.SeqMigrateTask.SequenceNameS,
				}, map[string]interface{}{
					"TaskStatus":      constant.TaskDatabaseStatusFailed,
					"SourceSqlDigest": seqDigestS,
					"TargetSqlDigest": seqDigestT,
					"IsCompatible":    constant.DatabaseMigrateSequenceCompatible,
					"ErrorDetail":     errMsg,
					"Duration":        duration,
				})
				if err != nil {
					return err
				}
				_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
					TaskName:    s.TaskName,
					SchemaNameS: s.SeqMigrateTask.SchemaNameS,
					LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] schema_name_s [%s] sequence_name_s [%s] migrate sync database failed, cost [%v]",
						stringutil.CurrentTimeFormatString(),
						stringutil.StringLower(constant.TaskModeSequenceMigrate),
						s.TaskName,
						s.TaskFlow,
						s.SeqMigrateTask.SchemaNameS,
						s.SeqMigrateTask.SequenceNameS,
						duration,
					),
				})
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return err
			}

			return nil
		}
		err = model.Transaction(s.Ctx, func(txnCtx context.Context) error {
			duration := fmt.Sprintf("%f", time.Now().Sub(s.TaskStartTime).Seconds())
			_, err = model.GetISequenceMigrateTaskRW().UpdateSequenceMigrateTask(txnCtx, &task.SequenceMigrateTask{
				TaskName:      s.TaskName,
				SchemaNameS:   s.SeqMigrateTask.SchemaNameS,
				SequenceNameS: s.SeqMigrateTask.SequenceNameS,
			}, map[string]interface{}{
				"TaskStatus":      constant.TaskDatabaseStatusSuccess,
				"SourceSqlDigest": seqDigestS,
				"TargetSqlDigest": seqDigestT,
				"IsCompatible":    constant.DatabaseMigrateSequenceCompatible,
				"Duration":        time.Now().Sub(s.TaskStartTime).Seconds(),
				"ErrorDetail":     "",
			})
			if err != nil {
				return err
			}
			_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
				TaskName:    s.TaskName,
				SchemaNameS: s.SeqMigrateTask.SchemaNameS,
				LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] schema_name_s [%s] sequence_name_s [%s] migrate sync database success, cost [%v]",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(constant.TaskModeSequenceMigrate),
					s.TaskName,
					s.TaskFlow,
					s.SeqMigrateTask.SchemaNameS,
					s.SeqMigrateTask.SequenceNameS,
					duration,
				),
			})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("the task [%v] task_flow [%v] isn't support, please contact author or reselect", s.TaskName, s.TaskFlow)
	}
}
