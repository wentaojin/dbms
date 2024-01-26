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

	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model/rule"
	"go.uber.org/zap"

	"github.com/wentaojin/dbms/database"

	"github.com/wentaojin/dbms/database/oracle"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/datasource"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/pool"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type StructMigrateTask struct {
	Ctx          context.Context
	TaskName     string
	TaskFlow     string
	MigrateTasks []*task.StructMigrateTask
	DatasourceS  *datasource.Datasource
	DatasourceT  *datasource.Datasource
	TaskParams   *pb.StructMigrateParam
}

func (st *StructMigrateTask) Start() error {
	logger.Info("struct migrate task get rules",
		zap.String("task_name", st.TaskName),
		zap.String("task_flow", st.TaskFlow))

	taskInfo, err := model.GetITaskRW().GetTask(st.Ctx, &task.Task{TaskName: st.TaskName})
	if err != nil {
		return err
	}
	dbTypeSli := stringutil.StringSplit(st.TaskFlow, constant.StringSeparatorAite)
	buildInDatatypeRules, err := model.GetIBuildInDatatypeRuleRW().QueryBuildInDatatypeRule(st.Ctx, dbTypeSli[0], dbTypeSli[1])
	if err != nil {
		return err
	}
	buildInDefaultValueRules, err := model.GetBuildInDefaultValueRuleRW().QueryBuildInDefaultValueRule(st.Ctx, dbTypeSli[0], dbTypeSli[1])
	if err != nil {
		return err
	}

	groupSchemaTasks := make(map[string][]*task.StructMigrateTask)
	// the according schemaName, split task group for the migrateTasks
	groupSchemas := make(map[string]struct{})
	for _, m := range st.MigrateTasks {
		groupSchemas[m.SchemaNameS] = struct{}{}
	}

	for s, _ := range groupSchemas {
		var tasks []*task.StructMigrateTask
		for _, m := range st.MigrateTasks {
			if s == m.SchemaNameS {
				tasks = append(tasks, m)
			}
		}
		groupSchemaTasks[s] = tasks
	}

	// init database conn
	logger.Info("struct migrate task init connection",
		zap.String("task_name", st.TaskName),
		zap.String("task_flow", st.TaskFlow))
	targetSource, err := database.NewDatabase(st.Ctx, st.DatasourceT, "")
	if err != nil {
		return err
	}

	for sourceSchema, tasks := range groupSchemaTasks {
		schemaTaskTime := time.Now()

		sourceSource, err := database.NewDatabase(st.Ctx, st.DatasourceS, sourceSchema)
		if err != nil {
			return err
		}

		orac := sourceSource.(*oracle.Database)

		logger.Info("struct migrate task inspect task",
			zap.String("task_name", st.TaskName),
			zap.String("task_flow", st.TaskFlow))
		dbCharsetS, schemaCollationS, nlsComp, tableCollationS, dbCollationS, dbCharsetT, err := InspectStructMigrateTask(st.TaskName, st.TaskFlow, sourceSchema, orac, st.DatasourceS.ConnectCharset, st.DatasourceT.ConnectCharset)
		if err != nil {
			return err
		}

		// write schema
		logger.Info("struct migrate task get route",
			zap.String("task_name", st.TaskName),
			zap.String("task_flow", st.TaskFlow))
		schemaStartTime := time.Now()

		var createSchema string

		schemaRoute, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(st.Ctx, &rule.SchemaRouteRule{TaskName: st.TaskName, SchemaNameS: sourceSchema})
		if err != nil {
			return err
		}

		logger.Info("struct migrate task process schema",
			zap.String("task_name", st.TaskName),
			zap.String("task_flow", st.TaskFlow),
			zap.String("schema_name_s", schemaRoute.SchemaNameS),
			zap.String("schema_name_t", schemaRoute.SchemaNameT))
		switch {
		case strings.EqualFold(st.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(st.TaskFlow, constant.TaskFlowOracleToMySQL):
			if dbCollationS {
				targetSchemaCollation, ok := constant.MigrateTableStructureDatabaseCollationMap[st.TaskFlow][stringutil.StringUpper(schemaCollationS)][dbCharsetT]
				if !ok {
					return fmt.Errorf("oracle current task [%s] taskflow [%s] schema [%s] collation [%s] isn't support", st.TaskName, st.TaskFlow, sourceSchema, schemaCollationS)
				}
				if st.TaskParams.CreateIfNotExist {
					createSchema = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET %s COLLATE %s;", schemaRoute.SchemaNameT, dbCharsetT, targetSchemaCollation)
				} else {
					createSchema = fmt.Sprintf("CREATE DATABASE `%s` DEFAULT CHARACTER SET %s COLLATE %s;", schemaRoute.SchemaNameT, dbCharsetT, targetSchemaCollation)
				}
			} else {
				targetSchemaCollation, ok := constant.MigrateTableStructureDatabaseCollationMap[st.TaskFlow][stringutil.StringUpper(nlsComp)][dbCharsetT]
				if !ok {
					return fmt.Errorf("oracle current task [%s] taskflow [%s] schema [%s] nls_comp collation [%s] isn't support", st.TaskName, st.TaskFlow, sourceSchema, nlsComp)
				}
				if st.TaskParams.CreateIfNotExist {
					createSchema = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET %s COLLATE %s;", schemaRoute.SchemaNameT, dbCharsetT, targetSchemaCollation)
				} else {
					createSchema = fmt.Sprintf("CREATE DATABASE `%s` DEFAULT CHARACTER SET %s COLLATE %s;", schemaRoute.SchemaNameT, dbCharsetT, targetSchemaCollation)
				}
			}
		default:
			return fmt.Errorf("oracle current task [%s] taskflow [%s] schema [%s] isn't support, please contact author or reselect", st.TaskName, st.TaskFlow, sourceSchema)
		}

		encryptCreateSchema, err := stringutil.Encrypt(createSchema, []byte(constant.DefaultDataEncryptDecryptKey))
		if err != nil {
			return err
		}

		// schema create failed, return
		_, err = model.GetIStructMigrateTaskRW().CreateStructMigrateTask(st.Ctx, &task.StructMigrateTask{
			TaskName:        st.TaskName,
			SchemaNameS:     sourceSchema,
			TableTypeS:      constant.OracleDatabaseTableTypeSchema,
			SchemaNameT:     schemaRoute.SchemaNameT,
			TaskStatus:      constant.TaskDatabaseStatusSuccess,
			TargetSqlDigest: encryptCreateSchema,
			IsSchemaCreate:  constant.DatabaseIsSchemaCreateSqlYES,
			Duration:        time.Now().Sub(schemaStartTime).Seconds(),
		})
		if err != nil {
			return err
		}

		// direct write database -> schema
		if st.TaskParams.DirectWrite {
			_, err = targetSource.ExecContext(st.Ctx, createSchema)
			if err != nil {
				return err
			}
		}
		logger.Info("struct migrate task process tables",
			zap.String("task_name", st.TaskName),
			zap.String("task_flow", st.TaskFlow))

		p := pool.NewPool(st.Ctx, int(st.TaskParams.MigrateThread),
			pool.WithTaskQueueSize(int(st.TaskParams.TaskQueueSize)),
			pool.WithPanicHandle(true),
			pool.WithResultCallback(func(r pool.Result) {
				smt := r.Task.Job.(*task.StructMigrateTask)
				if r.Error != nil {
					logger.Warn("struct migrate task",
						zap.String("task_name", st.TaskName),
						zap.String("task_flow", st.TaskFlow),
						zap.String("schema_name_s", smt.SchemaNameS),
						zap.String("table_name_s", smt.TableNameS),
						zap.Error(r.Error))

					errW := model.Transaction(st.Ctx, func(txnCtx context.Context) error {
						_, err = model.GetIStructMigrateTaskRW().UpdateStructMigrateTask(txnCtx,
							&task.StructMigrateTask{TaskName: smt.TaskName, SchemaNameS: smt.SchemaNameS, TableNameS: smt.TableNameS},
							map[string]interface{}{
								"TaskStatus":  constant.TaskDatabaseStatusFailed,
								"ErrorDetail": r.Error.Error(),
							})
						if err != nil {
							return err
						}
						_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
							TaskName:    smt.TaskName,
							SchemaNameS: smt.SchemaNameS,
							TableNameS:  smt.TableNameS,
							LogDetail: fmt.Sprintf("%v [%v] struct migrate task [%v] taskflow [%v] source table [%v.%v] failed, please see [struct_migrate_task] detail",
								stringutil.CurrentTimeFormatString(),
								constant.TaskModeStructMigrate,
								smt.TaskName,
								st.TaskFlow,
								smt.SchemaNameS,
								smt.TableNameS),
						})
						if err != nil {
							return err
						}
						return nil
					})
					if errW != nil {
						panic(errW)
					}
				}
			}),
			pool.WithCanceledHandle(func(ctx context.Context, t pool.Task) error {
				smt := t.Job.(*task.StructMigrateTask)
				errW := model.Transaction(st.Ctx, func(txnCtx context.Context) error {
					tableS, err := model.GetIStructMigrateTaskRW().GetStructMigrateTaskTable(txnCtx,
						&task.StructMigrateTask{TaskName: smt.TaskName, SchemaNameS: smt.SchemaNameS, TableNameS: smt.TableNameS})
					if err != nil {
						return err
					}
					if len(tableS) > 0 {
						return fmt.Errorf("the table [struct_migrate_task] task_name [%v] schema_name_s [%v] and table_name_s [%s] records can't be over one", smt.TaskName, smt.SchemaNameS, smt.TableNameS)
					}

					if strings.EqualFold(tableS[0].TaskStatus, constant.TaskDatabaseStatusStopped) {
						return nil
					}

					_, err = model.GetIStructMigrateTaskRW().UpdateStructMigrateTask(txnCtx,
						&task.StructMigrateTask{TaskName: smt.TaskName, SchemaNameS: smt.SchemaNameS, TableNameS: smt.TableNameS},
						map[string]interface{}{
							"TaskStatus":  constant.TaskDatabaseStatusStopped,
							"ErrorDetail": fmt.Sprintf("the struct migrate task [%v] source table [%v.%v] had be canceled", smt.TaskName, smt.SchemaNameS, smt.TableNameS),
						})
					if err != nil {
						return err
					}
					_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
						TaskName:    smt.TaskName,
						SchemaNameS: smt.SchemaNameS,
						TableNameS:  smt.TableNameS,
						LogDetail: fmt.Sprintf("%v [%v] struct migrate task [%v] source table [%v.%v] had be canceled, please see [struct_migrate_task] detail",
							stringutil.CurrentTimeFormatString(),
							constant.TaskModeStructMigrate,
							smt.TaskName,
							smt.SchemaNameS,
							smt.TableNameS),
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
			}),
			pool.WithExecuteHandle(func(ctx context.Context, t pool.Task) error {
				startTime := time.Now()
				smt := t.Job.(*task.StructMigrateTask)

				// if the schema table success, skip
				if strings.EqualFold(smt.TaskStatus, constant.TaskDatabaseStatusSuccess) {
					logger.Info("struct migrate task",
						zap.String("task_name", st.TaskName),
						zap.String("task_flow", st.TaskFlow),
						zap.String("schema_name_s", smt.SchemaNameS),
						zap.String("table_name_s", smt.TableNameS),
						zap.String("task_status", constant.TaskDatabaseStatusSuccess),
						zap.String("table task had done", "skip migrate"),
						zap.String("cost", time.Now().Sub(startTime).String()))
					return nil
				}
				if strings.EqualFold(smt.TaskStatus, constant.TaskDatabaseStatusRunning) {
					logger.Info("struct migrate task",
						zap.String("task_name", st.TaskName),
						zap.String("task_flow", st.TaskFlow),
						zap.String("schema_name_s", smt.SchemaNameS),
						zap.String("table_name_s", smt.TableNameS),
						zap.String("task_status", constant.TaskDatabaseStatusRunning),
						zap.String("table task has running", "current status may panic, skip migrate, please double check"),
						zap.String("cost", time.Now().Sub(startTime).String()))
					return nil
				}

				err = model.Transaction(ctx, func(txnCtx context.Context) error {
					_, err = model.GetIStructMigrateTaskRW().UpdateStructMigrateTask(txnCtx, &task.StructMigrateTask{
						TaskName:    t.Name,
						SchemaNameS: smt.SchemaNameS,
						TableNameS:  smt.TableNameS},
						map[string]interface{}{
							"TaskStatus": constant.TaskDatabaseStatusRunning,
						})
					if err != nil {
						return err
					}
					_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
						TaskName:    t.Name,
						SchemaNameS: smt.SchemaNameS,
						TableNameS:  smt.TableNameS,
						LogDetail: fmt.Sprintf("%v [%v] the worker task [%v] source table [%v.%v] starting",
							stringutil.CurrentTimeFormatString(),
							stringutil.StringLower(constant.TaskModeStructMigrate),
							t.Name,
							smt.SchemaNameS,
							smt.TableNameS),
					})
					if err != nil {
						return err
					}
					return nil
				})
				if err != nil {
					return err
				}

				sourceTime := time.Now()
				dataSource := &Datasource{
					DatabaseS:        orac,
					SchemaNameS:      smt.SchemaNameS,
					TableNameS:       smt.TableNameS,
					TableTypeS:       smt.TableTypeS,
					CollationS:       dbCollationS,
					DBCharsetS:       dbCharsetS,
					DBCharsetT:       dbCharsetT,
					SchemaCollationS: schemaCollationS,
					TableCollationS:  tableCollationS[smt.TableNameS],
					DBNlsCompS:       nlsComp,
				}

				attrs, err := database.IDatabaseTableAttributes(dataSource)
				if err != nil {
					return err
				}
				logger.Info("struct migrate task",
					zap.String("task_name", st.TaskName),
					zap.String("task_flow", st.TaskFlow),
					zap.String("schema_name_s", smt.SchemaNameS),
					zap.String("table_name_s", smt.TableNameS),
					zap.String("task_stage", "datasource"),
					zap.String("datasource", dataSource.String()),
					zap.String("cost", time.Now().Sub(sourceTime).String()))
				ruleTime := time.Now()
				dataRule := &Rule{
					Ctx:                      ctx,
					TaskName:                 st.TaskName,
					TaskFlow:                 st.TaskFlow,
					SchemaNameS:              smt.SchemaNameS,
					TableNameS:               smt.TableNameS,
					TablePrimaryAttrs:        attrs.PrimaryKey,
					TableColumnsAttrs:        attrs.TableColumns,
					TableCommentAttrs:        attrs.TableComment,
					CreateIfNotExist:         st.TaskParams.CreateIfNotExist,
					CaseFieldRuleT:           taskInfo.CaseFieldRuleT,
					DBCollationS:             dbCollationS,
					DBCharsetS:               dbCharsetS,
					DBCharsetT:               dbCharsetT,
					BuildinDatatypeRules:     buildInDatatypeRules,
					BuildinDefaultValueRules: buildInDefaultValueRules,
				}

				rules, err := database.IDatabaseTableAttributesRule(dataRule)
				if err != nil {
					return err
				}
				logger.Info("struct migrate task",
					zap.String("task_name", st.TaskName),
					zap.String("task_flow", st.TaskFlow),
					zap.String("schema_name_s", smt.SchemaNameS),
					zap.String("table_name_s", smt.TableNameS),
					zap.String("task_stage", "rule"),
					zap.String("rule", dataRule.String()),
					zap.String("cost", time.Now().Sub(ruleTime).String()))

				tableTime := time.Now()
				dataTable := &Table{
					TaskName:            st.TaskName,
					TaskFlow:            st.TaskFlow,
					Datasource:          dataSource,
					TableAttributes:     attrs,
					TableAttributesRule: rules,
				}

				tableStruct, err := database.IDatabaseTableStruct(dataTable)
				if err != nil {
					return err
				}

				logger.Info("struct migrate task",
					zap.String("task_name", st.TaskName),
					zap.String("task_flow", st.TaskFlow),
					zap.String("schema_name_s", smt.SchemaNameS),
					zap.String("table_name_s", smt.TableNameS),
					zap.String("task_stage", "struct"),
					zap.String("struct", dataTable.String()),
					zap.String("cost", time.Now().Sub(tableTime).String()))

				writerTime := time.Now()
				var w database.ITableStructDatabaseWriter
				w = NewStructMigrateDatabase(ctx, st.TaskName, st.TaskFlow, targetSource, startTime, tableStruct)

				if st.TaskParams.DirectWrite {
					err = w.SyncStructDatabase()
					if err != nil {
						return err
					}
					logger.Info("struct migrate task",
						zap.String("task_name", st.TaskName),
						zap.String("task_flow", st.TaskFlow),
						zap.String("schema_name_s", smt.SchemaNameS),
						zap.String("table_name_s", smt.TableNameS),
						zap.String("task_stage", "struct sync database"),
						zap.String("cost", time.Now().Sub(writerTime).String()))

					return nil
				}

				err = w.WriteStructDatabase()
				if err != nil {
					return err
				}
				logger.Info("struct migrate task",
					zap.String("task_name", st.TaskName),
					zap.String("task_flow", st.TaskFlow),
					zap.String("schema_name_s", smt.SchemaNameS),
					zap.String("table_name_s", smt.TableNameS),
					zap.String("task_stage", "struct write database"),
					zap.String("cost", time.Now().Sub(writerTime).String()))

				return nil
			}))

		for _, taskJob := range tasks {
			p.SubmitTask(pool.Task{
				Name:  st.TaskName,
				Group: st.TaskName,
				Job:   taskJob,
			})
		}

		p.Wait()

		p.Release()

		err = sourceSource.Close()
		if err != nil {
			return err
		}
		logger.Info("struct migrate task",
			zap.String("task_name", st.TaskName),
			zap.String("task_flow", st.TaskFlow),
			zap.String("schema_name_s", sourceSchema),
			zap.String("cost", time.Now().Sub(schemaTaskTime).String()))
	}
	return nil
}

func InspectStructMigrateTask(taskName, taskFlow string, schemaNameS string, orac *oracle.Database, connectDBCharsetS, connectDBCharsetT string) (string, string, string, map[string]string, bool, string, error) {
	var (
		dbCharsetS       string
		dbCharsetT       string
		schemaCollationS string
	)
	tableCollationS := make(map[string]string)

	dbCharsetS = stringutil.StringUpper(connectDBCharsetS)

	dbCharsetT = constant.MigrateTableStructureDatabaseCharsetMap[taskFlow][dbCharsetS]
	if !strings.EqualFold(connectDBCharsetT, dbCharsetT) {
		return dbCharsetS, schemaCollationS, "", tableCollationS, false, dbCharsetT, fmt.Errorf("oracle current subtask [%s] taskflow [%s] schema [%s] mapping charset [%s] isn't equal with database connect charset [%s], please adjust database connect charset", taskName, taskFlow, schemaNameS, dbCharsetS, connectDBCharsetT)
	}

	dbCharset, err := orac.GetDatabaseCharset()
	if err != nil {
		return dbCharsetS, schemaCollationS, "", tableCollationS, false, dbCharsetT, err
	}
	nlsComp, nlsSort, err := orac.GetDatabaseCharsetCollation()
	if err != nil {
		return dbCharsetS, schemaCollationS, "", tableCollationS, false, dbCharsetT, err
	}
	if _, ok := constant.MigrateTableStructureDatabaseCollationMap[taskFlow][stringutil.StringUpper(nlsComp)][constant.MigrateTableStructureDatabaseCharsetMap[taskFlow][dbCharset]]; !ok {
		return dbCharsetS, schemaCollationS, nlsComp, tableCollationS, false, dbCharsetT,
			fmt.Errorf("oracle database nls comp [%s] , mysql db isn't support", nlsComp)
	}
	if _, ok := constant.MigrateTableStructureDatabaseCollationMap[taskFlow][stringutil.StringUpper(nlsSort)][constant.MigrateTableStructureDatabaseCharsetMap[taskFlow][dbCharset]]; !ok {
		return dbCharsetS, schemaCollationS, nlsComp, tableCollationS, false, dbCharsetT, fmt.Errorf("oracle database nls sort [%s] , mysql db isn't support", nlsSort)
	}

	if !strings.EqualFold(nlsSort, nlsComp) {
		return dbCharsetS, schemaCollationS, nlsComp, tableCollationS, false, dbCharsetT, fmt.Errorf("oracle database nls_sort [%s] and nls_comp [%s] isn't different, need be equal; because mysql db isn't support", nlsSort, nlsComp)
	}

	// whether the oracle version can specify table and field collation，if the oracle database version is 12.2 and the above version, it's specify table and field collation, otherwise can't specify
	// oracle database nls_sort/nls_comp value need to be equal, USING_NLS_COMP value is nls_comp
	oracleDBVersion, err := orac.GetDatabaseVersion()
	if err != nil {
		return dbCharsetS, schemaCollationS, nlsComp, tableCollationS, false, dbCharsetT, err
	}

	oracleCollation := false
	if stringutil.VersionOrdinal(oracleDBVersion) >= stringutil.VersionOrdinal(constant.OracleDatabaseTableAndColumnSupportVersion) {
		oracleCollation = true
	}

	if oracleCollation {
		schemaCollationS, err = orac.GetDatabaseSchemaCollation(schemaNameS)
		if err != nil {
			return dbCharsetS, schemaCollationS, nlsComp, tableCollationS, false, dbCharsetT, err
		}
		tableCollationS, err = orac.GetDatabaseSchemaTableCollation(schemaNameS, schemaCollationS)
		if err != nil {
			return dbCharsetS, schemaCollationS, nlsComp, tableCollationS, false, dbCharsetT, err
		}
	}
	return dbCharsetS, schemaCollationS, nlsComp, tableCollationS, oracleCollation, dbCharsetT, nil
}
