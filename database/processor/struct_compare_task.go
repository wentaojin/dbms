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

	"github.com/golang/snappy"
	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/buildin"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/thread"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"github.com/wentaojin/dbms/utils/structure"
	"go.uber.org/zap"
)

type StructCompareTask struct {
	Ctx                       context.Context
	Task                      *task.Task
	DBTypeS                   string
	DBTypeT                   string
	DatabaseS                 database.IDatabase
	DatabaseT                 database.IDatabase
	DBCharsetS                string
	DBCharsetT                string
	SchemaNameS               string
	SchemaNameT               string
	StartTime                 time.Time
	TaskParams                *pb.StructCompareParam
	BuildInDatatypeRulesS     []*buildin.BuildinDatatypeRule
	BuildInDefaultValueRulesS []*buildin.BuildinDefaultvalRule
	BuildInDatatypeRulesT     []*buildin.BuildinDatatypeRule
	BuildInDefaultValueRulesT []*buildin.BuildinDefaultvalRule

	ReadyInit chan bool
}

func (dmt *StructCompareTask) Init() error {
	defer close(dmt.ReadyInit)

	logger.Info("struct compare task init table",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))

	if !dmt.TaskParams.EnableCheckpoint {
		err := model.GetIStructCompareSummaryRW().DeleteStructCompareSummaryName(dmt.Ctx, []string{dmt.Task.TaskName})
		if err != nil {
			return err
		}
		err = model.GetIStructCompareTaskRW().DeleteStructCompareTaskName(dmt.Ctx, []string{dmt.Task.TaskName})
		if err != nil {
			return err
		}
	}
	logger.Warn("struct compare task checkpoint skip",
		zap.String("task_name", dmt.Task.TaskName),
		zap.String("task_mode", dmt.Task.TaskMode),
		zap.String("task_flow", dmt.Task.TaskFlow),
		zap.Bool("enable_checkpoint", dmt.TaskParams.EnableCheckpoint))
	s, err := model.GetIStructCompareSummaryRW().GetStructCompareSummary(dmt.Ctx, &task.StructCompareSummary{TaskName: dmt.Task.TaskName, SchemaNameS: dmt.SchemaNameS})
	if err != nil {
		return err
	}

	if strings.EqualFold(s.InitFlag, constant.TaskInitStatusNotFinished) {
		err = model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
			err = model.GetIStructCompareSummaryRW().DeleteStructCompareSummaryName(txnCtx, []string{dmt.Task.TaskName})
			if err != nil {
				return err
			}
			err = model.GetIStructCompareTaskRW().DeleteStructCompareTaskName(txnCtx, []string{dmt.Task.TaskName})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	} else {
		dmt.ReadyInit <- true
	}

	// filter database table
	schemaTaskTables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(dmt.Ctx, &rule.MigrateTaskTable{
		TaskName:    dmt.Task.TaskName,
		SchemaNameS: dmt.SchemaNameS,
	})
	if err != nil {
		return err
	}
	var (
		includeTables         []string
		excludeTables         []string
		databaseTaskTableSliS []string // task tables
		databaseTaskTableSliT []string
	)
	databaseTableTypeMap := make(map[string]string)
	taskTablesMap := make(map[string]struct{})

	for _, t := range schemaTaskTables {
		if strings.EqualFold(t.IsExclude, constant.MigrateTaskTableIsExclude) {
			excludeTables = append(excludeTables, t.TableNameS)
		}
		if strings.EqualFold(t.IsExclude, constant.MigrateTaskTableIsNotExclude) {
			includeTables = append(includeTables, t.TableNameS)
		}
	}

	tableObjs, err := dmt.DatabaseS.FilterDatabaseTable(dmt.SchemaNameS, includeTables, excludeTables)
	if err != nil {
		return err
	}

	tableRouteRuleS := make(map[string]string)
	tableRouteRuleT := make(map[string]string)

	// rule case field
	ruleStart := time.Now()
	for _, t := range tableObjs.TaskTables {
		var (
			tabNameS string
			tabNameT string
		)
		// the according target case field rule convert
		if strings.EqualFold(dmt.Task.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleLower) {
			tabNameS = stringutil.StringLower(t)
		}
		if strings.EqualFold(dmt.Task.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleUpper) {
			tabNameS = stringutil.StringUpper(t)
		}
		if strings.EqualFold(dmt.Task.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleOrigin) {
			tabNameS = t
		}
		if strings.EqualFold(dmt.Task.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleLower) {
			tabNameT = stringutil.StringLower(t)
		}
		if strings.EqualFold(dmt.Task.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleUpper) {
			tabNameT = stringutil.StringUpper(t)
		}
		if strings.EqualFold(dmt.Task.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleOrigin) {
			tabNameT = t
		}
		databaseTaskTableSliS = append(databaseTaskTableSliS, tabNameS)
		databaseTaskTableSliT = append(databaseTaskTableSliT, tabNameT)

		taskTablesMap[tabNameS] = struct{}{}

		tableRouteRuleS[tabNameS] = tabNameT
		tableRouteRuleT[tabNameT] = tabNameS
	}

	logger.Info("struct compare task rule case",
		zap.Any("table_route_rule_s", tableRouteRuleS),
		zap.Any("table_route_rule_t", tableRouteRuleT),
		zap.String("cost", time.Now().Sub(ruleStart).String()))

	routeStart := time.Now()
	databaseTableTypeMap, err = dmt.DatabaseS.GetDatabaseTableType(dmt.SchemaNameS)
	if err != nil {
		return err
	}

	allTablesT, err := dmt.DatabaseT.GetDatabaseTable(dmt.SchemaNameT)
	if err != nil {
		return err
	}
	// get table route rule
	tableRoutes, err := model.GetIMigrateTableRouteRW().FindTableRouteRule(dmt.Ctx, &rule.TableRouteRule{
		TaskName:    dmt.Task.TaskName,
		SchemaNameS: dmt.SchemaNameS,
	})
	if len(tableRoutes) > 0 {
		for _, tr := range tableRoutes {
			if val, ok := tableRouteRuleS[tr.TableNameS]; ok {
				tableRouteRuleS[tr.TableNameS] = tr.TableNameT
			} else {
				return fmt.Errorf("the database table_name_s [%s] route table_name_t [%s] rule not exist, current case field rule table_name_t value is [%s] ", tr.TableNameS, tr.TableNameT, val)
			}
			if val, ok := tableRouteRuleT[tr.TableNameT]; ok {
				tableRouteRuleT[tr.TableNameT] = tr.TableNameS
			} else {
				return fmt.Errorf("the database table_name_t [%s] route table_name_s [%s] rule not exist, current case field rule table_name_s value is [%s] ", tr.TableNameT, tr.TableNameS, val)
			}
		}
	}

	tableRouteRuleTNew := make(map[string]string)
	for _, t := range allTablesT {
		tableRouteRuleTNew[t] = t
	}

	var panicTables []string
	for tab, _ := range tableRouteRuleT {
		if _, ok := tableRouteRuleTNew[tab]; !ok {
			panicTables = append(panicTables, tab)
		}
	}

	logger.Info("struct compare task route rule",
		zap.Any("table_route_rule_s", tableRouteRuleS),
		zap.Any("table_route_rule_t", tableRouteRuleT),
		zap.String("cost", time.Now().Sub(routeStart).String()))

	if len(panicTables) > 0 {
		return fmt.Errorf("the task [%v] task_flow [%v] task_mode [%v] source database tables aren't existed in the target database, please create the tables [%v]", dmt.Task.TaskName, dmt.Task.TaskFlow, dmt.Task.TaskMode, stringutil.StringJoin(panicTables, constant.StringSeparatorComma))
	}

	// clear the struct compare task table
	migrateTasks, err := model.GetIStructCompareTaskRW().BatchFindStructCompareTask(dmt.Ctx, &task.StructCompareTask{TaskName: dmt.Task.TaskName})
	if err != nil {
		return err
	}

	// repeatInitTableMap used for store the struct_migrate_task table name has be finished, avoid repeated initialization
	repeatInitTableMap := make(map[string]struct{})
	if len(migrateTasks) > 0 {
		for _, smt := range migrateTasks {
			if _, ok := taskTablesMap[smt.TableNameS]; !ok {
				err = model.GetIStructCompareTaskRW().DeleteStructCompareTask(dmt.Ctx, smt.ID)
				if err != nil {
					return err
				}
			} else {
				repeatInitTableMap[smt.TableNameS] = struct{}{}
			}
		}
	}

	err = model.GetIStructCompareSummaryRW().DeleteStructCompareSummary(dmt.Ctx, &task.StructCompareSummary{TaskName: dmt.Task.TaskName})
	if err != nil {
		return err
	}

	// database tables
	// init database table
	// get table column route rule
	for _, sourceTable := range databaseTaskTableSliS {
		// if the table is existed, then skip init
		if _, ok := repeatInitTableMap[sourceTable]; ok {
			continue
		}
		var (
			targetTable string
		)
		if val, ok := tableRouteRuleS[sourceTable]; ok {
			targetTable = val
		} else {
			// the according target case field rule convert
			if strings.EqualFold(dmt.Task.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleLower) {
				targetTable = stringutil.StringLower(sourceTable)
			}
			if strings.EqualFold(dmt.Task.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleUpper) {
				targetTable = stringutil.StringUpper(sourceTable)
			}
			if strings.EqualFold(dmt.Task.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleOrigin) {
				targetTable = sourceTable
			}
		}

		_, err = model.GetIStructCompareTaskRW().CreateStructCompareTask(dmt.Ctx, &task.StructCompareTask{
			TaskName:    dmt.Task.TaskName,
			SchemaNameS: dmt.SchemaNameS,
			TableNameS:  sourceTable,
			TableTypeS:  databaseTableTypeMap[sourceTable],
			SchemaNameT: dmt.SchemaNameT,
			TableNameT:  targetTable,
			TaskStatus:  constant.TaskDatabaseStatusWaiting,
		})
		if err != nil {
			return err
		}
	}

	_, err = model.GetIStructCompareSummaryRW().CreateStructCompareSummary(dmt.Ctx,
		&task.StructCompareSummary{
			TaskName:    dmt.Task.TaskName,
			SchemaNameS: dmt.SchemaNameS,
			TableTotals: uint64(len(databaseTaskTableSliS)),
			InitFlag:    constant.TaskInitStatusFinished,
			CompareFlag: constant.TaskCompareStatusNotFinished,
		})
	if err != nil {
		return err
	}

	dmt.ReadyInit <- true
	return nil
}

func (dmt *StructCompareTask) Run() error {
	for ready := range dmt.ReadyInit {
		if ready {
			err := dmt.Process()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (dmt *StructCompareTask) Resume() error {
	logger.Info("struct compare task resume table",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))
	return nil
}

func (dmt *StructCompareTask) Last() error {
	logger.Info("struct compare task last table",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))
	return nil
}

func (dmt *StructCompareTask) Process() error {
	logger.Info("struct compare task run table",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))
	s, err := model.GetIStructCompareSummaryRW().GetStructCompareSummary(dmt.Ctx, &task.StructCompareSummary{TaskName: dmt.Task.TaskName, SchemaNameS: dmt.SchemaNameS})
	if err != nil {
		return err
	}

	if strings.EqualFold(s.CompareFlag, constant.TaskCompareStatusFinished) {
		logger.Warn("struct compare task process skip",
			zap.String("task_name", dmt.Task.TaskName),
			zap.String("task_mode", dmt.Task.TaskMode),
			zap.String("task_flow", dmt.Task.TaskFlow),
			zap.String("init_flag", s.InitFlag),
			zap.String("compare_flag", s.CompareFlag),
			zap.String("action", "compare skip"))
		_, err = model.GetIStructCompareSummaryRW().UpdateStructCompareSummary(dmt.Ctx,
			&task.StructCompareSummary{
				TaskName:    dmt.Task.TaskName,
				SchemaNameS: dmt.SchemaNameS},
			map[string]interface{}{
				"CompareFlag": constant.TaskCompareStatusFinished,
				"Duration":    fmt.Sprintf("%f", time.Now().Sub(dmt.StartTime).Seconds()),
			})
		if err != nil {
			return err
		}
		return nil
	}
	logger.Info("struct compare task get tables",
		zap.String("task_name", dmt.Task.TaskName),
		zap.String("task_mode", dmt.Task.TaskMode),
		zap.String("task_flow", dmt.Task.TaskFlow),
		zap.String("schema_name_s", dmt.SchemaNameS))

	var migrateTasks []*task.StructCompareTask
	err = model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
		// get migrate task tables
		migrateTasks, err = model.GetIStructCompareTaskRW().FindStructCompareTask(txnCtx,
			&task.StructCompareTask{
				TaskName:    dmt.Task.TaskName,
				SchemaNameS: dmt.SchemaNameS,
				TaskStatus:  constant.TaskDatabaseStatusWaiting,
			})
		if err != nil {
			return err
		}
		migrateFailedTasks, err := model.GetIStructCompareTaskRW().FindStructCompareTask(txnCtx,
			&task.StructCompareTask{
				TaskName:    dmt.Task.TaskName,
				SchemaNameS: dmt.SchemaNameS,
				TaskStatus:  constant.TaskDatabaseStatusFailed})
		if err != nil {
			return err
		}
		migrateRunningTasks, err := model.GetIStructCompareTaskRW().FindStructCompareTask(txnCtx,
			&task.StructCompareTask{
				TaskName:    dmt.Task.TaskName,
				SchemaNameS: dmt.SchemaNameS,
				TaskStatus:  constant.TaskDatabaseStatusRunning})
		if err != nil {
			return err
		}
		migrateStopTasks, err := model.GetIStructCompareTaskRW().FindStructCompareTask(txnCtx,
			&task.StructCompareTask{
				TaskName:    dmt.Task.TaskName,
				SchemaNameS: dmt.SchemaNameS,
				TaskStatus:  constant.TaskDatabaseStatusStopped})
		if err != nil {
			return err
		}
		migrateTasks = append(migrateTasks, migrateFailedTasks...)
		migrateTasks = append(migrateTasks, migrateRunningTasks...)
		migrateTasks = append(migrateTasks, migrateStopTasks...)
		return nil
	})
	if err != nil {
		return err
	}

	logger.Info("struct compare task process table",
		zap.String("task_name", dmt.Task.TaskName),
		zap.String("task_mode", dmt.Task.TaskMode),
		zap.String("task_flow", dmt.Task.TaskFlow),
		zap.String("schema_name_s", dmt.SchemaNameS))

	g := thread.NewGroup()
	g.SetLimit(int(dmt.TaskParams.CompareThread))

	go func() {
		for _, j := range migrateTasks {
			g.Go(j, func(j interface{}) error {
				dt := j.(*task.StructCompareTask)
				gTime := time.Now()

				logger.Info("struct compare task process table",
					zap.String("task_name", dmt.Task.TaskName),
					zap.String("task_mode", dmt.Task.TaskMode),
					zap.String("task_flow", dmt.Task.TaskFlow),
					zap.String("schema_name_s", dmt.SchemaNameS),
					zap.String("table_name_s", dt.TableNameS))
				errW := model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
					_, err = model.GetIStructCompareTaskRW().UpdateStructCompareTask(txnCtx,
						&task.StructCompareTask{TaskName: dt.TaskName, SchemaNameS: dt.SchemaNameS, TableNameS: dt.TableNameS},
						map[string]interface{}{
							"TaskStatus": constant.TaskDatabaseStatusRunning,
						})
					if err != nil {
						return err
					}
					_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
						TaskName:    dt.TaskName,
						SchemaNameS: dt.SchemaNameS,
						TableNameS:  dt.TableNameS,
						LogDetail: fmt.Sprintf("%v [%v] struct compare task [%v] taskflow [%v] source table [%v.%v] compare start",
							stringutil.CurrentTimeFormatString(),
							stringutil.StringLower(dmt.Task.TaskMode),
							dt.TaskName,
							dmt.Task.TaskFlow,
							dt.SchemaNameS,
							dt.TableNameS),
					})
					if err != nil {
						return err
					}
					return nil
				})
				if errW != nil {
					return errW
				}

				switch dmt.Task.TaskFlow {
				case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL, constant.TaskFlowPostgresToTiDB, constant.TaskFlowPostgresToMySQL:
					upProcTime := time.Now()
					var upstreamProcessor *structure.Table
					if strings.EqualFold(dmt.DBTypeS, constant.DatabaseTypeOracle) {
						upstreamProcessor, err = database.IStructCompareProcessor(&OracleProcessor{
							Ctx:                      dmt.Ctx,
							TaskName:                 dmt.Task.TaskName,
							TaskFlow:                 dmt.Task.TaskFlow,
							SchemaName:               dt.SchemaNameS,
							TableName:                dt.TableNameS,
							DBCharset:                dmt.DBCharsetS,
							Database:                 dmt.DatabaseS,
							BuildinDatatypeRules:     dmt.BuildInDatatypeRulesS,
							BuildinDefaultValueRules: dmt.BuildInDefaultValueRulesS,
							ColumnRouteRules:         make(map[string]string),
							IsBaseline:               true,
						}, dmt.TaskParams.IgnoreCaseCompare)
						if err != nil {
							return fmt.Errorf("the struct compare processor database [%s] failed: %v", dmt.DBTypeS, err)
						}
					} else if strings.EqualFold(dmt.DBTypeS, constant.DatabaseTypePostgresql) {
						upstreamProcessor, err = database.IStructCompareProcessor(&PostgresProcessor{
							Ctx:                      dmt.Ctx,
							TaskName:                 dmt.Task.TaskName,
							TaskFlow:                 dmt.Task.TaskFlow,
							SchemaName:               dt.SchemaNameS,
							TableName:                dt.TableNameS,
							DBCharset:                dmt.DBCharsetS,
							Database:                 dmt.DatabaseS,
							BuildinDatatypeRules:     dmt.BuildInDatatypeRulesS,
							BuildinDefaultValueRules: dmt.BuildInDefaultValueRulesS,
							ColumnRouteRules:         make(map[string]string),
							IsBaseline:               true,
						}, dmt.TaskParams.IgnoreCaseCompare)
						if err != nil {
							return fmt.Errorf("the struct compare processor database [%s] failed: %v", dmt.DBTypeS, err)
						}
					}

					logger.Info("struct compare task processor",
						zap.String("task_name", dmt.Task.TaskName),
						zap.String("task_mode", dmt.Task.TaskMode),
						zap.String("task_flow", dmt.Task.TaskFlow),
						zap.Any("upstream", upstreamProcessor),
						zap.String("cost", time.Now().Sub(upProcTime).String()))

					// oracle baseline, mysql not configure task and not configure rules
					downProcTime := time.Now()
					downstreamProcessor, err := database.IStructCompareProcessor(&MySQLProcessor{
						Ctx:                      dmt.Ctx,
						TaskName:                 dmt.Task.TaskName,
						TaskFlow:                 dmt.Task.TaskFlow,
						SchemaName:               dt.SchemaNameT,
						TableName:                dt.TableNameT,
						DBCharset:                dmt.DBCharsetT,
						Database:                 dmt.DatabaseT,
						BuildinDatatypeRules:     dmt.BuildInDatatypeRulesT,
						BuildinDefaultValueRules: dmt.BuildInDefaultValueRulesT,
						ColumnRouteRules:         make(map[string]string),
						IsBaseline:               false,
					}, dmt.TaskParams.IgnoreCaseCompare)
					if err != nil {
						return fmt.Errorf("the struct compare processor database [%s] failed: %v", dmt.DBTypeT, err)
					}

					logger.Info("struct compare task processor",
						zap.String("task_name", dmt.Task.TaskName),
						zap.String("task_mode", dmt.Task.TaskMode),
						zap.String("task_flow", dmt.Task.TaskFlow),
						zap.Any("downstream", downstreamProcessor),
						zap.String("cost", time.Now().Sub(downProcTime).String()))

					compareDetail, err := database.IStructCompareTable(&Table{
						TaskName: dmt.Task.TaskName,
						TaskFlow: dmt.Task.TaskFlow,
						Source:   upstreamProcessor,
						Target:   downstreamProcessor,
					})
					if err != nil {
						return fmt.Errorf("the struct compare table processor failed: %v", err)
					}
					if strings.EqualFold(compareDetail, "") {
						errW = model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
							_, err = model.GetIStructCompareTaskRW().UpdateStructCompareTask(txnCtx,
								&task.StructCompareTask{TaskName: dt.TaskName, SchemaNameS: dt.SchemaNameS, TableNameS: dt.TableNameS},
								map[string]interface{}{
									"TaskStatus": constant.TaskDatabaseStatusEqual,
									"Duration":   fmt.Sprintf("%f", time.Now().Sub(gTime).Seconds()),
								})
							if err != nil {
								return err
							}
							_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
								TaskName:    dt.TaskName,
								SchemaNameS: dt.SchemaNameS,
								TableNameS:  dt.TableNameS,
								LogDetail: fmt.Sprintf("%v [%v] struct compare task [%v] taskflow [%v] source table [%v.%v] compare equal, please see [struct_compare_task] detail",
									stringutil.CurrentTimeFormatString(),
									stringutil.StringLower(constant.TaskModeStructCompare),
									dt.TaskName,
									dmt.Task.TaskMode,
									dt.SchemaNameS,
									dt.TableNameS),
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

					originStructS, err := dmt.DatabaseS.GetDatabaseTableOriginStruct(dt.SchemaNameS, dt.TableNameS, "TABLE")
					if err != nil {
						return fmt.Errorf("the struct compare table get source origin struct failed: %v", err)
					}
					originStructT, err := dmt.DatabaseT.GetDatabaseTableOriginStruct(dt.SchemaNameT, dt.TableNameT, "")
					if err != nil {
						return fmt.Errorf("the struct compare table get target origin struct failed: %v", err)
					}

					encOriginS := snappy.Encode(nil, []byte(originStructS))
					encryptOriginS, err := stringutil.Encrypt(stringutil.BytesToString(encOriginS), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					encOriginT := snappy.Encode(nil, []byte(originStructT))
					encryptOriginT, err := stringutil.Encrypt(stringutil.BytesToString(encOriginT), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					encCompareDetail := snappy.Encode(nil, []byte(compareDetail))
					encryptCompareDetail, err := stringutil.Encrypt(stringutil.BytesToString(encCompareDetail), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					errW = model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
						_, err = model.GetIStructCompareTaskRW().UpdateStructCompareTask(txnCtx,
							&task.StructCompareTask{TaskName: dt.TaskName, SchemaNameS: dt.SchemaNameS, TableNameS: dt.TableNameS},
							map[string]interface{}{
								"TaskStatus":      constant.TaskDatabaseStatusNotEqual,
								"SourceSqlDigest": encryptOriginS,
								"TargetSqlDigest": encryptOriginT,
								"CompareDetail":   encryptCompareDetail,
								"Duration":        fmt.Sprintf("%f", time.Now().Sub(gTime).Seconds()),
							})
						if err != nil {
							return err
						}
						_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
							TaskName:    dt.TaskName,
							SchemaNameS: dt.SchemaNameS,
							TableNameS:  dt.TableNameS,
							LogDetail: fmt.Sprintf("%v [%v] struct compare task [%v] taskflow [%v] source table [%v.%v] compare equal, please see [struct_compare_task] detail",
								stringutil.CurrentTimeFormatString(),
								stringutil.StringLower(dmt.Task.TaskMode),
								dt.TaskName,
								dmt.Task.TaskFlow,
								dt.SchemaNameS,
								dt.TableNameS),
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
				default:
					return fmt.Errorf("oracle current task [%s] schema [%s] taskflow [%s] column rule isn't support, please contact author", dmt.Task.TaskName, dt.SchemaNameS, dmt.Task.TaskFlow)
				}
			})
		}
		g.Wait()
	}()

	for res := range g.ResultC {
		if res.Error != nil {
			smt := res.Task.(*task.StructCompareTask)
			logger.Error("struct compare task process tables",
				zap.String("task_name", dmt.Task.TaskName),
				zap.String("task_mode", dmt.Task.TaskMode),
				zap.String("task_flow", dmt.Task.TaskFlow),
				zap.String("schema_name_s", smt.SchemaNameS),
				zap.String("table_name_s", smt.TableNameS),
				zap.Error(res.Error))

			if err := thread.Retry(
				&thread.RetryConfig{
					MaxRetries: thread.DefaultThreadErrorMaxRetries,
					Delay:      thread.DefaultThreadErrorRereyDelay,
				},
				func(err error) bool {
					return true
				},
				func() error {
					errW := model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
						_, err = model.GetIStructCompareTaskRW().UpdateStructCompareTask(txnCtx,
							&task.StructCompareTask{TaskName: smt.TaskName, SchemaNameS: smt.SchemaNameS, TableNameS: smt.TableNameS},
							map[string]interface{}{
								"TaskStatus":  constant.TaskDatabaseStatusFailed,
								"Duration":    res.Duration,
								"ErrorDetail": res.Error.Error(),
							})
						if err != nil {
							return err
						}
						_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
							TaskName:    smt.TaskName,
							SchemaNameS: smt.SchemaNameS,
							TableNameS:  smt.TableNameS,
							LogDetail: fmt.Sprintf("%v [%v] struct compare task [%v] taskflow [%v] source table [%v.%v] failed, please see [struct_compare_task] detail",
								stringutil.CurrentTimeFormatString(),
								stringutil.StringLower(dmt.Task.TaskMode),
								smt.TaskName,
								dmt.Task.TaskFlow,
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
				}); err != nil {
				return err
			}
		}
	}

	err = model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
		tableStatusRecs, err := model.GetIStructCompareTaskRW().FindStructCompareTaskGroupByTaskStatus(txnCtx, dmt.Task.TaskName)
		if err != nil {
			return err
		}
		for _, rec := range tableStatusRecs {
			switch rec.TaskStatus {
			case constant.TaskDatabaseStatusEqual:
				_, err = model.GetIStructCompareSummaryRW().UpdateStructCompareSummary(txnCtx, &task.StructCompareSummary{
					TaskName:    rec.TaskName,
					SchemaNameS: dmt.SchemaNameS,
				}, map[string]interface{}{
					"TableEquals": rec.StatusCounts,
				})
				if err != nil {
					return err
				}
			case constant.TaskDatabaseStatusNotEqual:
				_, err = model.GetIStructCompareSummaryRW().UpdateStructCompareSummary(txnCtx, &task.StructCompareSummary{
					TaskName:    rec.TaskName,
					SchemaNameS: dmt.SchemaNameS,
				}, map[string]interface{}{
					"TableNotEquals": rec.StatusCounts,
				})
				if err != nil {
					return err
				}
			case constant.TaskDatabaseStatusFailed:
				_, err = model.GetIStructCompareSummaryRW().UpdateStructCompareSummary(txnCtx, &task.StructCompareSummary{
					TaskName:    rec.TaskName,
					SchemaNameS: dmt.SchemaNameS,
				}, map[string]interface{}{
					"TableFails": rec.StatusCounts,
				})
				if err != nil {
					return err
				}
			case constant.TaskDatabaseStatusWaiting:
				_, err = model.GetIStructCompareSummaryRW().UpdateStructCompareSummary(txnCtx, &task.StructCompareSummary{
					TaskName:    rec.TaskName,
					SchemaNameS: dmt.SchemaNameS,
				}, map[string]interface{}{
					"TableWaits": rec.StatusCounts,
				})
				if err != nil {
					return err
				}
			case constant.TaskDatabaseStatusRunning:
				_, err = model.GetIStructCompareSummaryRW().UpdateStructCompareSummary(txnCtx, &task.StructCompareSummary{
					TaskName:    rec.TaskName,
					SchemaNameS: dmt.SchemaNameS,
				}, map[string]interface{}{
					"TableRuns": rec.StatusCounts,
				})
				if err != nil {
					return err
				}
			case constant.TaskDatabaseStatusStopped:
				_, err = model.GetIStructCompareSummaryRW().UpdateStructCompareSummary(txnCtx, &task.StructCompareSummary{
					TaskName:    rec.TaskName,
					SchemaNameS: dmt.SchemaNameS,
				}, map[string]interface{}{
					"TableStops": rec.StatusCounts,
				})
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("the task [%v] task_mode [%s] task_flow [%v] schema_name_s [%v] task_status [%v] panic, please contact auhtor or reselect", dmt.Task.TaskName, dmt.Task.TaskMode, dmt.Task.TaskFlow, dmt.SchemaNameS, rec.TaskStatus)
			}
		}

		_, err = model.GetIStructCompareSummaryRW().UpdateStructCompareSummary(txnCtx,
			&task.StructCompareSummary{
				TaskName:    dmt.Task.TaskName,
				SchemaNameS: dmt.SchemaNameS},
			map[string]interface{}{
				"CompareFlag": constant.TaskCompareStatusFinished,
				"Duration":    fmt.Sprintf("%f", time.Now().Sub(dmt.StartTime).Seconds()),
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
