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
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/buildin"
	"github.com/wentaojin/dbms/model/datasource"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
)

type AssessMigrateTask struct {
	Ctx         context.Context
	Task        *task.Task
	DatasourceS *datasource.Datasource
	DatasourceT *datasource.Datasource
	TaskParams  *pb.AssessMigrateParam
	DBTypeS     string
	DBTypeT     string
}

func (amt *AssessMigrateTask) Start() error {
	schemaTaskTime := time.Now()
	logger.Info("assess migrate task init database connection",
		zap.String("task_name", amt.Task.TaskName),
		zap.String("task_mode", amt.Task.TaskMode), zap.String("task_flow", amt.Task.TaskFlow))

	sourceDatasource, err := model.GetIDatasourceRW().GetDatasource(amt.Ctx, amt.Task.DatasourceNameS)
	if err != nil {
		return err
	}
	databaseS, err := database.NewDatabase(amt.Ctx, sourceDatasource, amt.TaskParams.SchemaNameS, int64(amt.TaskParams.CallTimeout))
	if err != nil {
		return err
	}
	defer databaseS.Close()

	logger.Info("assess migrate task init task",
		zap.String("task_name", amt.Task.TaskName), zap.String("task_mode", amt.Task.TaskMode), zap.String("task_flow", amt.Task.TaskFlow))
	_, err = model.GetITaskLogRW().CreateLog(amt.Ctx, &task.Log{
		TaskName:    amt.Task.TaskName,
		SchemaNameS: amt.TaskParams.SchemaNameS,
		LogDetail: fmt.Sprintf("%v [%v] assess migrate task [%v] taskflow [%v] schema_name_s [%v] init",
			stringutil.CurrentTimeFormatString(),
			stringutil.StringLower(constant.TaskModeAssessMigrate),
			amt.Task.TaskName,
			amt.Task.TaskMode,
			amt.TaskParams.SchemaNameS),
	})
	if err != nil {
		return err
	}
	err = amt.InitAssessMigrateTask()
	if err != nil {
		return err
	}

	dbTypeSli := stringutil.StringSplit(amt.Task.TaskFlow, constant.StringSeparatorAite)
	amt.DBTypeS = dbTypeSli[0]
	amt.DBTypeT = dbTypeSli[1]

	logger.Info("assess migrate task get task",
		zap.String("task_name", amt.Task.TaskName), zap.String("task_mode", amt.Task.TaskMode), zap.String("task_flow", amt.Task.TaskFlow))
	migrateTasks, err := model.GetIAssessMigrateTaskRW().GetAssessMigrateTask(amt.Ctx, &task.AssessMigrateTask{TaskName: amt.Task.TaskName, SchemaNameS: amt.TaskParams.SchemaNameS})
	if err != nil {
		return err
	}

	if strings.EqualFold(migrateTasks[0].TaskStatus, constant.TaskDatabaseStatusSuccess) {
		logger.Info("assess migrate task",
			zap.String("task_name", amt.Task.TaskName),
			zap.String("task_mode", amt.Task.TaskMode),
			zap.String("task_flow", amt.Task.TaskFlow),
			zap.String("cost", time.Now().Sub(schemaTaskTime).String()))
		_, err = model.GetITaskLogRW().CreateLog(amt.Ctx, &task.Log{
			TaskName:    amt.Task.TaskName,
			SchemaNameS: amt.TaskParams.SchemaNameS,
			LogDetail: fmt.Sprintf("%v [%v] assess migrate task [%v] taskflow [%v] schema_name_s [%v] successfully run, skip running, you can use assess gen command get the report",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeAssessMigrate),
				amt.Task.TaskName,
				amt.Task.TaskMode,
				amt.TaskParams.SchemaNameS),
		})
		if err != nil {
			return err
		}
		return nil
	}

	err = model.Transaction(amt.Ctx, func(txnCtx context.Context) error {
		_, err = model.GetIAssessMigrateTaskRW().UpdateAssessMigrateTask(amt.Ctx, &task.AssessMigrateTask{
			TaskName:    amt.Task.TaskName,
			SchemaNameS: amt.TaskParams.SchemaNameS,
		}, map[string]interface{}{
			"TaskStatus": constant.TaskDatabaseStatusRunning,
		})
		if err != nil {
			return err
		}
		_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
			TaskName:    amt.Task.TaskName,
			SchemaNameS: amt.TaskParams.SchemaNameS,
			LogDetail: fmt.Sprintf("%v [%v] assess migrate task [%v] taskflow [%v] schema_name_s [%v] assess start",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeAssessMigrate),
				amt.Task.TaskName,
				amt.Task.TaskMode,
				amt.TaskParams.SchemaNameS),
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	_, err = model.GetITaskLogRW().CreateLog(amt.Ctx, &task.Log{
		TaskName:    amt.Task.TaskName,
		SchemaNameS: amt.TaskParams.SchemaNameS,
		LogDetail: fmt.Sprintf("%v [%v] assess migrate task [%v] taskflow [%v] schema_name_s [%v] query buildin rule",
			stringutil.CurrentTimeFormatString(),
			stringutil.StringLower(constant.TaskModeAssessMigrate),
			amt.Task.TaskName,
			amt.Task.TaskMode,
			amt.TaskParams.SchemaNameS),
	})
	if err != nil {
		return err
	}
	compatibles, err := model.GetBuildInCompatibleRuleRW().QueryBuildInCompatibleRule(amt.Ctx, amt.DBTypeS, amt.DBTypeT)
	if err != nil {
		return err
	}
	objAssessCompsMap := make(map[string]buildin.BuildinCompatibleRule)
	for _, c := range compatibles {
		objAssessCompsMap[stringutil.StringUpper(c.ObjectNameS)] = buildin.BuildinCompatibleRule{
			ID:            c.ID,
			DBTypeS:       c.DBTypeS,
			DBTypeT:       c.DBTypeT,
			ObjectNameS:   c.ObjectNameS,
			IsCompatible:  c.IsCompatible,
			IsConvertible: c.IsConvertible,
			Entity:        c.Entity,
		}
	}

	buildDatatypeRules, err := model.GetIBuildInDatatypeRuleRW().QueryBuildInDatatypeRule(amt.Ctx, amt.DBTypeS, amt.DBTypeT)
	if err != nil {
		return err
	}
	buildDatatypeMap := make(map[string]buildin.BuildinDatatypeRule)
	for _, d := range buildDatatypeRules {
		buildDatatypeMap[stringutil.StringUpper(d.DatatypeNameS)] = buildin.BuildinDatatypeRule{
			ID:            d.ID,
			DBTypeS:       d.DBTypeS,
			DBTypeT:       d.DBTypeT,
			DatatypeNameS: d.DatatypeNameS,
			DatatypeNameT: d.DatatypeNameT,
			Entity:        d.Entity,
		}
	}

	defaultValues, err := model.GetBuildInDefaultValueRuleRW().QueryBuildInDefaultValueRule(amt.Ctx, amt.DBTypeS, amt.DBTypeT)
	if err != nil {
		return err
	}
	defaultValuesMap := make(map[string]buildin.BuildinDefaultvalRule)
	for _, d := range defaultValues {
		defaultValuesMap[stringutil.StringUpper(d.DefaultValueS)] = buildin.BuildinDefaultvalRule{
			ID:            d.ID,
			DBTypeS:       d.DBTypeS,
			DBTypeT:       d.DBTypeT,
			DefaultValueS: d.DefaultValueS,
			DefaultValueT: d.DefaultValueT,
			Entity:        d.Entity,
		}
	}

	logger.Info("assess migrate task process schema",
		zap.String("task_name", amt.Task.TaskName),
		zap.String("task_mode", amt.Task.TaskMode),
		zap.String("task_flow", amt.Task.TaskFlow))
	var (
		usernames    []string
		usernamesNew []string
	)
	if strings.EqualFold(migrateTasks[0].SchemaNameS, "") {
		usernames, err = databaseS.GetDatabaseSchemaNameALL()
		if err != nil {
			return err
		}
	} else {
		username, err := databaseS.GetDatabaseSchemaNameSingle(migrateTasks[0].SchemaNameS)
		if err != nil {
			return err
		}
		usernames = append(usernames, username)
	}

	if len(usernames) == 0 {
		return fmt.Errorf("the oracle schema are not exist, please reselect")
	}

	logger.Info("assess migrate task gather schema",
		zap.String("task_name", amt.Task.TaskName),
		zap.String("task_mode", amt.Task.TaskMode),
		zap.String("task_flow", amt.Task.TaskFlow))

	for _, u := range usernames {
		usernamesNew = append(usernamesNew, fmt.Sprintf("'%s'", u))
	}

	logger.Info("assess migrate task assess schema",
		zap.String("task_name", amt.Task.TaskName),
		zap.String("task_mode", amt.Task.TaskMode),
		zap.String("task_flow", amt.Task.TaskFlow),
		zap.String("startTime", time.Now().String()))
	assessTotal := 0
	compatibleS := 0
	incompatibleS := 0
	convertibleS := 0
	inconvertibleS := 0
	logger.Info("assess migrate task assess schema overview",
		zap.String("task_name", amt.Task.TaskName),
		zap.String("task_mode", amt.Task.TaskMode),
		zap.String("task_flow", amt.Task.TaskFlow))
	_, err = model.GetITaskLogRW().CreateLog(amt.Ctx, &task.Log{
		TaskName:    amt.Task.TaskName,
		SchemaNameS: amt.TaskParams.SchemaNameS,
		LogDetail: fmt.Sprintf("%v [%v] assess migrate task [%v] taskflow [%v] schema_name_s [%v] query overview result",
			stringutil.CurrentTimeFormatString(),
			stringutil.StringLower(constant.TaskModeAssessMigrate),
			amt.Task.TaskName,
			amt.Task.TaskMode,
			amt.TaskParams.SchemaNameS),
	})
	if err != nil {
		return err
	}
	dbOverview, overviewS, err := GetAssessDatabaseOverviewResult(databaseS, objAssessCompsMap, migrateTasks[0].AssessUser, migrateTasks[0].AssessFile)
	if err != nil {
		return err
	}
	assessTotal += overviewS.AssessTotal
	compatibleS += overviewS.Compatible
	incompatibleS += overviewS.Incompatible
	convertibleS += overviewS.Convertible
	inconvertibleS += overviewS.InConvertible

	logger.Info("assess migrate task assess schema compatible",
		zap.String("task_name", amt.Task.TaskName),
		zap.String("task_mode", amt.Task.TaskMode),
		zap.String("task_flow", amt.Task.TaskFlow))
	_, err = model.GetITaskLogRW().CreateLog(amt.Ctx, &task.Log{
		TaskName:    amt.Task.TaskName,
		SchemaNameS: amt.TaskParams.SchemaNameS,
		LogDetail: fmt.Sprintf("%v [%v] assess migrate task [%v] taskflow [%v] schema_name_s [%v] query compatible result",
			stringutil.CurrentTimeFormatString(),
			stringutil.StringLower(constant.TaskModeAssessMigrate),
			amt.Task.TaskName,
			amt.Task.TaskMode,
			amt.TaskParams.SchemaNameS),
	})
	if err != nil {
		return err
	}
	dbCompatibles, compS, err := GetAssessDatabaseCompatibleResult(databaseS, usernamesNew, objAssessCompsMap, buildDatatypeMap, defaultValuesMap)
	if err != nil {
		return err
	}
	assessTotal += compS.AssessTotal
	compatibleS += compS.Compatible
	incompatibleS += compS.Incompatible
	convertibleS += compS.Convertible
	inconvertibleS += compS.InConvertible

	logger.Info("assess migrate task assess schema check",
		zap.String("task_name", amt.Task.TaskName),
		zap.String("task_mode", amt.Task.TaskMode),
		zap.String("task_flow", amt.Task.TaskFlow))
	_, err = model.GetITaskLogRW().CreateLog(amt.Ctx, &task.Log{
		TaskName:    amt.Task.TaskName,
		SchemaNameS: amt.TaskParams.SchemaNameS,
		LogDetail: fmt.Sprintf("%v [%v] assess migrate task [%v] taskflow [%v] schema_name_s [%v] query check result",
			stringutil.CurrentTimeFormatString(),
			stringutil.StringLower(constant.TaskModeAssessMigrate),
			amt.Task.TaskName,
			amt.Task.TaskMode,
			amt.TaskParams.SchemaNameS),
	})
	if err != nil {
		return err
	}
	dbChecks, checkS, err := GetAssessDatabaseCheckResult(databaseS, usernamesNew)
	if err != nil {
		return err
	}
	assessTotal += checkS.AssessTotal
	compatibleS += checkS.Compatible
	incompatibleS += checkS.Incompatible
	convertibleS += checkS.Convertible
	inconvertibleS += checkS.InConvertible

	logger.Info("assess migrate task assess schema related",
		zap.String("task_name", amt.Task.TaskName),
		zap.String("task_mode", amt.Task.TaskMode),
		zap.String("task_flow", amt.Task.TaskFlow))
	_, err = model.GetITaskLogRW().CreateLog(amt.Ctx, &task.Log{
		TaskName:    amt.Task.TaskName,
		SchemaNameS: amt.TaskParams.SchemaNameS,
		LogDetail: fmt.Sprintf("%v [%v] assess migrate task [%v] taskflow [%v] schema_name_s [%v] query related result",
			stringutil.CurrentTimeFormatString(),
			stringutil.StringLower(constant.TaskModeAssessMigrate),
			amt.Task.TaskName,
			amt.Task.TaskMode,
			amt.TaskParams.SchemaNameS),
	})
	if err != nil {
		return err
	}
	dbRelated, relatedS, err := GetAssessDatabaseRelatedResult(databaseS, usernamesNew)
	if err != nil {
		return err
	}
	assessTotal += relatedS.AssessTotal
	compatibleS += relatedS.Compatible
	incompatibleS += relatedS.Incompatible
	convertibleS += relatedS.Convertible
	inconvertibleS += relatedS.InConvertible

	rep := &Report{
		ReportOverview: dbOverview,
		ReportSummary: &ReportSummary{
			AssessTotal:   assessTotal,
			Compatible:    compatibleS,
			Incompatible:  incompatibleS,
			Convertible:   convertibleS,
			InConvertible: inconvertibleS,
		},
		ReportCompatible: dbCompatibles,
		ReportCheck:      dbChecks,
		ReportRelated:    dbRelated,
	}

	endTime := time.Now()
	err = model.Transaction(amt.Ctx, func(txnCtx context.Context) error {
		encryptDetails, err := stringutil.Encrypt(rep.String(), []byte(constant.DefaultDataEncryptDecryptKey))
		if err != nil {
			return err
		}
		_, err = model.GetIAssessMigrateTaskRW().UpdateAssessMigrateTask(amt.Ctx, &task.AssessMigrateTask{
			TaskName:    amt.Task.TaskName,
			SchemaNameS: amt.TaskParams.SchemaNameS,
		}, map[string]interface{}{
			"TaskStatus":   constant.TaskDatabaseStatusSuccess,
			"AssessDetail": encryptDetails,
			"Duration":     fmt.Sprintf("%f", endTime.Sub(schemaTaskTime).Seconds()),
		})
		if err != nil {
			return err
		}
		_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
			TaskName:    amt.Task.TaskName,
			SchemaNameS: amt.TaskParams.SchemaNameS,
			LogDetail: fmt.Sprintf("%v [%v] assess migrate task [%v] taskflow [%v] schema_name_s [%v] assess success",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeAssessMigrate),
				amt.Task.TaskName,
				amt.Task.TaskMode,
				amt.TaskParams.SchemaNameS),
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	logger.Info("assess migrate task",
		zap.String("task_name", amt.Task.TaskName),
		zap.String("task_mode", amt.Task.TaskMode),
		zap.String("task_flow", amt.Task.TaskFlow),
		zap.String("cost", endTime.Sub(schemaTaskTime).String()))
	return nil
}

func (amt *AssessMigrateTask) InitAssessMigrateTask() error {
	migrateTask, err := model.GetIAssessMigrateTaskRW().GetAssessMigrateTask(amt.Ctx, &task.AssessMigrateTask{TaskName: amt.Task.TaskName, SchemaNameS: amt.TaskParams.SchemaNameS})
	if err != nil {
		return err
	}
	// init database table
	logger.Info("assess migrate task init",
		zap.String("task_name", amt.Task.TaskName), zap.String("task_mode", amt.Task.TaskMode), zap.String("task_flow", amt.Task.TaskFlow))
	if len(migrateTask) == 0 {
		_, err = model.GetIAssessMigrateTaskRW().CreateAssessMigrateTask(amt.Ctx, &task.AssessMigrateTask{
			TaskName:    amt.Task.TaskName,
			SchemaNameS: amt.TaskParams.SchemaNameS,
			AssessUser:  amt.DatasourceS.Username,
			AssessFile:  fmt.Sprintf("report_%s.html", amt.Task.TaskName),
			TaskStatus:  constant.TaskDatabaseStatusWaiting,
		})
		if err != nil {
			return err
		}
		_, err = model.GetITaskRW().UpdateTask(amt.Ctx, &task.Task{TaskName: amt.Task.TaskName}, map[string]interface{}{"TaskInit": constant.TaskInitStatusFinished})
		if err != nil {
			return err
		}
	}
	return nil
}

func GetAssessDatabaseOverviewResult(databaseS database.IDatabase, objAssessCompsMap map[string]buildin.BuildinCompatibleRule, reportUser, reportName string) (*ReportOverview, ReportSummary, error) {
	overview, rs, err := AssessDatabaseOverview(databaseS, objAssessCompsMap, reportName, reportUser)
	if err != nil {
		return nil, ReportSummary{}, err
	}
	return overview, rs, nil
}

func GetAssessDatabaseCompatibleResult(databaseS database.IDatabase, schemaNames []string, objAssessCompsMap map[string]buildin.BuildinCompatibleRule, buildDatatypeMap map[string]buildin.BuildinDatatypeRule, defaultValuesMap map[string]buildin.BuildinDefaultvalRule) (*ReportCompatible, *ReportSummary, error) {
	var (
		ListSchemaTableTypeCompatibles          []SchemaTableTypeCompatibles
		ListSchemaColumnTypeCompatibles         []SchemaColumnTypeCompatibles
		ListSchemaConstraintTypeCompatibles     []SchemaConstraintTypeCompatibles
		ListSchemaIndexTypeCompatibles          []SchemaIndexTypeCompatibles
		ListSchemaDefaultValueCompatibles       []SchemaDefaultValueCompatibles
		ListSchemaViewTypeCompatibles           []SchemaViewTypeCompatibles
		ListSchemaObjectTypeCompatibles         []SchemaObjectTypeCompatibles
		ListSchemaPartitionTypeCompatibles      []SchemaPartitionTypeCompatibles
		ListSchemaSubPartitionTypeCompatibles   []SchemaSubPartitionTypeCompatibles
		ListSchemaTemporaryTableTypeCompatibles []SchemaTemporaryTableTypeCompatibles
	)

	assessTotal := 0
	compatibleS := 0
	incompatibleS := 0
	convertibleS := 0
	inconvertibleS := 0

	ListSchemaTableTypeCompatibles, tableSummary, err := AssessDatabaseSchemaTableTypeCompatible(databaseS, schemaNames, objAssessCompsMap)
	if err != nil {
		return nil, nil, err
	}

	assessTotal += tableSummary.AssessTotal
	compatibleS += tableSummary.Compatible
	incompatibleS += tableSummary.Incompatible
	convertibleS += tableSummary.Convertible
	inconvertibleS += tableSummary.InConvertible

	ListSchemaColumnTypeCompatibles, columnSummary, err := AssessDatabaseSchemaColumnTypeCompatible(databaseS, schemaNames, buildDatatypeMap)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += columnSummary.AssessTotal
	compatibleS += columnSummary.Compatible
	incompatibleS += columnSummary.Incompatible
	convertibleS += columnSummary.Convertible
	inconvertibleS += columnSummary.InConvertible

	ListSchemaConstraintTypeCompatibles, constraintSummary, err := AssessDatabaseSchemaConstraintTypeCompatible(databaseS, schemaNames, objAssessCompsMap)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += constraintSummary.AssessTotal
	compatibleS += constraintSummary.Compatible
	incompatibleS += constraintSummary.Incompatible
	convertibleS += constraintSummary.Convertible
	inconvertibleS += constraintSummary.InConvertible

	ListSchemaIndexTypeCompatibles, indexSummary, err := AssessDatabaseSchemaIndexTypeCompatible(databaseS, schemaNames, objAssessCompsMap)
	if err != nil {
		return nil, nil, err
	}

	assessTotal += indexSummary.AssessTotal
	compatibleS += indexSummary.Compatible
	incompatibleS += indexSummary.Incompatible
	convertibleS += indexSummary.Convertible
	inconvertibleS += indexSummary.InConvertible

	ListSchemaDefaultValueCompatibles, defaultValSummary, err := AssessDatabaseSchemaDefaultValue(databaseS, schemaNames, defaultValuesMap)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += defaultValSummary.AssessTotal
	compatibleS += defaultValSummary.Compatible
	incompatibleS += defaultValSummary.Incompatible
	convertibleS += defaultValSummary.Convertible
	inconvertibleS += defaultValSummary.InConvertible

	ListSchemaViewTypeCompatibles, viewSummary, err := AssessDatabaseSchemaViewTypeCompatible(databaseS, schemaNames, objAssessCompsMap)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += viewSummary.AssessTotal
	compatibleS += viewSummary.Compatible
	incompatibleS += viewSummary.Incompatible
	convertibleS += viewSummary.Convertible
	inconvertibleS += viewSummary.InConvertible

	ListSchemaObjectTypeCompatibles, codeSummary, err := AssessDatabaseSchemaObjectTypeCompatible(databaseS, schemaNames, objAssessCompsMap)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += codeSummary.AssessTotal
	compatibleS += codeSummary.Compatible
	incompatibleS += codeSummary.Incompatible
	convertibleS += codeSummary.Convertible
	inconvertibleS += codeSummary.InConvertible

	ListSchemaPartitionTypeCompatibles, partitionSummary, err := AssessDatabaseSchemaPartitionTypeCompatible(databaseS, schemaNames, objAssessCompsMap)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += partitionSummary.AssessTotal
	compatibleS += partitionSummary.Compatible
	incompatibleS += partitionSummary.Incompatible
	convertibleS += partitionSummary.Convertible
	inconvertibleS += partitionSummary.InConvertible

	ListSchemaSubPartitionTypeCompatibles, subPartitionSummary, err := AssessDatabaseSchemaSubPartitionTypeCompatible(databaseS, schemaNames, objAssessCompsMap)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += subPartitionSummary.AssessTotal
	compatibleS += subPartitionSummary.Compatible
	incompatibleS += subPartitionSummary.Incompatible
	convertibleS += subPartitionSummary.Convertible
	inconvertibleS += subPartitionSummary.InConvertible

	ListSchemaTemporaryTableTypeCompatibles, tempSummary, err := AssessDatabaseSchemaTemporaryTableCompatible(databaseS, schemaNames, objAssessCompsMap)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += tempSummary.AssessTotal
	compatibleS += tempSummary.Compatible
	incompatibleS += tempSummary.Incompatible
	convertibleS += tempSummary.Convertible
	inconvertibleS += tempSummary.InConvertible

	return &ReportCompatible{
			ListSchemaTableTypeCompatibles:          ListSchemaTableTypeCompatibles,
			ListSchemaColumnTypeCompatibles:         ListSchemaColumnTypeCompatibles,
			ListSchemaConstraintTypeCompatibles:     ListSchemaConstraintTypeCompatibles,
			ListSchemaIndexTypeCompatibles:          ListSchemaIndexTypeCompatibles,
			ListSchemaDefaultValueCompatibles:       ListSchemaDefaultValueCompatibles,
			ListSchemaViewTypeCompatibles:           ListSchemaViewTypeCompatibles,
			ListSchemaObjectTypeCompatibles:         ListSchemaObjectTypeCompatibles,
			ListSchemaPartitionTypeCompatibles:      ListSchemaPartitionTypeCompatibles,
			ListSchemaSubPartitionTypeCompatibles:   ListSchemaSubPartitionTypeCompatibles,
			ListSchemaTemporaryTableTypeCompatibles: ListSchemaTemporaryTableTypeCompatibles,
		}, &ReportSummary{
			AssessTotal:   assessTotal,
			Compatible:    compatibleS,
			Incompatible:  incompatibleS,
			Convertible:   convertibleS,
			InConvertible: inconvertibleS,
		}, nil
}

func GetAssessDatabaseRelatedResult(databaseS database.IDatabase, schemaNames []string) (*ReportRelated, *ReportSummary, error) {
	var (
		ListSchemaActiveSession          []SchemaActiveSession
		ListSchemaTableSizeData          []SchemaTableSizeData
		ListSchemaTableRowsTOP           []SchemaTableRowsTOP
		ListSchemaCodeObject             []SchemaCodeObject
		ListSchemaSynonymObject          []SchemaSynonymObject
		ListSchemaMaterializedViewObject []SchemaMaterializedViewObject
		ListSchemaTableAvgRowLengthTOP   []SchemaTableAvgRowLengthTOP
		ListSchemaTableNumberTypeEqual0  []SchemaTableNumberTypeEqualZero
	)

	assessTotal := 0
	compatibleS := 0
	incompatibleS := 0
	convertibleS := 0
	inconvertibleS := 0

	ListSchemaActiveSession, sessionSummary, err := AssessDatabaseMaxActiveSessionCount(databaseS, schemaNames)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += sessionSummary.AssessTotal
	compatibleS += sessionSummary.Compatible
	incompatibleS += sessionSummary.Incompatible
	convertibleS += sessionSummary.Convertible
	inconvertibleS += sessionSummary.InConvertible

	ListSchemaTableSizeData, overviewSummary, err := AssessDatabaseSchemaOverview(databaseS, schemaNames)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += overviewSummary.AssessTotal
	compatibleS += overviewSummary.Compatible
	incompatibleS += overviewSummary.Incompatible
	convertibleS += overviewSummary.Convertible
	inconvertibleS += overviewSummary.InConvertible

	ListSchemaTableRowsTOP, tableSummary, err := AssessDatabaseSchemaTableRowsTOP(databaseS, schemaNames, 10)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += tableSummary.AssessTotal
	compatibleS += tableSummary.Compatible
	incompatibleS += tableSummary.Incompatible
	convertibleS += tableSummary.Convertible
	inconvertibleS += tableSummary.InConvertible

	ListSchemaCodeObject, objSummary, err := AssessDatabaseSchemaCodeOverview(databaseS, schemaNames)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += objSummary.AssessTotal
	compatibleS += objSummary.Compatible
	incompatibleS += objSummary.Incompatible
	convertibleS += objSummary.Convertible
	inconvertibleS += objSummary.InConvertible

	ListSchemaSynonymObject, seqSummary, err := AssessDatabaseSchemaSynonymOverview(databaseS, schemaNames)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += seqSummary.AssessTotal
	compatibleS += seqSummary.Compatible
	incompatibleS += seqSummary.Incompatible
	convertibleS += seqSummary.Convertible
	inconvertibleS += seqSummary.InConvertible

	ListSchemaMaterializedViewObject, mViewSummary, err := AssessDatabaseSchemaMaterializedViewOverview(databaseS, schemaNames)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += mViewSummary.AssessTotal
	compatibleS += mViewSummary.Compatible
	incompatibleS += mViewSummary.Incompatible
	convertibleS += mViewSummary.Convertible
	inconvertibleS += mViewSummary.InConvertible

	ListSchemaTableAvgRowLengthTOP, tableTSummary, err := AssessDatabaseSchemaTableAvgRowLengthTOP(databaseS, schemaNames, 10)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += tableTSummary.AssessTotal
	compatibleS += tableTSummary.Compatible
	incompatibleS += tableTSummary.Incompatible
	convertibleS += tableTSummary.Convertible
	inconvertibleS += tableTSummary.InConvertible

	ListSchemaTableNumberTypeEqual0, equalSummary, err := AssessDatabaseSchemaTableNumberTypeEqualZero(databaseS, schemaNames, "NUMBER")
	if err != nil {
		return nil, nil, err
	}
	assessTotal += equalSummary.AssessTotal
	compatibleS += equalSummary.Compatible
	incompatibleS += equalSummary.Incompatible
	convertibleS += equalSummary.Convertible
	inconvertibleS += equalSummary.InConvertible

	return &ReportRelated{
			ListSchemaActiveSession:            ListSchemaActiveSession,
			ListSchemaTableSizeData:            ListSchemaTableSizeData,
			ListSchemaTableRowsTOP:             ListSchemaTableRowsTOP,
			ListSchemaCodeObject:               ListSchemaCodeObject,
			ListSchemaSynonymObject:            ListSchemaSynonymObject,
			ListSchemaMaterializedViewObject:   ListSchemaMaterializedViewObject,
			ListSchemaTableAvgRowLengthTOP:     ListSchemaTableAvgRowLengthTOP,
			ListSchemaTableNumberTypeEqualZero: ListSchemaTableNumberTypeEqual0,
		}, &ReportSummary{
			AssessTotal:   assessTotal,
			Compatible:    compatibleS,
			Incompatible:  incompatibleS,
			Convertible:   convertibleS,
			InConvertible: inconvertibleS,
		}, nil
}

func GetAssessDatabaseCheckResult(databaseS database.IDatabase, schemaNames []string) (*ReportCheck, *ReportSummary, error) {
	var (
		ListSchemaPartitionTableCountsCheck  []SchemaPartitionTableCountsCheck
		ListSchemaTableRowLengthCheck        []SchemaTableRowLengthCheck
		ListSchemaTableIndexRowLengthCheck   []SchemaTableIndexRowLengthCheck
		ListSchemaTableColumnCountsCheck     []SchemaTableColumnCountsCheck
		ListSchemaIndexCountsCheck           []SchemaTableIndexCountsCheck
		ListUsernameLengthCheck              []UsernameLengthCheck
		ListSchemaTableNameLengthCheck       []SchemaTableNameLengthCheck
		ListSchemaTableColumnNameLengthCheck []SchemaTableColumnNameLengthCheck
		ListSchemaTableIndexNameLengthCheck  []SchemaTableIndexNameLengthCheck
		ListSchemaViewNameLengthCheck        []SchemaViewNameLengthCheck
		ListSchemaSequenceNameLengthCheck    []SchemaSequenceNameLengthCheck
	)

	assessTotal := 0
	compatibleS := 0
	incompatibleS := 0
	convertibleS := 0
	inconvertibleS := 0

	ListSchemaPartitionTableCountsCheck, partitionSummary, err := AssessDatabasePartitionTableCountsCheck(databaseS, schemaNames, 1024)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += partitionSummary.AssessTotal
	compatibleS += partitionSummary.Compatible
	incompatibleS += partitionSummary.Incompatible
	convertibleS += partitionSummary.Convertible
	inconvertibleS += partitionSummary.InConvertible

	ListSchemaTableRowLengthCheck, tableSummary, err := AssessDatabaseTableRowLengthMBCheck(databaseS, schemaNames, 6)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += tableSummary.AssessTotal
	compatibleS += tableSummary.Compatible
	incompatibleS += tableSummary.Incompatible
	convertibleS += tableSummary.Convertible
	inconvertibleS += tableSummary.InConvertible

	ListSchemaTableIndexRowLengthCheck, indexSummary, err := AssessDatabaseTableIndexRowLengthCheck(databaseS, schemaNames, 3072)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += indexSummary.AssessTotal
	compatibleS += indexSummary.Compatible
	incompatibleS += indexSummary.Incompatible
	convertibleS += indexSummary.Convertible
	inconvertibleS += indexSummary.InConvertible

	ListSchemaTableColumnCountsCheck, columnSummary, err := AssessDatabaseTableColumnCountsCheck(databaseS, schemaNames, 512)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += columnSummary.AssessTotal
	compatibleS += columnSummary.Compatible
	incompatibleS += columnSummary.Incompatible
	convertibleS += columnSummary.Convertible
	inconvertibleS += columnSummary.InConvertible

	ListSchemaIndexCountsCheck, indexCSummary, err := AssessDatabaseTableIndexCountsCheck(databaseS, schemaNames, 64)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += indexCSummary.AssessTotal
	compatibleS += indexCSummary.Compatible
	incompatibleS += indexCSummary.Incompatible
	convertibleS += indexCSummary.Convertible
	inconvertibleS += indexCSummary.InConvertible

	ListUsernameLengthCheck, usernameLSummary, err := AssessDatabaseUsernameLengthCheck(databaseS, schemaNames, 64)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += usernameLSummary.AssessTotal
	compatibleS += usernameLSummary.Compatible
	incompatibleS += usernameLSummary.Incompatible
	convertibleS += usernameLSummary.Convertible
	inconvertibleS += usernameLSummary.InConvertible

	ListSchemaTableNameLengthCheck, tableLSummary, err := AssessDatabaseTableNameLengthCheck(databaseS, schemaNames, 64)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += tableLSummary.AssessTotal
	compatibleS += tableLSummary.Compatible
	incompatibleS += tableLSummary.Incompatible
	convertibleS += tableLSummary.Convertible
	inconvertibleS += tableLSummary.InConvertible

	ListSchemaTableColumnNameLengthCheck, columnLSummary, err := AssessDatabaseColumnNameLengthCheck(databaseS, schemaNames, 64)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += columnLSummary.AssessTotal
	compatibleS += columnLSummary.Compatible
	incompatibleS += columnLSummary.Incompatible
	convertibleS += columnLSummary.Convertible
	inconvertibleS += columnLSummary.InConvertible

	ListSchemaTableIndexNameLengthCheck, indexLSummary, err := AssessDatabaseIndexNameLengthCheck(databaseS, schemaNames, 64)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += indexLSummary.AssessTotal
	compatibleS += indexLSummary.Compatible
	incompatibleS += indexLSummary.Incompatible
	convertibleS += indexLSummary.Convertible
	inconvertibleS += indexLSummary.InConvertible

	ListSchemaViewNameLengthCheck, viewSummary, err := AssessDatabaseViewNameLengthCheck(databaseS, schemaNames, 64)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += viewSummary.AssessTotal
	compatibleS += viewSummary.Compatible
	incompatibleS += viewSummary.Incompatible
	convertibleS += viewSummary.Convertible
	inconvertibleS += viewSummary.InConvertible

	ListSchemaSequenceNameLengthCheck, seqSummary, err := AssessDatabaseSequenceNameLengthCheck(databaseS, schemaNames, 64)
	if err != nil {
		return nil, nil, err
	}
	assessTotal += seqSummary.AssessTotal
	compatibleS += seqSummary.Compatible
	incompatibleS += seqSummary.Incompatible
	convertibleS += seqSummary.Convertible
	inconvertibleS += seqSummary.InConvertible

	return &ReportCheck{
			ListSchemaPartitionTableCountsCheck:  ListSchemaPartitionTableCountsCheck,
			ListSchemaTableRowLengthCheck:        ListSchemaTableRowLengthCheck,
			ListSchemaTableIndexRowLengthCheck:   ListSchemaTableIndexRowLengthCheck,
			ListSchemaTableColumnCountsCheck:     ListSchemaTableColumnCountsCheck,
			ListSchemaIndexCountsCheck:           ListSchemaIndexCountsCheck,
			ListUsernameLengthCheck:              ListUsernameLengthCheck,
			ListSchemaTableNameLengthCheck:       ListSchemaTableNameLengthCheck,
			ListSchemaTableColumnNameLengthCheck: ListSchemaTableColumnNameLengthCheck,
			ListSchemaTableIndexNameLengthCheck:  ListSchemaTableIndexNameLengthCheck,
			ListSchemaViewNameLengthCheck:        ListSchemaViewNameLengthCheck,
			ListSchemaSequenceNameLengthCheck:    ListSchemaSequenceNameLengthCheck,
		}, &ReportSummary{
			AssessTotal:   assessTotal,
			Compatible:    compatibleS,
			Incompatible:  incompatibleS,
			Convertible:   convertibleS,
			InConvertible: incompatibleS,
		}, nil
}
