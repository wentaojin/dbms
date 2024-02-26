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
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/wentaojin/dbms/model/common"

	"github.com/wentaojin/dbms/utils/etcdutil"

	clientv3 "go.etcd.io/etcd/client/v3"

	"go.uber.org/zap"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/logger"

	"github.com/wentaojin/dbms/database/oracle/taskflow"

	"github.com/wentaojin/dbms/model/datasource"
	"github.com/wentaojin/dbms/model/migrate"
	"github.com/wentaojin/dbms/model/rule"

	"github.com/wentaojin/dbms/model/params"

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/model/task"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/proto/pb"
)

func UpsertStructMigrateTask(ctx context.Context, req *pb.UpsertStructMigrateTaskRequest) (string, error) {
	taskInfo, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: req.TaskName})
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(taskInfo.TaskMode, constant.TaskModeStructMigrate) && !strings.EqualFold(taskInfo.TaskMode, "") {
		return "", fmt.Errorf("the task name [%s] has be existed in the task mode [%s], please rename the global unqiue task name", req.TaskName, taskInfo.TaskMode)
	}

	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		var (
			datasourceS *datasource.Datasource
			datasourceT *datasource.Datasource
			err         error
		)
		datasourceS, err = model.GetIDatasourceRW().GetDatasource(txnCtx, req.DatasourceNameS)
		if err != nil {
			return err
		}
		datasourceT, err = model.GetIDatasourceRW().GetDatasource(txnCtx, req.DatasourceNameT)
		if err != nil {
			return err
		}
		_, err = model.GetITaskRW().CreateTask(txnCtx, &task.Task{
			TaskName:        req.TaskName,
			TaskMode:        constant.TaskModeStructMigrate,
			TaskFlow:        stringutil.StringBuilder(stringutil.StringUpper(datasourceS.DbType), constant.StringSeparatorAite, stringutil.StringUpper(datasourceT.DbType)),
			DatasourceNameS: req.DatasourceNameS,
			DatasourceNameT: req.DatasourceNameT,
			CaseFieldRuleS:  req.CaseFieldRule.CaseFieldRuleS,
			CaseFieldRuleT:  req.CaseFieldRule.CaseFieldRuleT,
			TaskStatus:      constant.TaskDatabaseStatusWaiting,
			Entity:          &common.Entity{Comment: req.Comment},
		})
		if err != nil {
			return err
		}

		err = UpsertSchemaRouteRule(txnCtx, req.TaskName, req.DatasourceNameS, req.CaseFieldRule, req.SchemaRouteRule)
		if err != nil {
			return err
		}

		fieldInfos := stringutil.GetJSONTagFieldValue(req.StructMigrateParam)
		for jsonTag, fieldValue := range fieldInfos {
			_, err = model.GetIParamsRW().CreateTaskCustomParam(txnCtx, &params.TaskCustomParam{
				TaskName:   req.TaskName,
				TaskMode:   constant.TaskModeStructMigrate,
				ParamName:  jsonTag,
				ParamValue: fieldValue,
			})
			if err != nil {
				return err
			}
		}

		for _, r := range req.StructMigrateRule.TaskStructRules {
			if !strings.EqualFold(r.String(), "") {
				var (
					sourceColumnType string
					targetColumnType string
				)
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameOrigin) {
					sourceColumnType = r.ColumnTypeS
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameOrigin) {
					targetColumnType = r.ColumnTypeT
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameLower) {
					sourceColumnType = stringutil.StringLower(r.ColumnTypeS)
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameLower) {
					targetColumnType = stringutil.StringLower(r.ColumnTypeT)
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameUpper) {
					sourceColumnType = stringutil.StringUpper(r.ColumnTypeS)
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameUpper) {
					targetColumnType = stringutil.StringUpper(r.ColumnTypeT)
				}

				_, err = model.GetIStructMigrateTaskRuleRW().CreateTaskStructRule(txnCtx, &migrate.TaskStructRule{
					TaskName:      req.TaskName,
					ColumnTypeS:   sourceColumnType,
					ColumnTypeT:   targetColumnType,
					DefaultValueS: r.DefaultValueS,
					DefaultValueT: r.DefaultValueT,
				})
				if err != nil {
					return err
				}
			}
		}
		for _, r := range req.StructMigrateRule.SchemaStructRules {
			if !strings.EqualFold(r.String(), "") {
				var (
					sourceColumnType string
					sourceSchema     string
					targetColumnType string
				)
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameOrigin) {
					sourceSchema = r.SchemaNameS
					sourceColumnType = r.ColumnTypeS
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameOrigin) {
					targetColumnType = r.ColumnTypeT
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameLower) {
					sourceSchema = stringutil.StringLower(r.SchemaNameS)
					sourceColumnType = stringutil.StringLower(r.ColumnTypeS)
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameLower) {
					targetColumnType = stringutil.StringLower(r.ColumnTypeT)
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameUpper) {
					sourceSchema = stringutil.StringUpper(r.SchemaNameS)
					sourceColumnType = stringutil.StringUpper(r.ColumnTypeS)
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameUpper) {
					targetColumnType = stringutil.StringUpper(r.ColumnTypeT)
				}
				_, err = model.GetIStructMigrateSchemaRuleRW().CreateSchemaStructRule(txnCtx, &migrate.SchemaStructRule{
					TaskName:      req.TaskName,
					SchemaNameS:   sourceSchema,
					ColumnTypeS:   sourceColumnType,
					ColumnTypeT:   targetColumnType,
					DefaultValueS: r.DefaultValueS,
					DefaultValueT: r.DefaultValueT,
				})
				if err != nil {
					return err
				}
			}
		}
		for _, r := range req.StructMigrateRule.TableStructRules {
			if !strings.EqualFold(r.String(), "") {
				var (
					sourceColumnType string
					sourceSchema     string
					sourceTable      string
					targetColumnType string
				)
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameOrigin) {
					sourceSchema = r.SchemaNameS
					sourceTable = r.TableNameS
					sourceColumnType = r.ColumnTypeS
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameOrigin) {
					targetColumnType = r.ColumnTypeT
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameLower) {
					sourceSchema = stringutil.StringLower(r.SchemaNameS)
					sourceTable = stringutil.StringLower(r.TableNameS)
					sourceColumnType = stringutil.StringLower(r.ColumnTypeS)
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameLower) {
					targetColumnType = stringutil.StringLower(r.ColumnTypeT)
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameUpper) {
					sourceSchema = stringutil.StringUpper(r.SchemaNameS)
					sourceTable = stringutil.StringUpper(r.TableNameS)
					sourceColumnType = stringutil.StringUpper(r.ColumnTypeS)
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameUpper) {
					targetColumnType = stringutil.StringUpper(r.ColumnTypeT)
				}
				_, err = model.GetIStructMigrateTableRuleRW().CreateTableStructRule(txnCtx, &migrate.TableStructRule{
					TaskName:      req.TaskName,
					SchemaNameS:   sourceSchema,
					TableNameS:    sourceTable,
					ColumnTypeS:   sourceColumnType,
					ColumnTypeT:   targetColumnType,
					DefaultValueS: r.DefaultValueS,
					DefaultValueT: r.DefaultValueT,
				})
				if err != nil {
					return err
				}
			}
		}
		for _, r := range req.StructMigrateRule.ColumnStructRules {
			if !strings.EqualFold(r.String(), "") {
				var (
					sourceColumnType string
					sourceSchema     string
					sourceTable      string
					sourceColumn     string
					targetColumnType string
				)
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameOrigin) {
					sourceSchema = r.SchemaNameS
					sourceTable = r.TableNameS
					sourceColumn = r.ColumnNameS
					sourceColumnType = r.ColumnTypeS
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameOrigin) {
					targetColumnType = r.ColumnTypeT
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameLower) {
					sourceSchema = stringutil.StringLower(r.SchemaNameS)
					sourceTable = stringutil.StringLower(r.TableNameS)
					sourceColumn = stringutil.StringLower(r.ColumnNameS)
					sourceColumnType = stringutil.StringLower(r.ColumnTypeS)
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameLower) {
					targetColumnType = stringutil.StringLower(r.ColumnTypeT)
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameUpper) {
					sourceSchema = stringutil.StringUpper(r.SchemaNameS)
					sourceTable = stringutil.StringUpper(r.TableNameS)
					sourceColumn = stringutil.StringUpper(r.ColumnNameS)
					sourceColumnType = stringutil.StringUpper(r.ColumnTypeS)
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameUpper) {
					targetColumnType = stringutil.StringUpper(r.ColumnTypeT)
				}
				_, err = model.GetIStructMigrateColumnRuleRW().CreateColumnStructRule(txnCtx, &migrate.ColumnStructRule{
					TaskName:      req.TaskName,
					SchemaNameS:   sourceSchema,
					TableNameS:    sourceTable,
					ColumnNameS:   sourceColumn,
					ColumnTypeS:   sourceColumnType,
					ColumnTypeT:   targetColumnType,
					DefaultValueS: r.DefaultValueS,
					DefaultValueT: r.DefaultValueT,
				})
				if err != nil {
					return err
				}
			}
		}

		for _, r := range req.StructMigrateRule.TableAttrsRules {
			if !strings.EqualFold(r.String(), "") {
				var sourceSchema string

				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameOrigin) {
					sourceSchema = r.SchemaNameS
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameLower) {
					sourceSchema = stringutil.StringLower(r.SchemaNameS)
				}
				if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameUpper) {
					sourceSchema = stringutil.StringUpper(r.SchemaNameS)
				}
				for _, t := range r.TableNamesS {
					var sourceTable string

					if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameOrigin) {
						sourceTable = t
					}
					if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameLower) {
						sourceTable = stringutil.StringLower(t)
					}
					if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameUpper) {
						sourceTable = stringutil.StringUpper(t)
					}
					_, err = model.GetIStructMigrateTableAttrsRuleRW().CreateTableAttrsRule(txnCtx, &migrate.TableAttrsRule{
						TaskName:    req.TaskName,
						SchemaNameS: sourceSchema,
						TableNameS:  sourceTable,
						TableAttrT:  r.TableAttrsT,
					})
				}
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	jsonStr, err := stringutil.MarshalJSON(req)
	if err != nil {
		return jsonStr, err
	}
	return jsonStr, nil
}

func DeleteStructMigrateTask(ctx context.Context, req *pb.DeleteStructMigrateTaskRequest) (string, error) {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		err := model.GetITaskRW().DeleteTask(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = DeleteSchemaRouteRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIParamsRW().DeleteTaskCustomParam(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIMigrateSchemaRouteRW().DeleteSchemaRouteRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIMigrateTableRouteRW().DeleteTableRouteRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIMigrateColumnRouteRW().DeleteColumnRouteRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIStructMigrateTaskRuleRW().DeleteTaskStructRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIStructMigrateSchemaRuleRW().DeleteSchemaStructRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIStructMigrateTableRuleRW().DeleteTableStructRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIStructMigrateColumnRuleRW().DeleteColumnStructRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIStructMigrateTableAttrsRuleRW().DeleteTableAttrsRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIStructMigrateTaskRW().DeleteStructMigrateTaskName(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIStructMigrateSummaryRW().DeleteStructMigrateSummaryName(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	jsonStr, err := stringutil.MarshalJSON(req)
	if err != nil {
		return jsonStr, err
	}
	return jsonStr, nil
}

func ShowStructMigrateTask(ctx context.Context, req *pb.ShowStructMigrateTaskRequest) (string, error) {
	var (
		resp  *pb.UpsertStructMigrateTaskRequest
		param *pb.StructMigrateParam
		rules *pb.StructMigrateRule
	)

	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err := model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: req.TaskName})
		if err != nil {
			return err
		}

		paramsInfo, err := model.GetIParamsRW().QueryTaskCustomParam(txnCtx, &params.TaskCustomParam{
			TaskName: req.TaskName,
			TaskMode: constant.TaskModeStructMigrate,
		})
		if err != nil {
			return err
		}

		paramMap := make(map[string]string)
		for _, p := range paramsInfo {
			paramMap[p.ParamName] = p.ParamValue
		}

		migrateThread, err := strconv.Atoi(paramMap[constant.ParamNameStructMigrateMigrateThread])
		if err != nil {
			return err
		}

		createIfNotExist, err := strconv.ParseBool(paramMap[constant.ParamNameStructMigrateCreateIfNotExist])
		if err != nil {
			return err
		}
		enableDirectCreate, err := strconv.ParseBool(paramMap[constant.ParamNameStructMigrateEnableDirectCreate])
		if err != nil {
			return err
		}

		param = &pb.StructMigrateParam{
			MigrateThread:      uint64(migrateThread),
			CreateIfNotExist:   createIfNotExist,
			EnableDirectCreate: enableDirectCreate,
		}

		taskRules, err := model.GetIStructMigrateTaskRuleRW().QueryTaskStructRule(txnCtx, &migrate.TaskStructRule{TaskName: req.TaskName})
		if err != nil {
			return err
		}

		for _, d := range taskRules {
			rules.TaskStructRules = append(rules.TaskStructRules, &pb.TaskStructRule{
				ColumnTypeS:   d.ColumnTypeS,
				ColumnTypeT:   d.ColumnTypeT,
				DefaultValueS: d.DefaultValueS,
				DefaultValueT: d.DefaultValueT,
			})
		}

		schemaRules, err := model.GetIStructMigrateSchemaRuleRW().FindSchemaStructRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}

		for _, d := range schemaRules {
			rules.SchemaStructRules = append(rules.SchemaStructRules, &pb.SchemaStructRule{
				SchemaNameS:   d.SchemaNameS,
				ColumnTypeS:   d.ColumnTypeS,
				ColumnTypeT:   d.ColumnTypeT,
				DefaultValueS: d.DefaultValueS,
				DefaultValueT: d.DefaultValueT,
			})
		}

		tableRules, err := model.GetIStructMigrateTableRuleRW().FindTableStructRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}

		for _, d := range tableRules {
			rules.TableStructRules = append(rules.TableStructRules, &pb.TableStructRule{
				SchemaNameS:   d.SchemaNameS,
				TableNameS:    d.TableNameS,
				ColumnTypeS:   d.ColumnTypeS,
				ColumnTypeT:   d.ColumnTypeT,
				DefaultValueS: d.DefaultValueS,
				DefaultValueT: d.DefaultValueT,
			})
		}

		columnRules, err := model.GetIStructMigrateColumnRuleRW().FindColumnStructRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		for _, d := range columnRules {
			rules.ColumnStructRules = append(rules.ColumnStructRules, &pb.ColumnStructRule{
				SchemaNameS:   d.SchemaNameS,
				TableNameS:    d.TableNameS,
				ColumnNameS:   d.ColumnNameS,
				ColumnTypeS:   d.ColumnTypeS,
				ColumnTypeT:   d.ColumnTypeT,
				DefaultValueS: d.DefaultValueS,
				DefaultValueT: d.DefaultValueT,
			})
		}

		tableAttrRules, err := model.GetIStructMigrateTableAttrsRuleRW().FindTableAttrsRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}

		schemaUniqMap := make(map[string]struct{})
		for _, t := range tableAttrRules {
			schemaUniqMap[t.SchemaNameS] = struct{}{}
		}

		for s, _ := range schemaUniqMap {
			var (
				sourceTables []string
				tableAttrT   string
			)
			for _, t := range tableAttrRules {
				if strings.EqualFold(s, t.SchemaNameS) {
					sourceTables = append(sourceTables, t.TableNameS)
				}
				tableAttrT = t.TableAttrT
			}
			rules.TableAttrsRules = append(rules.TableAttrsRules, &pb.TableAttrsRule{
				SchemaNameS: s,
				TableNamesS: sourceTables,
				TableAttrsT: tableAttrT,
			})
		}

		schemaRouteRule, err := ShowSchemaRouteRule(txnCtx, taskInfo.TaskName)
		if err != nil {
			return err
		}

		resp = &pb.UpsertStructMigrateTaskRequest{
			TaskName:        taskInfo.TaskName,
			DatasourceNameS: taskInfo.DatasourceNameS,
			DatasourceNameT: taskInfo.DatasourceNameT,
			CaseFieldRule: &pb.CaseFieldRule{
				CaseFieldRuleS: taskInfo.CaseFieldRuleS,
				CaseFieldRuleT: taskInfo.CaseFieldRuleT,
			},
			Comment:            taskInfo.Comment,
			SchemaRouteRule:    schemaRouteRule,
			StructMigrateParam: param,
			StructMigrateRule:  rules,
		}

		return nil
	})
	if err != nil {
		return "", err
	}

	jsonStr, err := stringutil.MarshalJSON(resp)
	if err != nil {
		return jsonStr, err
	}
	return jsonStr, nil
}

func GenStructMigrateTask(ctx context.Context, serverAddr, taskName, outputDir string) error {
	etcdClient, err := etcdutil.CreateClient(ctx, []string{stringutil.WithHostPort(serverAddr)}, nil)
	if err != nil {
		return err
	}
	keyResp, err := etcdutil.GetKey(etcdClient, constant.DefaultMasterDatabaseDBMSKey, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	switch {
	case len(keyResp.Kvs) > 1:
		return fmt.Errorf("get key [%v] values is over one record from etcd server, it's panic, need check and fix, records are [%v]", constant.DefaultMasterDatabaseDBMSKey, keyResp.Kvs)
	case len(keyResp.Kvs) == 1:
		// open database conn
		var dbCfg *model.Database
		err = json.Unmarshal(keyResp.Kvs[0].Value, &dbCfg)
		if err != nil {
			return fmt.Errorf("json unmarshal [%v] to struct database faild: [%v]", stringutil.BytesToString(keyResp.Kvs[0].Value), err)
		}
		err = model.CreateDatabaseReadWrite(dbCfg)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("get key [%v] values isn't exist record from etcd server, it's panic, need check and fix, records are [%v]", constant.DefaultMasterDatabaseDBMSKey, keyResp.Kvs)
	}
	taskInfo, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: taskName, TaskMode: constant.TaskModeStructMigrate})
	if err != nil {
		return err
	}

	if !strings.EqualFold(taskInfo.TaskStatus, constant.TaskDatabaseStatusSuccess) {
		return fmt.Errorf("the [%v] task [%v] status [%v] is running in the worker [%v], please waiting success and retry", stringutil.StringLower(taskInfo.TaskMode),
			taskInfo.TaskName, stringutil.StringLower(taskInfo.TaskStatus), taskInfo.WorkerAddr)
	}

	var (
		migrateTasks []*task.StructMigrateTask
	)
	// get migrate task tables
	migrateTasks, err = model.GetIStructMigrateTaskRW().QueryStructMigrateTask(ctx, &task.StructMigrateTask{TaskName: taskInfo.TaskName, TaskStatus: constant.TaskDatabaseStatusSuccess, IsSchemaCreate: constant.DatabaseIsSchemaCreateSqlNO})
	if err != nil {
		return err
	}

	// the according schemaName, split task group for the migrateTasks
	groupSchemas := make(map[string]struct{})
	for _, m := range migrateTasks {
		groupSchemas[m.SchemaNameS] = struct{}{}
	}

	for schema, _ := range groupSchemas {
		var w database.IStructMigrateFileWriter
		w = taskflow.NewStructMigrateFile(ctx, taskInfo.TaskName, taskInfo.TaskFlow, schema, outputDir)
		err = w.InitOutputFile()
		if err != nil {
			return err
		}
		err = w.SyncStructFile()
		if err != nil {
			return err
		}
	}
	return nil
}

func StartStructMigrateTask(ctx context.Context, taskName, workerAddr string) error {
	startTime := time.Now()
	logger.Info("struct migrate task start", zap.String("task_name", taskName))
	logger.Info("struct migrate task get task information", zap.String("task_name", taskName))
	var (
		taskInfo    *task.Task
		datasourceS *datasource.Datasource
		datasourceT *datasource.Datasource
		err         error
	)
	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err = model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: taskName, TaskMode: constant.TaskModeStructMigrate})
		if err != nil {
			return err
		}
		datasourceS, err = model.GetIDatasourceRW().GetDatasource(txnCtx, taskInfo.DatasourceNameS)
		if err != nil {
			return err
		}
		datasourceT, err = model.GetIDatasourceRW().GetDatasource(txnCtx, taskInfo.DatasourceNameT)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	logger.Info("struct migrate task update task status",
		zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
	_, err = model.GetITaskRW().UpdateTask(ctx, &task.Task{
		TaskName: taskInfo.TaskName,
	}, map[string]interface{}{
		"WorkerAddr": workerAddr,
		"TaskStatus": constant.TaskDatabaseStatusRunning,
		"StartTime":  startTime,
	})
	if err != nil {
		return err
	}

	var databaseS, databaseT database.IDatabase

	logger.Info("struct migrate task get schema route",
		zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
	schemaRoute, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(ctx, &rule.SchemaRouteRule{TaskName: taskInfo.TaskName})
	if err != nil {
		return err
	}

	logger.Info("struct migrate task init database connection",
		zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
	databaseS, err = database.NewDatabase(ctx, datasourceS, schemaRoute.SchemaNameS)
	if err != nil {
		return err
	}
	defer databaseS.Close()

	switch {
	case strings.EqualFold(taskInfo.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(taskInfo.TaskFlow, constant.TaskFlowOracleToMySQL):
		databaseT, err = database.NewDatabase(ctx, datasourceT, "")
		if err != nil {
			return err
		}
		defer databaseT.Close()
	default:
		return fmt.Errorf("oracle current taskflow [%s] isn't support, please contact author or reselect", taskInfo.TaskFlow)
	}

	logger.Info("struct migrate task get task params",
		zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
	taskParams, err := getStructMigrateTasKParams(ctx, taskInfo.TaskName)
	if err != nil {
		return err
	}

	logger.Info("struct migrate task init task information",
		zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
	err = initStructMigrateTask(ctx, taskInfo, databaseS)
	if err != nil {
		return err
	}

	switch {
	case strings.EqualFold(taskInfo.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(taskInfo.TaskFlow, constant.TaskFlowOracleToMySQL):
		logger.Info("struct migrate task process migrate task",
			zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
		taskTime := time.Now()
		taskStruct := &taskflow.StructMigrateTask{
			Ctx:         ctx,
			Task:        taskInfo,
			SchemaNameS: schemaRoute.SchemaNameS,
			SchemaNameT: schemaRoute.SchemaNameT,
			DatabaseS:   databaseS,
			DatabaseT:   databaseT,
			DBCharsetS:  datasourceS.ConnectCharset,
			DBCharsetT:  datasourceT.ConnectCharset,
			TaskParams:  taskParams,
		}
		err = taskStruct.Start()
		if err != nil {
			return err
		}
		logger.Info("struct migrate task process migrate task",
			zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow),
			zap.String("cost", time.Now().Sub(taskTime).String()))
	default:
		return fmt.Errorf("the task [%v] task_mode [%v] task_flow [%v] isn't support, please contact auhtor or reselect", taskInfo.TaskName, taskInfo.TaskMode, taskInfo.TaskFlow)
	}

	// status
	var (
		migrateFailedResults  int64
		migrateWaitResults    int64
		migrateRunResults     int64
		migrateStopResults    int64
		migrateSuccessResults int64
		migrateTotalsResults  int64
	)

	statusRecords, err := model.GetIStructMigrateTaskRW().FindStructMigrateTaskGroupByTaskStatus(ctx, taskInfo.TaskName)
	if err != nil {
		return err
	}
	for _, rec := range statusRecords {
		switch strings.ToUpper(rec.TaskStatus) {
		case constant.TaskDatabaseStatusFailed:
			migrateFailedResults = rec.StatusCounts
			_, err = model.GetIStructMigrateSummaryRW().UpdateStructMigrateSummary(ctx, &task.StructMigrateSummary{
				TaskName:    rec.TaskName,
				SchemaNameS: schemaRoute.SchemaNameS,
			}, map[string]interface{}{
				"TableFails": rec.StatusCounts,
			})
			if err != nil {
				return err
			}
		case constant.TaskDatabaseStatusWaiting:
			migrateWaitResults = rec.StatusCounts
			_, err = model.GetIStructMigrateSummaryRW().UpdateStructMigrateSummary(ctx, &task.StructMigrateSummary{
				TaskName:    rec.TaskName,
				SchemaNameS: schemaRoute.SchemaNameS,
			}, map[string]interface{}{
				"TableWaits": rec.StatusCounts,
			})
			if err != nil {
				return err
			}
		case constant.TaskDatabaseStatusStopped:
			migrateStopResults = rec.StatusCounts
			_, err = model.GetIStructMigrateSummaryRW().UpdateStructMigrateSummary(ctx, &task.StructMigrateSummary{
				TaskName:    rec.TaskName,
				SchemaNameS: schemaRoute.SchemaNameS,
			}, map[string]interface{}{
				"TableStops": rec.StatusCounts,
			})
			if err != nil {
				return err
			}
		case constant.TaskDatabaseStatusRunning:
			migrateRunResults = rec.StatusCounts
			_, err = model.GetIStructMigrateSummaryRW().UpdateStructMigrateSummary(ctx, &task.StructMigrateSummary{
				TaskName:    rec.TaskName,
				SchemaNameS: schemaRoute.SchemaNameS,
			}, map[string]interface{}{
				"TableRuns": rec.StatusCounts,
			})
			if err != nil {
				return err
			}
		case constant.TaskDatabaseStatusSuccess:
			migrateSuccessResults = rec.StatusCounts
			_, err = model.GetIStructMigrateSummaryRW().UpdateStructMigrateSummary(ctx, &task.StructMigrateSummary{
				TaskName:    rec.TaskName,
				SchemaNameS: schemaRoute.SchemaNameS,
			}, map[string]interface{}{
				"TableSuccess": rec.StatusCounts,
			})
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("the task [%v] task_mode [%v] task_flow [%v] taskStatus [%v] panic, please contact auhtor or reselect", taskInfo.TaskName, taskInfo.TaskMode, taskInfo.TaskFlow, rec.TaskStatus)
		}
	}

	migrateTotalsResults = migrateFailedResults + migrateWaitResults + migrateStopResults + migrateSuccessResults + migrateRunResults

	if migrateFailedResults > 0 || migrateWaitResults > 0 || migrateRunResults > 0 || migrateStopResults > 0 {
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			_, err = model.GetITaskRW().UpdateTask(txnCtx, &task.Task{
				TaskName: taskInfo.TaskName,
			}, map[string]interface{}{
				"TaskStatus": constant.TaskDatabaseStatusFailed,
				"EndTime":    time.Now(),
			})
			if err != nil {
				return err
			}
			_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
				TaskName: taskInfo.TaskName,
				LogDetail: fmt.Sprintf("%v [%v] the worker [%v] task [%v] are exist failed [%d] or waiting [%d] or running [%d] or stopped [%d] status records during running operation, please see [struct_migrate_task] detail, total records [%d], success records [%d]",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(constant.TaskModeStructMigrate),
					taskInfo.WorkerAddr,
					taskInfo.TaskName,
					migrateFailedResults,
					migrateWaitResults,
					migrateRunResults,
					migrateStopResults,
					migrateTotalsResults,
					migrateSuccessResults),
			})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}

		logger.Info("struct migrate task failed",
			zap.String("task_name", taskName),
			zap.String("task_mode", taskInfo.TaskMode),
			zap.String("task_flow", taskInfo.TaskFlow),
			zap.Int64("total records", migrateTotalsResults),
			zap.Int64("failed records", migrateFailedResults),
			zap.Int64("wait records", migrateWaitResults),
			zap.Int64("running records", migrateRunResults),
			zap.Int64("stopped records", migrateStopResults),
			zap.Int64("success records", migrateSuccessResults),
			zap.String("detail tips", "please see [struct_migrate_task] detail"),
			zap.String("cost", time.Now().Sub(startTime).String()))

		return nil
	}
	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		_, err = model.GetITaskRW().UpdateTask(txnCtx, &task.Task{
			TaskName: taskInfo.TaskName,
		}, map[string]interface{}{
			"TaskStatus": constant.TaskDatabaseStatusSuccess,
			"EndTime":    time.Now(),
		})
		if err != nil {
			return err
		}
		_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
			TaskName: taskInfo.TaskName,
			LogDetail: fmt.Sprintf("%v [%v] the worker [%v] task [%v] running success, total records [%d], success records [%d], cost: [%v]",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeStructMigrate),
				taskInfo.WorkerAddr,
				taskName,
				migrateTotalsResults,
				migrateSuccessResults,
				time.Now().Sub(startTime).String()),
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	logger.Info("struct migrate task success",
		zap.String("task_name", taskName),
		zap.String("task_mode", taskInfo.TaskMode),
		zap.String("task_flow", taskInfo.TaskFlow),
		zap.Int64("total records", migrateTotalsResults),
		zap.Int64("success records", migrateSuccessResults),
		zap.String("detail tips", "please see [struct_migrate_task] detail"),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}

func StopStructMigrateTask(ctx context.Context, taskName string) error {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		_, err := model.GetITaskRW().UpdateTask(txnCtx, &task.Task{
			TaskName: taskName,
		}, map[string]interface{}{
			"TaskStatus": constant.TaskDatabaseStatusStopped,
		})
		if err != nil {
			return err
		}
		_, err = model.GetIStructMigrateTaskRW().BatchUpdateStructMigrateTask(txnCtx, &task.StructMigrateTask{
			TaskName:   taskName,
			TaskStatus: constant.TaskDatabaseStatusRunning,
		}, map[string]interface{}{
			"TaskStatus": constant.TaskDatabaseStatusStopped,
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

func getStructMigrateTasKParams(ctx context.Context, taskName string) (*pb.StructMigrateParam, error) {
	taskParam := &pb.StructMigrateParam{}

	migrateParams, err := model.GetIParamsRW().QueryTaskCustomParam(ctx, &params.TaskCustomParam{
		TaskName: taskName,
		TaskMode: constant.TaskModeStructMigrate,
	})
	if err != nil {
		return taskParam, err
	}
	for _, p := range migrateParams {
		if strings.EqualFold(p.ParamName, constant.ParamNameStructMigrateMigrateThread) {
			migrateThread, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.MigrateThread = migrateThread
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStructMigrateCreateIfNotExist) {
			createIfNotExist, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.CreateIfNotExist = createIfNotExist
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStructMigrateEnableDirectCreate) {
			enableDirectCreate, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.EnableDirectCreate = enableDirectCreate
		}
	}
	return taskParam, nil
}

func initStructMigrateTask(ctx context.Context, taskInfo *task.Task, databaseS database.IDatabase) error {
	schemaRoute, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(ctx,
		&rule.SchemaRouteRule{TaskName: taskInfo.TaskName})
	if err != nil {
		return err
	}

	// filter database table
	schemaTaskTables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(ctx, &rule.MigrateTaskTable{
		TaskName:    schemaRoute.TaskName,
		SchemaNameS: schemaRoute.SchemaNameS,
	})
	if err != nil {
		return err
	}
	var (
		includeTables  []string
		excludeTables  []string
		databaseTables []string // task tables
	)
	databaseTableTypeMap := make(map[string]string)

	for _, t := range schemaTaskTables {
		if strings.EqualFold(t.IsExclude, constant.MigrateTaskTableIsExclude) {
			excludeTables = append(excludeTables, t.TableNameS)
		}
		if strings.EqualFold(t.IsExclude, constant.MigrateTaskTableIsNotExclude) {
			includeTables = append(includeTables, t.TableNameS)
		}
	}

	databaseTables, err = databaseS.FilterDatabaseTable(schemaRoute.SchemaNameS, includeTables, excludeTables)
	if err != nil {
		return err
	}
	databaseTableTypeMap, err = databaseS.GetDatabaseTableType(schemaRoute.SchemaNameS)
	if err != nil {
		return err
	}

	// get table route rule
	tableRouteRule := make(map[string]string)

	tableRoutes, err := model.GetIMigrateTableRouteRW().FindTableRouteRule(ctx, &rule.TableRouteRule{
		TaskName:    schemaRoute.TaskName,
		SchemaNameS: schemaRoute.SchemaNameS,
	})
	for _, tr := range tableRoutes {
		tableRouteRule[tr.TableNameS] = tr.TableNameT
	}

	// clear the struct migrate task table
	migrateTasks, err := model.GetIStructMigrateTaskRW().BatchFindStructMigrateTask(ctx, &task.StructMigrateTask{TaskName: taskInfo.TaskName})
	if err != nil {
		return err
	}

	// repeatInitTableMap used for store the struct_migrate_task table name has be finished, avoid repeated initialization
	repeatInitTableMap := make(map[string]struct{})
	if len(migrateTasks) > 0 {
		taskTablesMap := make(map[string]struct{})
		for _, t := range databaseTables {
			taskTablesMap[t] = struct{}{}
		}
		for _, smt := range migrateTasks {
			if _, ok := taskTablesMap[smt.TableNameS]; !ok {
				err = model.GetIStructMigrateTaskRW().DeleteStructMigrateTask(ctx, smt.ID)
				if err != nil {
					return err
				}
			} else {
				repeatInitTableMap[smt.TableNameS] = struct{}{}
			}
		}
	}

	// database tables
	// init database table
	// get table column route rule
	for _, sourceTable := range databaseTables {
		initStructInfos, err := model.GetIStructMigrateTaskRW().GetStructMigrateTaskTable(ctx, &task.StructMigrateTask{
			TaskName:    taskInfo.TaskName,
			SchemaNameS: schemaRoute.SchemaNameS,
			TableNameS:  sourceTable,
		})
		if err != nil {
			return err
		}
		if len(initStructInfos) > 1 {
			return fmt.Errorf("the struct migrate task table is over one, it should be only one")
		}
		// if the table is existed and task_status success, then skip init
		if _, ok := repeatInitTableMap[sourceTable]; ok && strings.EqualFold(initStructInfos[0].TaskStatus, constant.TaskDatabaseStatusSuccess) {
			continue
		}
		var (
			targetTable string
		)
		if val, ok := tableRouteRule[sourceTable]; ok {
			targetTable = val
		} else {
			// the according target case field rule convert
			if strings.EqualFold(taskInfo.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleLower) {
				targetTable = stringutil.StringLower(sourceTable)
			}
			if strings.EqualFold(taskInfo.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleUpper) {
				targetTable = stringutil.StringUpper(sourceTable)
			}
			if strings.EqualFold(taskInfo.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleOrigin) {
				targetTable = sourceTable
			}
		}

		_, err = model.GetIStructMigrateTaskRW().CreateStructMigrateTask(ctx, &task.StructMigrateTask{
			TaskName:    taskInfo.TaskName,
			SchemaNameS: schemaRoute.SchemaNameS,
			TableNameS:  sourceTable,
			TableTypeS:  databaseTableTypeMap[sourceTable],
			SchemaNameT: schemaRoute.SchemaNameT,
			TableNameT:  targetTable,
			TaskStatus:  constant.TaskDatabaseStatusWaiting,
		})
		if err != nil {
			return err
		}
	}

	_, err = model.GetIStructMigrateSummaryRW().CreateStructMigrateSummary(ctx,
		&task.StructMigrateSummary{
			TaskName:    taskInfo.TaskName,
			SchemaNameS: schemaRoute.SchemaNameS,
			TableTotals: uint64(len(databaseTables) + 1), // include schema create sql
		})
	if err != nil {
		return err
	}
	return nil
}
