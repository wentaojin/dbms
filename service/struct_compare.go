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

	"github.com/fatih/color"
	"github.com/wentaojin/dbms/database/processor"
	"github.com/wentaojin/dbms/database/taskflow"

	"github.com/wentaojin/dbms/utils/etcdutil"

	clientv3 "go.etcd.io/etcd/client/v3"

	"go.uber.org/zap"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/logger"

	"github.com/wentaojin/dbms/model/common"
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

func PromptStructCompareTask(ctx context.Context, taskName, serverAddr string) error {
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
	taskInfo, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: taskName})
	if err != nil {
		return err
	}
	if !strings.EqualFold(taskInfo.TaskMode, constant.TaskModeStructCompare) && !strings.EqualFold(taskInfo.TaskMode, "") {
		return fmt.Errorf("the task name [%s] has be existed in the task mode [%s], please rename the global unqiue task name", taskName, taskInfo.TaskMode)
	}
	if strings.EqualFold(taskInfo.TaskMode, constant.TaskModeStructCompare) {
		if err = stringutil.PromptForAnswerOrAbortError(
			"Yes, I know my configuration file will be overwrite.",
			fmt.Sprintf("This operation will overwrite the task_mode [%s] task_name [%s] configuration file\n.",
				color.HiYellowString(strings.ToLower(constant.TaskModeStructCompare)),
				color.HiYellowString(taskInfo.TaskName),
			)+"\nAre you sure to continue?",
		); err != nil {
			return err
		}
		_, err := DeleteStructCompareTask(ctx, &pb.DeleteStructCompareTaskRequest{TaskName: []string{taskName}})
		if err != nil {
			return err
		}
	}
	return nil
}

func UpsertStructCompareTask(ctx context.Context, req *pb.UpsertStructCompareTaskRequest) (string, error) {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
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
			TaskMode:        constant.TaskModeStructCompare,
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

		err = UpsertSchemaRouteRule(txnCtx, req.TaskName, req.DatasourceNameS, req.CaseFieldRule, req.SchemaRouteRule, nil, nil)
		if err != nil {
			return err
		}

		fieldInfos := stringutil.GetJSONTagFieldValue(req.StructCompareParam)
		for jsonTag, fieldValue := range fieldInfos {
			_, err = model.GetIParamsRW().CreateTaskCustomParam(txnCtx, &params.TaskCustomParam{
				TaskName:   req.TaskName,
				TaskMode:   constant.TaskModeStructCompare,
				ParamName:  jsonTag,
				ParamValue: fieldValue,
			})
			if err != nil {
				return err
			}
		}

		for _, r := range req.StructCompareRule.TaskStructRules {
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
		for _, r := range req.StructCompareRule.SchemaStructRules {
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
		for _, r := range req.StructCompareRule.TableStructRules {
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
		for _, r := range req.StructCompareRule.ColumnStructRules {
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

func DeleteStructCompareTask(ctx context.Context, req *pb.DeleteStructCompareTaskRequest) (string, error) {
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

func ShowStructCompareTask(ctx context.Context, req *pb.ShowStructCompareTaskRequest) (string, error) {
	var (
		resp  *pb.UpsertStructCompareTaskRequest
		param *pb.StructCompareParam
		rules *pb.StructCompareRule
	)

	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err := model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: req.TaskName})
		if err != nil {
			return err
		}

		paramsInfo, err := model.GetIParamsRW().QueryTaskCustomParam(txnCtx, &params.TaskCustomParam{
			TaskName: req.TaskName,
			TaskMode: constant.TaskModeStructCompare,
		})
		if err != nil {
			return err
		}

		paramMap := make(map[string]string)
		for _, p := range paramsInfo {
			paramMap[p.ParamName] = p.ParamValue
		}

		compareThread, err := strconv.Atoi(paramMap[constant.ParamNameStructCompareCompareThread])
		if err != nil {
			return err
		}
		callTimeout, err := strconv.ParseUint(paramMap[constant.ParamNameStructCompareCallTimeout], 10, 64)
		if err != nil {
			return err
		}
		enableCheckpoints, err := strconv.ParseBool(paramMap[constant.ParamNameStructCompareEnableCheckpoint])
		if err != nil {
			return err
		}
		ignoreCaseCompare, err := strconv.ParseBool(paramMap[constant.ParamNameStructCompareIgnoreCaseCompare])
		if err != nil {
			return err
		}

		param = &pb.StructCompareParam{
			CompareThread:     uint64(compareThread),
			EnableCheckpoint:  enableCheckpoints,
			CallTimeout:       callTimeout,
			IgnoreCaseCompare: ignoreCaseCompare,
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

		schemaRouteRule, _, _, err := ShowSchemaRouteRule(txnCtx, taskInfo.TaskName)
		if err != nil {
			return err
		}

		resp = &pb.UpsertStructCompareTaskRequest{
			TaskName:        taskInfo.TaskName,
			DatasourceNameS: taskInfo.DatasourceNameS,
			DatasourceNameT: taskInfo.DatasourceNameT,
			CaseFieldRule: &pb.CaseFieldRule{
				CaseFieldRuleS: taskInfo.CaseFieldRuleS,
				CaseFieldRuleT: taskInfo.CaseFieldRuleT,
			},
			Comment:            taskInfo.Comment,
			SchemaRouteRule:    schemaRouteRule,
			StructCompareParam: param,
			StructCompareRule:  rules,
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

func GenStructCompareTask(ctx context.Context, serverAddr, taskName, outputDir string) error {
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

	var (
		taskInfo    *task.Task
		schemaRoute *rule.SchemaRouteRule
	)
	if errMsg := model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err = model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: taskName, TaskMode: constant.TaskModeStructCompare})
		if err != nil {
			return err
		}
		schemaRoute, err = model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(txnCtx, &rule.SchemaRouteRule{TaskName: taskName})
		if err != nil {
			return err
		}
		return nil
	}); errMsg != nil {
		return errMsg
	}

	if !strings.EqualFold(taskInfo.TaskStatus, constant.TaskDatabaseStatusSuccess) {
		return fmt.Errorf("the [%v] task [%v] status [%v] is running in the worker [%v], please waiting success and retry", stringutil.StringLower(taskInfo.TaskMode),
			taskInfo.TaskName, stringutil.StringLower(taskInfo.TaskStatus), taskInfo.WorkerAddr)
	}

	var w database.IFileWriter
	w = processor.NewStructCompareFile(ctx, taskInfo.TaskName, schemaRoute.SchemaNameS, outputDir)
	err = w.InitFile()
	if err != nil {
		return err
	}
	err = w.SyncFile()
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}
	return nil
}

func StartStructCompareTask(ctx context.Context, taskName, workerAddr string) error {
	startTime := time.Now()
	logger.Info("struct compare task start", zap.String("task_name", taskName))
	logger.Info("struct compare task get task information", zap.String("task_name", taskName))
	var (
		taskInfo         *task.Task
		sourceDatasource *datasource.Datasource
		targetDatasource *datasource.Datasource
		err              error
	)
	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err = model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: taskName, TaskMode: constant.TaskModeStructCompare})
		if err != nil {
			return err
		}
		sourceDatasource, err = model.GetIDatasourceRW().GetDatasource(txnCtx, taskInfo.DatasourceNameS)
		if err != nil {
			return err
		}
		targetDatasource, err = model.GetIDatasourceRW().GetDatasource(txnCtx, taskInfo.DatasourceNameT)
		if err != nil {
			return err
		}
		err = model.GetITaskLogRW().DeleteLog(txnCtx, []string{taskName})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	logger.Info("struct compare task update task status",
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

	logger.Info("struct compare task get task params",
		zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
	taskParams, err := getStructCompareTasKParams(ctx, taskInfo.TaskName)
	if err != nil {
		return err
	}

	if !taskParams.EnableCheckpoint {
		logger.Warn("struct compare task clear task records",
			zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			err = model.GetIStructCompareSummaryRW().DeleteStructCompareSummaryName(txnCtx, []string{taskInfo.TaskName})
			if err != nil {
				return err
			}
			err = model.GetIStructCompareTaskRW().DeleteStructCompareTaskName(txnCtx, []string{taskInfo.TaskName})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	switch taskInfo.TaskFlow {
	case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB, constant.TaskFlowPostgresToMySQL, constant.TaskFlowPostgresToTiDB:
		logger.Info("struct compare task process task", zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
		taskTime := time.Now()
		sc := &taskflow.StructCompareTask{
			Ctx:         ctx,
			Task:        taskInfo,
			DatasourceS: sourceDatasource,
			DatasourceT: targetDatasource,
			TaskParams:  taskParams,
		}
		err = sc.Start()
		if err != nil {
			return err
		}
		logger.Info("struct compare task process task",
			zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow),
			zap.String("cost", time.Now().Sub(taskTime).String()))
	default:
		return fmt.Errorf("current struct compare task [%s] datasource [%s] source [%s] isn't support, please contact auhtor or reselect", taskName, sourceDatasource.DatasourceName, sourceDatasource.DbType)
	}

	// status
	var (
		failedResults   int64
		waitResults     int64
		runResults      int64
		stopResults     int64
		equalResults    int64
		notEqualResults int64
		totalsResults   int64
	)

	statusRecords, err := model.GetIStructCompareTaskRW().FindStructCompareTaskGroupByTaskStatus(ctx, taskName)
	if err != nil {
		return err
	}
	for _, rec := range statusRecords {
		switch strings.ToUpper(rec.TaskStatus) {
		case constant.TaskDatabaseStatusFailed:
			failedResults = rec.StatusCounts
		case constant.TaskDatabaseStatusWaiting:
			waitResults = rec.StatusCounts
		case constant.TaskDatabaseStatusStopped:
			stopResults = rec.StatusCounts
		case constant.TaskDatabaseStatusRunning:
			runResults = rec.StatusCounts
		case constant.TaskDatabaseStatusEqual:
			equalResults = rec.StatusCounts
		case constant.TaskDatabaseStatusNotEqual:
			notEqualResults = rec.StatusCounts
		default:
			return fmt.Errorf("the task [%v] task_mode [%s] task_flow [%v] taskStatus [%v] panic, please contact auhtor or reselect", taskInfo.TaskName, taskInfo.TaskMode, taskInfo.TaskFlow, rec.TaskStatus)
		}
	}

	totalsResults = failedResults + waitResults + stopResults + equalResults + notEqualResults + runResults

	if failedResults > 0 || waitResults > 0 || runResults > 0 || stopResults > 0 {
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			_, err = model.GetITaskRW().UpdateTask(txnCtx, &task.Task{
				TaskName: taskName,
			}, map[string]interface{}{
				"TaskStatus": constant.TaskDatabaseStatusFailed,
				"EndTime":    time.Now(),
			})
			if err != nil {
				return err
			}
			_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
				TaskName: taskName,
				LogDetail: fmt.Sprintf("%v [%v] the worker [%v] task [%v] are exist failed [%d] or waiting [%d] or running [%d] or stopped [%d] status records during running operation, please see [struct_compare_task] detail, total records [%d], qual records [%d], not qual records [%d]",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(constant.TaskModeStructCompare),
					taskInfo.WorkerAddr,
					taskName,
					failedResults,
					waitResults,
					runResults,
					stopResults,
					totalsResults,
					equalResults,
					notEqualResults),
			})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}

		logger.Info("struct compare task failed",
			zap.String("task_name", taskInfo.TaskName),
			zap.String("task_mode", taskInfo.TaskMode),
			zap.String("task_flow", taskInfo.TaskFlow),
			zap.Int64("total records", totalsResults),
			zap.Int64("failed records", failedResults),
			zap.Int64("wait records", waitResults),
			zap.Int64("running records", runResults),
			zap.Int64("stopped records", stopResults),
			zap.Int64("equal records", equalResults),
			zap.Int64("not equal records", notEqualResults),
			zap.String("detail tips", "please see [struct_compare_task] detail"),
			zap.String("cost", time.Now().Sub(startTime).String()))

		return nil
	}
	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		_, err = model.GetITaskRW().UpdateTask(txnCtx, &task.Task{
			TaskName: taskName,
		}, map[string]interface{}{
			"TaskStatus": constant.TaskDatabaseStatusSuccess,
			"EndTime":    time.Now(),
		})
		if err != nil {
			return err
		}
		_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
			TaskName: taskName,
			LogDetail: fmt.Sprintf("%v [%v] the worker [%v] task [%v] running success, total records [%d], qual records [%d], not qual records [%d], cost: [%v]",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeStructCompare),
				taskInfo.WorkerAddr,
				taskName,
				totalsResults,
				equalResults,
				notEqualResults,
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

	logger.Info("struct compare task success",
		zap.String("task_name", taskInfo.TaskName),
		zap.String("task_mode", taskInfo.TaskMode),
		zap.String("task_flow", taskInfo.TaskFlow),
		zap.Int64("total records", totalsResults),
		zap.Int64("equal records", equalResults),
		zap.Int64("not equal records", notEqualResults),
		zap.String("detail tips", "please see [struct_compare_task] detail"),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}

func StopStructCompareTask(ctx context.Context, taskName string) error {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		_, err := model.GetITaskRW().UpdateTask(txnCtx, &task.Task{
			TaskName: taskName,
		}, map[string]interface{}{
			"TaskStatus": constant.TaskDatabaseStatusStopped,
		})
		if err != nil {
			return err
		}
		_, err = model.GetIStructCompareTaskRW().BatchUpdateStructCompareTask(txnCtx, &task.StructCompareTask{
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

func getStructCompareTasKParams(ctx context.Context, taskName string) (*pb.StructCompareParam, error) {
	taskParam := &pb.StructCompareParam{}

	migrateParams, err := model.GetIParamsRW().QueryTaskCustomParam(ctx, &params.TaskCustomParam{
		TaskName: taskName,
		TaskMode: constant.TaskModeStructCompare,
	})
	if err != nil {
		return taskParam, err
	}
	for _, p := range migrateParams {
		if strings.EqualFold(p.ParamName, constant.ParamNameStructCompareCompareThread) {
			compareThread, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.CompareThread = compareThread
		}

		if strings.EqualFold(p.ParamName, constant.ParamNameStructCompareEnableCheckpoint) {
			enableCheckpoint, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return nil, err
			}
			taskParam.EnableCheckpoint = enableCheckpoint
		}

		if strings.EqualFold(p.ParamName, constant.ParamNameStructCompareIgnoreCaseCompare) {
			ignoreCaseCompare, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return nil, err
			}
			taskParam.IgnoreCaseCompare = ignoreCaseCompare
		}

		if strings.EqualFold(p.ParamName, constant.ParamNameStructCompareCallTimeout) {
			callTimeout, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.CallTimeout = callTimeout
		}
	}
	return taskParam, nil
}
