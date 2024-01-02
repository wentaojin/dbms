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
package master

import (
	"context"
	"errors"
	"strings"

	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/proto/pb"

	"github.com/wentaojin/dbms/openapi"
)

func (s *Server) upsertDatabase(ctx context.Context, req openapi.APIPutDatabaseJSONRequestBody) (string, error) {
	kvResp, err := s.UpsertDatabase(ctx, &pb.UpsertDatabaseRequest{
		Database: &pb.Database{
			Username:      *req.Username,
			Password:      *req.Password,
			Host:          *req.Host,
			Port:          *req.Port,
			Schema:        *req.Schema,
			SlowThreshold: *req.SlowThreshold,
		},
	})
	if err != nil {
		return "", err
	}

	if strings.EqualFold(kvResp.Response.Result, constant.ResponseResultStatusSuccess) {
		return kvResp.Response.Message, nil
	}
	return "", errors.New(kvResp.Response.Message)
}

func (s *Server) listDatabase(ctx context.Context) (string, error) {
	kvResp, err := s.ShowDatabase(ctx, &pb.ShowDatabaseRequest{})
	if err != nil {
		return kvResp.String(), err
	}
	if strings.EqualFold(kvResp.Response.Result, constant.ResponseResultStatusSuccess) {
		return kvResp.Response.Message, nil
	}
	return "", errors.New(kvResp.Response.Message)
}

func (s *Server) deleteDatabase(ctx context.Context) (string, error) {
	kvResp, err := s.DeleteDatabase(ctx, &pb.DeleteDatabaseRequest{})
	if err != nil {
		return kvResp.String(), err
	}
	if strings.EqualFold(kvResp.Response.Result, constant.ResponseResultStatusSuccess) {
		return kvResp.Response.Message, nil
	}
	return "", errors.New(kvResp.Response.Message)
}

func (s *Server) upsertDatasource(ctx context.Context, req openapi.APIPutDatasourceJSONRequestBody) (string, error) {
	var pds []*pb.Datasource

	for _, r := range *req.Datasource {
		ds := &pb.Datasource{
			DatasourceName: *r.DatasourceName,
			DbType:         *r.DbType,
			Username:       *r.Username,
			Password:       *r.Password,
			Host:           *r.Host,
			Port:           *r.Port,
			ConnectCharset: *r.ConnectCharset,
			ConnectParams:  *r.ConnectParams,
			ServiceName:    *r.ServiceName,
			PdbName:        *r.PdbName,
			Comment:        *r.Comment,
			ConnectStatus:  *r.ConnectStatus,
		}
		pds = append(pds, ds)
	}
	resp, err := s.UpsertDatasource(ctx, &pb.UpsertDatasourceRequest{
		Datasource: pds})
	if err != nil {
		return resp.String(), err
	}
	if strings.EqualFold(resp.Response.Result, constant.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) deleteDatasource(ctx context.Context, req openapi.APIDeleteDatasourceJSONRequestBody) (string, error) {
	resp, err := s.DeleteDatasource(ctx, &pb.DeleteDatasourceRequest{
		DatasourceName: req})
	if err != nil {
		return resp.String(), err
	}
	if strings.EqualFold(resp.Response.Result, constant.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) listDatasource(ctx context.Context, req openapi.APIListDatasourceJSONRequestBody) (string, error) {
	resp, err := s.ShowDatasource(ctx, &pb.ShowDatasourceRequest{
		DatasourceName: *req.Param,
		Page:           *req.Page,
		PageSize:       *req.PageSize})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, constant.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) upsertTaskMigrateRule(ctx context.Context, req openapi.APIPutTaskMigrateRuleJSONRequestBody) (string, error) {
	var migrateSchemaRs []*pb.SchemaRouteRule

	for _, r := range *req.MigrateSchemaRules {
		var tableRoutes []*pb.TableRouteRule

		for _, t := range *r.SourceTableRoutes {
			tableRoutes = append(tableRoutes, &pb.TableRouteRule{
				SourceTable:      *t.SourceTable,
				TargetTable:      *t.TargetTable,
				ColumnRouteRules: *t.SourceColumnRoutes,
			})
		}
		migrateSchemaRs = append(migrateSchemaRs, &pb.SchemaRouteRule{
			SourceSchema:       *r.SourceSchema,
			TargetSchema:       *r.TargetSchema,
			SourceIncludeTable: *r.SourceIncludeTable,
			SourceExcludeTable: *r.SourceExcludeTable,
			TableRouteRules:    tableRoutes,
		})
	}

	resp, err := s.UpsertTaskMigrateRule(ctx, &pb.UpsertMigrateTaskRuleRequest{
		MigrateTaskRule: &pb.MigrateTaskRule{
			TaskRuleName:     *req.TaskRuleName,
			DatasourceNameS:  *req.DatasourceNameS,
			DatasourceNameT:  *req.DatasourceNameT,
			Comment:          *req.Comment,
			SchemaRouteRules: migrateSchemaRs,
		}})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, constant.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) deleteTaskMigrateRule(ctx context.Context, req openapi.APIDeleteTaskMigrateRuleJSONRequestBody) (string, error) {
	resp, err := s.DeleteTaskMigrateRule(ctx, &pb.DeleteMigrateTaskRuleRequest{TaskRuleName: req})
	if err != nil {
		return resp.String(), err
	}
	if strings.EqualFold(resp.Response.Result, constant.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) listTaskMigrateRule(ctx context.Context, req openapi.APIListTaskMigrateRuleJSONRequestBody) (string, error) {
	resp, err := s.ShowTaskMigrateRule(ctx, &pb.ShowMigrateTaskRuleRequest{
		TaskRuleName: *req.Param,
		Page:         *req.Page,
		PageSize:     *req.PageSize,
	})
	if err != nil {
		return resp.String(), err
	}
	if strings.EqualFold(resp.Response.Result, constant.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) upsertStructMigrateTask(ctx context.Context, req openapi.APIPutStructMigrateTaskJSONRequestBody) (string, error) {
	var (
		taskLevelRules   []*pb.TaskStructRule
		schemaLevelRules []*pb.SchemaStructRule
		tableLevelRules  []*pb.TableStructRule
		columnLevelRules []*pb.ColumnStructRule
		tableAttrsRules  []*pb.TableAttrsRule
	)

	for _, l := range *req.StructMigrateRule.TaskStructRules {
		taskLevelRules = append(taskLevelRules, &pb.TaskStructRule{
			ColumnTypeS:   *l.ColumnTypeS,
			ColumnTypeT:   *l.ColumnTypeT,
			DefaultValueS: *l.DefaultValueS,
			DefaultValueT: *l.DefaultValueT,
		})
	}
	for _, l := range *req.StructMigrateRule.SchemaStructRules {
		schemaLevelRules = append(schemaLevelRules, &pb.SchemaStructRule{
			SourceSchema:  *l.SourceSchema,
			ColumnTypeS:   *l.ColumnTypeS,
			ColumnTypeT:   *l.ColumnTypeT,
			DefaultValueS: *l.DefaultValueS,
			DefaultValueT: *l.DefaultValueT,
		})
	}
	for _, l := range *req.StructMigrateRule.TableStructRules {
		tableLevelRules = append(tableLevelRules, &pb.TableStructRule{
			SourceSchema:  *l.SourceSchema,
			SourceTable:   *l.SourceTable,
			ColumnTypeS:   *l.ColumnTypeS,
			ColumnTypeT:   *l.ColumnTypeT,
			DefaultValueS: *l.DefaultValueS,
			DefaultValueT: *l.DefaultValueT,
		})
	}
	for _, l := range *req.StructMigrateRule.ColumnStructRules {
		columnLevelRules = append(columnLevelRules, &pb.ColumnStructRule{
			SourceSchema:  *l.SourceSchema,
			SourceTable:   *l.SourceTable,
			SourceColumn:  *l.SourceColumn,
			ColumnTypeS:   *l.ColumnTypeS,
			ColumnTypeT:   *l.ColumnTypeT,
			DefaultValueS: *l.DefaultValueS,
			DefaultValueT: *l.DefaultValueT,
		})
	}
	for _, l := range *req.StructMigrateRule.TableAttrsRules {
		tableAttrsRules = append(tableAttrsRules, &pb.TableAttrsRule{
			SourceSchema: *l.SourceSchema,
			SourceTables: *l.SourceTables,
			TableAttrsT:  *l.TableAttrsT,
		})
	}
	resp, err := s.UpsertStructMigrateTask(ctx, &pb.UpsertStructMigrateTaskRequest{
		TaskName:     *req.TaskName,
		TaskRuleName: *req.TaskRuleName,
		StructMigrateParam: &pb.StructMigrateParam{
			CaseFieldRule: *req.StructMigrateParam.LowerCaseFieldName,
			MigrateThread: *req.StructMigrateParam.MigrateThread,
			DirectWrite:   *req.StructMigrateParam.DirectWrite,
			OutputDir:     *req.StructMigrateParam.OutputDir,
		},
		StructMigrateRule: &pb.StructMigrateRule{
			TaskStructRules:   taskLevelRules,
			SchemaStructRules: schemaLevelRules,
			TableStructRules:  tableLevelRules,
			ColumnStructRules: columnLevelRules,
			TableAttrsRules:   tableAttrsRules,
		},
	})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, constant.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) deleteStructMigrateTask(ctx context.Context, req openapi.APIDeleteStructMigrateTaskJSONRequestBody) (string, error) {
	resp, err := s.DeleteStructMigrateTask(ctx, &pb.DeleteStructMigrateTaskRequest{TaskName: req})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, constant.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) listStructMigrateTask(ctx context.Context, req openapi.APIListStructMigrateTaskJSONRequestBody) (string, error) {
	resp, err := s.ShowStructMigrateTask(ctx, &pb.ShowStructMigrateTaskRequest{
		TaskName: *req.Param,
		Page:     *req.Page,
		PageSize: *req.PageSize,
	})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, constant.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) upsertTask(ctx context.Context, req openapi.APIPutTaskJSONRequestBody) (string, error) {
	resp, err := s.OperateTask(ctx, &pb.OperateTaskRequest{
		Operate:  *req.Operate,
		TaskName: *req.TaskName,
		Express:  *req.Express,
	})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, constant.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) deleteTask(ctx context.Context, req openapi.APIDeleteTaskJSONRequestBody) (string, error) {
	resp, err := s.OperateTask(ctx, &pb.OperateTaskRequest{
		Operate:  *req.Operate,
		TaskName: *req.TaskName,
		Express:  *req.Express,
	})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, constant.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) killTask(ctx context.Context, req openapi.APIKillTaskJSONRequestBody) (string, error) {
	resp, err := s.OperateTask(ctx, &pb.OperateTaskRequest{
		Operate:  *req.Operate,
		TaskName: *req.TaskName,
		Express:  *req.Express,
	})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, constant.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}
