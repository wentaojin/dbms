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

	if strings.EqualFold(kvResp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return kvResp.Response.Message, nil
	}
	return "", errors.New(kvResp.Response.Message)
}

func (s *Server) listDatabase(ctx context.Context) (string, error) {
	kvResp, err := s.ShowDatabase(ctx, &pb.ShowDatabaseRequest{})
	if err != nil {
		return kvResp.String(), err
	}
	if strings.EqualFold(kvResp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return kvResp.Response.Message, nil
	}
	return "", errors.New(kvResp.Response.Message)
}

func (s *Server) deleteDatabase(ctx context.Context) (string, error) {
	kvResp, err := s.DeleteDatabase(ctx, &pb.DeleteDatabaseRequest{})
	if err != nil {
		return kvResp.String(), err
	}
	if strings.EqualFold(kvResp.Response.Result, openapi.ResponseResultStatusSuccess) {
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
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) deleteDatasource(ctx context.Context, req openapi.APIDeleteDatasourceJSONRequestBody) (string, error) {
	resp, err := s.DeleteDatasource(ctx, &pb.DeleteDatasourceRequest{DatasourceName: *req.Param})
	if err != nil {
		return resp.String(), err
	}
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
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
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) upsertStructMigrateTask(ctx context.Context, req openapi.APIPutStructMigrateJSONRequestBody) (string, error) {
	var (
		taskLevelRules   []*pb.TaskStructRule
		schemaLevelRules []*pb.SchemaStructRule
		tableLevelRules  []*pb.TableStructRule
		columnLevelRules []*pb.ColumnStructRule
		tableAttrsRules  []*pb.TableAttrsRule
		migrateSchemaRs  *pb.SchemaRouteRule
	)

	var tableRoutes []*pb.TableRouteRule

	for _, t := range *req.SchemaRouteRule.TableRouteRules {
		tableRoutes = append(tableRoutes, &pb.TableRouteRule{
			TableNameS:       *t.TableNameS,
			TableNameT:       *t.TableNameT,
			ColumnRouteRules: *t.ColumnRouteRules,
		})
	}
	migrateSchemaRs = &pb.SchemaRouteRule{
		SchemaNameS:     *req.SchemaRouteRule.SchemaNameS,
		SchemaNameT:     *req.SchemaRouteRule.SchemaNameT,
		IncludeTableS:   *req.SchemaRouteRule.IncludeTableS,
		ExcludeTableS:   *req.SchemaRouteRule.ExcludeTableS,
		TableRouteRules: tableRoutes,
	}

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
			SchemaNameS:   *l.SchemaNameS,
			ColumnTypeS:   *l.ColumnTypeS,
			ColumnTypeT:   *l.ColumnTypeT,
			DefaultValueS: *l.DefaultValueS,
			DefaultValueT: *l.DefaultValueT,
		})
	}
	for _, l := range *req.StructMigrateRule.TableStructRules {
		tableLevelRules = append(tableLevelRules, &pb.TableStructRule{
			SchemaNameS:   *l.SchemaNameS,
			TableNameS:    *l.TableNameS,
			ColumnTypeS:   *l.ColumnTypeS,
			ColumnTypeT:   *l.ColumnTypeT,
			DefaultValueS: *l.DefaultValueS,
			DefaultValueT: *l.DefaultValueT,
		})
	}
	for _, l := range *req.StructMigrateRule.ColumnStructRules {
		columnLevelRules = append(columnLevelRules, &pb.ColumnStructRule{
			SchemaNameS:   *l.SchemaNameS,
			TableNameS:    *l.TableNameS,
			ColumnNameS:   *l.ColumnNameS,
			ColumnTypeS:   *l.ColumnTypeS,
			ColumnTypeT:   *l.ColumnTypeT,
			DefaultValueS: *l.DefaultValueS,
			DefaultValueT: *l.DefaultValueT,
		})
	}
	for _, l := range *req.StructMigrateRule.TableAttrsRules {
		tableAttrsRules = append(tableAttrsRules, &pb.TableAttrsRule{
			SchemaNameS: *l.SchemaNameS,
			TableNamesS: *l.TableNamesS,
			TableAttrsT: *l.TableAttrsT,
		})
	}
	resp, err := s.UpsertStructMigrateTask(ctx, &pb.UpsertStructMigrateTaskRequest{
		TaskName:        *req.TaskName,
		DatasourceNameS: *req.DatasourceNameS,
		DatasourceNameT: *req.DatasourceNameT,
		Comment:         *req.Comment,
		CaseFieldRule: &pb.CaseFieldRule{
			CaseFieldRuleS: *req.CaseFieldRule.CaseFieldRuleS,
			CaseFieldRuleT: *req.CaseFieldRule.CaseFieldRuleT,
		},
		SchemaRouteRule: migrateSchemaRs,
		StructMigrateParam: &pb.StructMigrateParam{
			MigrateThread:    *req.StructMigrateParam.MigrateThread,
			DirectWrite:      *req.StructMigrateParam.DirectWrite,
			CreateIfNotExist: *req.StructMigrateParam.CreateIfNotExist,
			OutputDir:        *req.StructMigrateParam.OutputDir,
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
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) deleteStructMigrateTask(ctx context.Context, req openapi.APIDeleteStructMigrateJSONRequestBody) (string, error) {
	resp, err := s.DeleteStructMigrateTask(ctx, &pb.DeleteStructMigrateTaskRequest{TaskName: *req.Param})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) listStructMigrateTask(ctx context.Context, req openapi.APIListStructMigrateJSONRequestBody) (string, error) {
	resp, err := s.ShowStructMigrateTask(ctx, &pb.ShowStructMigrateTaskRequest{
		TaskName: *req.Param,
		Page:     *req.Page,
		PageSize: *req.PageSize,
	})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) upsertDataMigrateTask(ctx context.Context, req openapi.APIPutDataMigrateJSONRequestBody) (string, error) {
	var (
		migrateSchemaRs *pb.SchemaRouteRule
		tableRoutes     []*pb.TableRouteRule
	)

	for _, t := range *req.SchemaRouteRule.TableRouteRules {
		tableRoutes = append(tableRoutes, &pb.TableRouteRule{
			TableNameS:       *t.TableNameS,
			TableNameT:       *t.TableNameT,
			ColumnRouteRules: *t.ColumnRouteRules,
		})
	}
	migrateSchemaRs = &pb.SchemaRouteRule{
		SchemaNameS:     *req.SchemaRouteRule.SchemaNameS,
		SchemaNameT:     *req.SchemaRouteRule.SchemaNameT,
		IncludeTableS:   *req.SchemaRouteRule.IncludeTableS,
		ExcludeTableS:   *req.SchemaRouteRule.ExcludeTableS,
		TableRouteRules: tableRoutes,
	}

	resp, err := s.UpsertDataMigrateTask(ctx, &pb.UpsertDataMigrateTaskRequest{
		TaskName:        *req.TaskName,
		DatasourceNameS: *req.DatasourceNameS,
		DatasourceNameT: *req.DatasourceNameT,
		Comment:         *req.Comment,
		CaseFieldRule: &pb.CaseFieldRule{
			CaseFieldRuleS: *req.CaseFieldRule.CaseFieldRuleS,
			CaseFieldRuleT: *req.CaseFieldRule.CaseFieldRuleT,
		},
		SchemaRouteRule: migrateSchemaRs,
		DataMigrateParam: &pb.DataMigrateParam{
			TableThread:          *req.DataMigrateParam.TableThread,
			BatchSize:            *req.DataMigrateParam.BatchSize,
			ChunkSize:            *req.DataMigrateParam.ChunkSize,
			SqlThreadS:           *req.DataMigrateParam.SqlThreadS,
			SqlHintS:             *req.DataMigrateParam.SqlHintS,
			SqlThreadT:           *req.DataMigrateParam.SqlThreadT,
			SqlHintT:             *req.DataMigrateParam.SqlHintT,
			CallTimeout:          *req.DataMigrateParam.CallTimeout,
			EnableCheckpoint:     *req.DataMigrateParam.EnableCheckpoint,
			EnableConsistentRead: *req.DataMigrateParam.EnableConsistentRead,
		},
	})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) deleteDataMigrateTask(ctx context.Context, req openapi.APIDeleteDataMigrateJSONRequestBody) (string, error) {
	resp, err := s.DeleteDataMigrateTask(ctx, &pb.DeleteDataMigrateTaskRequest{TaskName: *req.Param})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) listDataMigrateTask(ctx context.Context, req openapi.APIListDataMigrateJSONRequestBody) (string, error) {
	resp, err := s.ShowDataMigrateTask(ctx, &pb.ShowDataMigrateTaskRequest{
		TaskName: *req.Param,
		Page:     *req.Page,
		PageSize: *req.PageSize,
	})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) upsertSqlMigrateTask(ctx context.Context, req openapi.APIPutSqlMigrateJSONRequestBody) (string, error) {
	var (
		sqlRoutes []*pb.SqlRouteRule
	)

	for _, t := range *req.SqlRouteRules {
		sqlRoutes = append(sqlRoutes, &pb.SqlRouteRule{
			SqlQueryS:        *t.SqlQueryS,
			SchemaNameT:      *t.SchemaNameT,
			TableNameT:       *t.TableNameT,
			SqlHintT:         *t.SqlHintT,
			ColumnRouteRules: *t.ColumnRouteRules,
		})
	}

	resp, err := s.UpsertSqlMigrateTask(ctx, &pb.UpsertSqlMigrateTaskRequest{
		TaskName:        *req.TaskName,
		DatasourceNameS: *req.DatasourceNameS,
		DatasourceNameT: *req.DatasourceNameT,
		Comment:         *req.Comment,
		SqlRouteRule:    sqlRoutes,
		SqlMigrateParam: &pb.SqlMigrateParam{
			BatchSize:            *req.SqlMigrateParam.BatchSize,
			SqlThreadS:           *req.SqlMigrateParam.SqlThreadS,
			SqlThreadT:           *req.SqlMigrateParam.SqlThreadT,
			SqlHintT:             *req.SqlMigrateParam.SqlHintT,
			CallTimeout:          *req.SqlMigrateParam.CallTimeout,
			EnableConsistentRead: *req.SqlMigrateParam.EnableConsistentRead,
		},
	})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) deleteSqlMigrateTask(ctx context.Context, req openapi.APIDeleteSqlMigrateJSONRequestBody) (string, error) {
	resp, err := s.DeleteSqlMigrateTask(ctx, &pb.DeleteSqlMigrateTaskRequest{TaskName: *req.Param})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) listSqlMigrateTask(ctx context.Context, req openapi.APIListSqlMigrateJSONRequestBody) (string, error) {
	resp, err := s.ShowSqlMigrateTask(ctx, &pb.ShowSqlMigrateTaskRequest{
		TaskName: *req.Param,
		Page:     *req.Page,
		PageSize: *req.PageSize,
	})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) operateTask(ctx context.Context, req openapi.APIPostTaskJSONRequestBody) (string, error) {
	resp, err := s.OperateTask(ctx, &pb.OperateTaskRequest{
		Operate:  *req.Operate,
		TaskName: *req.TaskName,
		Express:  *req.Express,
	})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}
