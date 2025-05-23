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
			InitThread:    *req.InitThread,
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
			SessionParams:  *r.SessionParams,
			DbName:         *r.DbName,
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

func (s *Server) upsertAssessMigrateTask(ctx context.Context, req openapi.APIPutAssessMigrateJSONRequestBody) (string, error) {
	resp, err := s.UpsertAssessMigrateTask(ctx, &pb.UpsertAssessMigrateTaskRequest{
		TaskName:        *req.TaskName,
		DatasourceNameS: *req.DatasourceNameS,
		DatasourceNameT: *req.DatasourceNameT,
		Comment:         *req.Comment,
		AssessMigrateParam: &pb.AssessMigrateParam{
			CaseFieldRuleS: *req.AssessMigrateParam.CaseFieldRuleS,
			SchemaNameS:    *req.AssessMigrateParam.SchemaNameS,
			CallTimeout:    *req.AssessMigrateParam.CallTimeout,
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

func (s *Server) deleteAssessMigrateTask(ctx context.Context, req openapi.APIDeleteAssessMigrateJSONRequestBody) (string, error) {
	resp, err := s.DeleteAssessMigrateTask(ctx, &pb.DeleteAssessMigrateTaskRequest{TaskName: *req.Param})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) listAssessMigrateTask(ctx context.Context, req openapi.APIListAssessMigrateJSONRequestBody) (string, error) {
	resp, err := s.ShowAssessMigrateTask(ctx, &pb.ShowAssessMigrateTaskRequest{
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
		SchemaNameS:      *req.SchemaRouteRule.SchemaNameS,
		SchemaNameT:      *req.SchemaRouteRule.SchemaNameT,
		IncludeTableS:    *req.SchemaRouteRule.IncludeTableS,
		ExcludeTableS:    *req.SchemaRouteRule.ExcludeTableS,
		IncludeSequenceS: *req.SchemaRouteRule.IncludeSequenceS,
		ExcludeSequenceS: *req.SchemaRouteRule.ExcludeSequenceS,
		TableRouteRules:  tableRoutes,
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
			MigrateThread:      *req.StructMigrateParam.MigrateThread,
			CreateIfNotExist:   *req.StructMigrateParam.CreateIfNotExist,
			EnableDirectCreate: *req.StructMigrateParam.EnableDirectCreate,
			EnableCheckpoint:   *req.StructMigrateParam.EnableCheckpoint,
			CallTimeout:        *req.StructMigrateParam.CallTimeout,
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

func (s *Server) upsertStructCompareTask(ctx context.Context, req openapi.APIPutStructCompareJSONRequestBody) (string, error) {
	var (
		taskLevelRules   []*pb.TaskStructRule
		schemaLevelRules []*pb.SchemaStructRule
		tableLevelRules  []*pb.TableStructRule
		columnLevelRules []*pb.ColumnStructRule
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

	for _, l := range *req.StructCompareRule.TaskStructRules {
		taskLevelRules = append(taskLevelRules, &pb.TaskStructRule{
			ColumnTypeS:   *l.ColumnTypeS,
			ColumnTypeT:   *l.ColumnTypeT,
			DefaultValueS: *l.DefaultValueS,
			DefaultValueT: *l.DefaultValueT,
		})
	}
	for _, l := range *req.StructCompareRule.SchemaStructRules {
		schemaLevelRules = append(schemaLevelRules, &pb.SchemaStructRule{
			SchemaNameS:   *l.SchemaNameS,
			ColumnTypeS:   *l.ColumnTypeS,
			ColumnTypeT:   *l.ColumnTypeT,
			DefaultValueS: *l.DefaultValueS,
			DefaultValueT: *l.DefaultValueT,
		})
	}
	for _, l := range *req.StructCompareRule.TableStructRules {
		tableLevelRules = append(tableLevelRules, &pb.TableStructRule{
			SchemaNameS:   *l.SchemaNameS,
			TableNameS:    *l.TableNameS,
			ColumnTypeS:   *l.ColumnTypeS,
			ColumnTypeT:   *l.ColumnTypeT,
			DefaultValueS: *l.DefaultValueS,
			DefaultValueT: *l.DefaultValueT,
		})
	}
	for _, l := range *req.StructCompareRule.ColumnStructRules {
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

	resp, err := s.UpsertStructCompareTask(ctx, &pb.UpsertStructCompareTaskRequest{
		TaskName:        *req.TaskName,
		DatasourceNameS: *req.DatasourceNameS,
		DatasourceNameT: *req.DatasourceNameT,
		Comment:         *req.Comment,
		CaseFieldRule: &pb.CaseFieldRule{
			CaseFieldRuleS: *req.CaseFieldRule.CaseFieldRuleS,
			CaseFieldRuleT: *req.CaseFieldRule.CaseFieldRuleT,
		},
		SchemaRouteRule: migrateSchemaRs,
		StructCompareParam: &pb.StructCompareParam{
			CompareThread:     *req.StructCompareParam.CompareThread,
			EnableCheckpoint:  *req.StructCompareParam.EnableCheckpoint,
			CallTimeout:       *req.StructCompareParam.CallTimeout,
			IgnoreCaseCompare: *req.StructCompareParam.IgnoreCaseCompare,
		},
		StructCompareRule: &pb.StructCompareRule{
			TaskStructRules:   taskLevelRules,
			SchemaStructRules: schemaLevelRules,
			TableStructRules:  tableLevelRules,
			ColumnStructRules: columnLevelRules,
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

func (s *Server) deleteStructCompareTask(ctx context.Context, req openapi.APIDeleteStructCompareJSONRequestBody) (string, error) {
	resp, err := s.DeleteStructCompareTask(ctx, &pb.DeleteStructCompareTaskRequest{TaskName: *req.Param})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) listStructCompareTask(ctx context.Context, req openapi.APIListStructCompareJSONRequestBody) (string, error) {
	resp, err := s.ShowStructCompareTask(ctx, &pb.ShowStructCompareTaskRequest{
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

func (s *Server) upsertStmtMigrateTask(ctx context.Context, req openapi.APIPutStmtMigrateJSONRequestBody) (string, error) {
	var (
		migrateSchemaRs  *pb.SchemaRouteRule
		tableRoutes      []*pb.TableRouteRule
		dataMigrateRules []*pb.DataMigrateRule
	)

	for _, t := range *req.SchemaRouteRule.TableRouteRules {
		tableRoutes = append(tableRoutes, &pb.TableRouteRule{
			TableNameS:       *t.TableNameS,
			TableNameT:       *t.TableNameT,
			ColumnRouteRules: *t.ColumnRouteRules,
		})
	}
	for _, t := range *req.DataMigrateRules {
		dataMigrateRules = append(dataMigrateRules, &pb.DataMigrateRule{
			TableNameS:          *t.TableNameS,
			EnableChunkStrategy: *t.EnableChunkStrategy,
			WhereRange:          *t.WhereRange,
			SqlHintS:            *t.SqlHintS,
		})
	}
	migrateSchemaRs = &pb.SchemaRouteRule{
		SchemaNameS:     *req.SchemaRouteRule.SchemaNameS,
		SchemaNameT:     *req.SchemaRouteRule.SchemaNameT,
		IncludeTableS:   *req.SchemaRouteRule.IncludeTableS,
		ExcludeTableS:   *req.SchemaRouteRule.ExcludeTableS,
		TableRouteRules: tableRoutes,
	}

	resp, err := s.UpsertStmtMigrateTask(ctx, &pb.UpsertStmtMigrateTaskRequest{
		TaskName:        *req.TaskName,
		DatasourceNameS: *req.DatasourceNameS,
		DatasourceNameT: *req.DatasourceNameT,
		Comment:         *req.Comment,
		CaseFieldRule: &pb.CaseFieldRule{
			CaseFieldRuleS: *req.CaseFieldRule.CaseFieldRuleS,
			CaseFieldRuleT: *req.CaseFieldRule.CaseFieldRuleT,
		},
		SchemaRouteRule:  migrateSchemaRs,
		DataMigrateRules: dataMigrateRules,
		StatementMigrateParam: &pb.StatementMigrateParam{
			TableThread:          *req.StatementMigrateParam.TableThread,
			WriteThread:          *req.StatementMigrateParam.WriteThread,
			BatchSize:            *req.StatementMigrateParam.BatchSize,
			ChunkSize:            *req.StatementMigrateParam.ChunkSize,
			SqlThreadS:           *req.StatementMigrateParam.SqlThreadS,
			SqlHintS:             *req.StatementMigrateParam.SqlHintS,
			SqlThreadT:           *req.StatementMigrateParam.SqlThreadT,
			SqlHintT:             *req.StatementMigrateParam.SqlHintT,
			CallTimeout:          *req.StatementMigrateParam.CallTimeout,
			EnableCheckpoint:     *req.StatementMigrateParam.EnableCheckpoint,
			EnableConsistentRead: *req.StatementMigrateParam.EnableConsistentRead,
			EnableSafeMode:       *req.StatementMigrateParam.EnableSafeMode,
			EnablePrepareStmt:    *req.StatementMigrateParam.EnablePrepareStmt,
			GarbledCharReplace:   *req.StatementMigrateParam.GarbledCharReplace,
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

func (s *Server) deleteStmtMigrateTask(ctx context.Context, req openapi.APIDeleteStmtMigrateJSONRequestBody) (string, error) {
	resp, err := s.DeleteStmtMigrateTask(ctx, &pb.DeleteStmtMigrateTaskRequest{TaskName: *req.Param})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) listStmtMigrateTask(ctx context.Context, req openapi.APIListStmtMigrateJSONRequestBody) (string, error) {
	resp, err := s.ShowStmtMigrateTask(ctx, &pb.ShowStmtMigrateTaskRequest{
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

func (s *Server) upsertDataCompareTask(ctx context.Context, req openapi.APIPutDataCompareJSONRequestBody) (string, error) {
	var (
		migrateSchemaRs *pb.SchemaRouteRule
		tableRoutes     []*pb.TableRouteRule
		compareRules    []*pb.DataCompareRule
	)

	for _, t := range *req.SchemaRouteRule.TableRouteRules {
		tableRoutes = append(tableRoutes, &pb.TableRouteRule{
			TableNameS:       *t.TableNameS,
			TableNameT:       *t.TableNameT,
			ColumnRouteRules: *t.ColumnRouteRules,
		})
	}

	for _, r := range *req.DataCompareRules {
		compareRules = append(compareRules, &pb.DataCompareRule{
			TableNameS:             *r.TableNameS,
			CompareConditionField:  *r.CompareConditionField,
			CompareConditionRangeS: *r.CompareConditionRangeS,
			CompareConditionRangeT: *r.CompareConditionRangeT,
			IgnoreConditionFields:  *r.IgnoreConditionFields,
			IgnoreSelectFields:     *r.IgnoreSelectFields,
			SqlHintS:               *r.SqlHintS,
			SqlHintT:               *r.SqlHintT,
		})
	}

	migrateSchemaRs = &pb.SchemaRouteRule{
		SchemaNameS:     *req.SchemaRouteRule.SchemaNameS,
		SchemaNameT:     *req.SchemaRouteRule.SchemaNameT,
		IncludeTableS:   *req.SchemaRouteRule.IncludeTableS,
		ExcludeTableS:   *req.SchemaRouteRule.ExcludeTableS,
		TableRouteRules: tableRoutes,
	}

	resp, err := s.UpsertDataCompareTask(ctx, &pb.UpsertDataCompareTaskRequest{
		TaskName:        *req.TaskName,
		DatasourceNameS: *req.DatasourceNameS,
		DatasourceNameT: *req.DatasourceNameT,
		Comment:         *req.Comment,
		CaseFieldRule: &pb.CaseFieldRule{
			CaseFieldRuleS: *req.CaseFieldRule.CaseFieldRuleS,
			CaseFieldRuleT: *req.CaseFieldRule.CaseFieldRuleT,
		},
		SchemaRouteRule:  migrateSchemaRs,
		DataCompareRules: compareRules,
		DataCompareParam: &pb.DataCompareParam{
			TableThread:            *req.DataCompareParam.TableThread,
			BatchSize:              *req.DataCompareParam.BatchSize,
			SqlThread:              *req.DataCompareParam.SqlThread,
			WriteThread:            *req.DataCompareParam.WriteThread,
			SqlHintS:               *req.DataCompareParam.SqlHintS,
			SqlHintT:               *req.DataCompareParam.SqlHintT,
			CallTimeout:            *req.DataCompareParam.CallTimeout,
			EnableCheckpoint:       *req.DataCompareParam.EnableCheckpoint,
			EnableConsistentRead:   *req.DataCompareParam.EnableConsistentRead,
			ConsistentReadPointS:   *req.DataCompareParam.ConsistentReadPointS,
			ConsistentReadPointT:   *req.DataCompareParam.ConsistentReadPointT,
			ChunkSize:              *req.DataCompareParam.ChunkSize,
			IgnoreConditionFields:  *req.DataCompareParam.IgnoreConditionFields,
			RepairStmtFlow:         *req.DataCompareParam.RepairStmtFlow,
			EnableCollationSetting: *req.DataCompareParam.EnableCollationSetting,
			DisableMd5Checksum:     *req.DataCompareParam.DisableMd5Checksum,
			Separator:              *req.DataCompareParam.Separator,
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

func (s *Server) deleteDataCompareTask(ctx context.Context, req openapi.APIDeleteDataCompareJSONRequestBody) (string, error) {
	resp, err := s.DeleteDataCompareTask(ctx, &pb.DeleteDataCompareTaskRequest{TaskName: *req.Param})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) listDataCompareTask(ctx context.Context, req openapi.APIListDataCompareJSONRequestBody) (string, error) {
	resp, err := s.ShowDataCompareTask(ctx, &pb.ShowDataCompareTaskRequest{
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

func (s *Server) upsertCsvMigrateTask(ctx context.Context, req openapi.APIPutCsvMigrateJSONRequestBody) (string, error) {
	var (
		migrateSchemaRs  *pb.SchemaRouteRule
		tableRoutes      []*pb.TableRouteRule
		dataMigrateRules []*pb.DataMigrateRule
	)

	for _, t := range *req.SchemaRouteRule.TableRouteRules {
		tableRoutes = append(tableRoutes, &pb.TableRouteRule{
			TableNameS:       *t.TableNameS,
			TableNameT:       *t.TableNameT,
			ColumnRouteRules: *t.ColumnRouteRules,
		})
	}
	for _, t := range *req.DataMigrateRules {
		dataMigrateRules = append(dataMigrateRules, &pb.DataMigrateRule{
			TableNameS:          *t.TableNameS,
			EnableChunkStrategy: *t.EnableChunkStrategy,
			WhereRange:          *t.WhereRange,
			SqlHintS:            *t.SqlHintS,
		})
	}
	migrateSchemaRs = &pb.SchemaRouteRule{
		SchemaNameS:     *req.SchemaRouteRule.SchemaNameS,
		SchemaNameT:     *req.SchemaRouteRule.SchemaNameT,
		IncludeTableS:   *req.SchemaRouteRule.IncludeTableS,
		ExcludeTableS:   *req.SchemaRouteRule.ExcludeTableS,
		TableRouteRules: tableRoutes,
	}

	resp, err := s.UpsertCsvMigrateTask(ctx, &pb.UpsertCsvMigrateTaskRequest{
		TaskName:        *req.TaskName,
		DatasourceNameS: *req.DatasourceNameS,
		DatasourceNameT: *req.DatasourceNameT,
		Comment:         *req.Comment,
		CaseFieldRule: &pb.CaseFieldRule{
			CaseFieldRuleS: *req.CaseFieldRule.CaseFieldRuleS,
			CaseFieldRuleT: *req.CaseFieldRule.CaseFieldRuleT,
		},
		SchemaRouteRule:  migrateSchemaRs,
		DataMigrateRules: dataMigrateRules,
		CsvMigrateParam: &pb.CsvMigrateParam{
			TableThread:          *req.CsvMigrateParam.TableThread,
			WriteThread:          *req.CsvMigrateParam.WriteThread,
			BatchSize:            *req.CsvMigrateParam.BatchSize,
			DiskUsageFactor:      *req.CsvMigrateParam.DiskUsageFactor,
			Header:               *req.CsvMigrateParam.Header,
			Separator:            *req.CsvMigrateParam.Separator,
			Terminator:           *req.CsvMigrateParam.Terminator,
			DataCharsetT:         *req.CsvMigrateParam.DataCharsetT,
			Delimiter:            *req.CsvMigrateParam.Delimiter,
			NullValue:            *req.CsvMigrateParam.NullValue,
			EscapeBackslash:      *req.CsvMigrateParam.EscapeBackslash,
			ChunkSize:            *req.CsvMigrateParam.ChunkSize,
			OutputDir:            strings.TrimSpace(*req.CsvMigrateParam.OutputDir),
			SqlThreadS:           *req.CsvMigrateParam.SqlThreadS,
			SqlHintS:             *req.CsvMigrateParam.SqlHintS,
			CallTimeout:          *req.CsvMigrateParam.CallTimeout,
			EnableCheckpoint:     *req.CsvMigrateParam.EnableCheckpoint,
			EnableConsistentRead: *req.CsvMigrateParam.EnableConsistentRead,
			EnableImportFeature:  *req.CsvMigrateParam.EnableImportFeature,
			CsvImportParams:      *req.CsvMigrateParam.CsvImportParams,
			GarbledCharReplace:   *req.CsvMigrateParam.GarbledCharReplace,
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

func (s *Server) deleteCsvMigrateTask(ctx context.Context, req openapi.APIDeleteCsvMigrateJSONRequestBody) (string, error) {
	resp, err := s.DeleteCsvMigrateTask(ctx, &pb.DeleteCsvMigrateTaskRequest{TaskName: *req.Param})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) listCsvMigrateTask(ctx context.Context, req openapi.APIListCsvMigrateJSONRequestBody) (string, error) {
	resp, err := s.ShowCsvMigrateTask(ctx, &pb.ShowCsvMigrateTaskRequest{
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
		sqlRoutes []*pb.SqlMigrateRule
	)

	for _, t := range *req.SqlMigrateRules {
		sqlRoutes = append(sqlRoutes, &pb.SqlMigrateRule{
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
		CaseFieldRule: &pb.CaseFieldRule{
			CaseFieldRuleS: *req.CaseFieldRule.CaseFieldRuleS,
			CaseFieldRuleT: *req.CaseFieldRule.CaseFieldRuleT,
		},
		SqlMigrateRules: sqlRoutes,
		SqlMigrateParam: &pb.SqlMigrateParam{
			WriteThread:          *req.SqlMigrateParam.WriteThread,
			BatchSize:            *req.SqlMigrateParam.BatchSize,
			SqlThreadS:           *req.SqlMigrateParam.SqlThreadS,
			SqlThreadT:           *req.SqlMigrateParam.SqlThreadT,
			SqlHintT:             *req.SqlMigrateParam.SqlHintT,
			CallTimeout:          *req.SqlMigrateParam.CallTimeout,
			EnableConsistentRead: *req.SqlMigrateParam.EnableConsistentRead,
			EnableSafeMode:       *req.SqlMigrateParam.EnableSafeMode,
			EnableCheckpoint:     *req.SqlMigrateParam.EnableCheckpoint,
			EnablePrepareStmt:    *req.SqlMigrateParam.EnablePrepareStmt,
			GarbledCharReplace:   *req.SqlMigrateParam.GarbledCharReplace,
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

func (s *Server) upsertDataScanTask(ctx context.Context, req openapi.APIPutDataScanJSONRequestBody) (string, error) {
	var (
		dataScanRules []*pb.DataScanRule
	)

	for _, r := range *req.DataScanRules {
		dataScanRules = append(dataScanRules, &pb.DataScanRule{
			TableNameS:       *r.TableNameS,
			SqlHintS:         *r.SqlHintS,
			TableSamplerateS: *r.TableSamplerateS,
		})
	}

	migrateSchemaRs := &pb.SchemaRouteRule{
		SchemaNameS:   *req.SchemaRouteRule.SchemaNameS,
		SchemaNameT:   *req.SchemaRouteRule.SchemaNameT,
		IncludeTableS: *req.SchemaRouteRule.IncludeTableS,
		ExcludeTableS: *req.SchemaRouteRule.ExcludeTableS,
	}

	resp, err := s.UpsertDataScanTask(ctx, &pb.UpsertDataScanTaskRequest{
		TaskName:        *req.TaskName,
		DatasourceNameS: *req.DatasourceNameS,
		DatasourceNameT: *req.DatasourceNameT,
		Comment:         *req.Comment,
		CaseFieldRule:   &pb.CaseFieldRule{CaseFieldRuleS: *req.CaseFieldRule.CaseFieldRuleS},
		SchemaRouteRule: migrateSchemaRs,
		DataScanRules:   dataScanRules,
		DataScanParam: &pb.DataScanParam{
			TableThread:          *req.DataScanParam.TableThread,
			WriteThread:          *req.DataScanParam.WriteThread,
			BatchSize:            *req.DataScanParam.BatchSize,
			ChunkSize:            *req.DataScanParam.ChunkSize,
			SqlThreadS:           *req.DataScanParam.SqlThreadS,
			SqlHintS:             *req.DataScanParam.SqlHintS,
			CallTimeout:          *req.DataScanParam.CallTimeout,
			TableSamplerateS:     *req.DataScanParam.TableSamplerateS,
			EnableCheckpoint:     *req.DataScanParam.EnableCheckpoint,
			EnableConsistentRead: *req.DataScanParam.EnableConsistentRead,
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

func (s *Server) deleteDataScanTask(ctx context.Context, req openapi.APIDeleteDataScanJSONRequestBody) (string, error) {
	resp, err := s.DeleteDataScanTask(ctx, &pb.DeleteDataScanTaskRequest{TaskName: *req.Param})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) listDataScanTask(ctx context.Context, req openapi.APIListDataScanJSONRequestBody) (string, error) {
	resp, err := s.ShowDataScanTask(ctx, &pb.ShowDataScanTaskRequest{
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

func (s *Server) upsertCdcConsumeTask(ctx context.Context, req openapi.APIPutCdcConsumeJSONRequestBody) (string, error) {
	migrateSchemaRs := &pb.SchemaRouteRule{
		SchemaNameS:   *req.SchemaRouteRule.SchemaNameS,
		SchemaNameT:   *req.SchemaRouteRule.SchemaNameT,
		IncludeTableS: *req.SchemaRouteRule.IncludeTableS,
		ExcludeTableS: *req.SchemaRouteRule.ExcludeTableS,
	}

	resp, err := s.UpsertCdcConsumeTask(ctx, &pb.UpsertCdcConsumeTaskRequest{
		TaskName:        *req.TaskName,
		DatasourceNameS: *req.DatasourceNameS,
		DatasourceNameT: *req.DatasourceNameT,
		Comment:         *req.Comment,
		CaseFieldRule: &pb.CaseFieldRule{
			CaseFieldRuleS: *req.CaseFieldRule.CaseFieldRuleS,
			CaseFieldRuleT: *req.CaseFieldRule.CaseFieldRuleT,
		},
		SchemaRouteRule: migrateSchemaRs,
		CdcConsumeParam: &pb.CdcConsumeParam{
			ServerAddress:         *req.CdcConsumeParam.ServerAddress,
			SubscribeTopic:        *req.CdcConsumeParam.SubscribeTopic,
			TableThread:           *req.CdcConsumeParam.TableThread,
			MessageCompression:    *req.CdcConsumeParam.MessageCompression,
			IdleResolvedThreshold: *req.CdcConsumeParam.IdleResolvedThreshold,
			CallTimeout:           *req.CdcConsumeParam.CallTimeout,
			EnableCheckpoint:      *req.CdcConsumeParam.EnableCheckpoint,
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

func (s *Server) deleteCdcConsumeTask(ctx context.Context, req openapi.APIDeleteCdcConsumeJSONRequestBody) (string, error) {
	resp, err := s.DeleteCdcConsumeTask(ctx, &pb.DeleteCdcConsumeTaskRequest{TaskName: *req.Param})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}

func (s *Server) listCdcConsumeTask(ctx context.Context, req openapi.APIListCdcConsumeJSONRequestBody) (string, error) {
	resp, err := s.ShowCdcConsumeTask(ctx, &pb.ShowCdcConsumeTaskRequest{
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
		HostIP:   *req.HostIP,
	})
	if err != nil {
		return "", err
	}
	if strings.EqualFold(resp.Response.Result, openapi.ResponseResultStatusSuccess) {
		return resp.Response.Message, nil
	}
	return "", errors.New(resp.Response.Message)
}
