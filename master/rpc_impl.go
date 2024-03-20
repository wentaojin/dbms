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

	"github.com/wentaojin/dbms/openapi"
	"github.com/wentaojin/dbms/service"

	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/proto/pb"
)

// UpsertDatabase implements MasterServer.UpsertDatabase
func (s *Server) UpsertDatabase(ctx context.Context, req *pb.UpsertDatabaseRequest) (*pb.UpsertDatabaseResponse, error) {
	upsertMsg, err := service.UpsertDatabase(s.etcdClient, constant.DefaultMasterDatabaseDBMSKey, req)
	if err != nil {
		return &pb.UpsertDatabaseResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}

	return &pb.UpsertDatabaseResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: upsertMsg,
	}}, nil
}

func (s *Server) DeleteDatabase(ctx context.Context, req *pb.DeleteDatabaseRequest) (*pb.DeleteDatabaseResponse, error) {
	err := service.DeleteDatabase(s.etcdClient, constant.DefaultMasterDatabaseDBMSKey)
	if err != nil {
		return &pb.DeleteDatabaseResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.DeleteDatabaseResponse{Response: &pb.Response{
		Result: openapi.ResponseResultStatusSuccess,
	}}, nil
}

func (s *Server) ShowDatabase(ctx context.Context, req *pb.ShowDatabaseRequest) (*pb.ShowDatabaseResponse, error) {
	showMsg, err := service.ShowDatabase(s.etcdClient, constant.DefaultMasterDatabaseDBMSKey)
	if err != nil {
		return &pb.ShowDatabaseResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.ShowDatabaseResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: showMsg,
	}}, nil
}

// UpsertDatasource implements MasterServer.UpsertDatasource
func (s *Server) UpsertDatasource(ctx context.Context, req *pb.UpsertDatasourceRequest) (*pb.UpsertDatasourceResponse, error) {
	createMsg, err := service.UpsertDatasource(ctx, req)
	if err != nil {
		return &pb.UpsertDatasourceResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.UpsertDatasourceResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: createMsg,
	}}, nil
}

func (s *Server) DeleteDatasource(ctx context.Context, req *pb.DeleteDatasourceRequest) (*pb.DeleteDatasourceResponse, error) {
	err := service.DeleteDatasource(ctx, req)
	if err != nil {
		return &pb.DeleteDatasourceResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.DeleteDatasourceResponse{Response: &pb.Response{
		Result: openapi.ResponseResultStatusSuccess,
	}}, nil
}

func (s *Server) ShowDatasource(ctx context.Context, req *pb.ShowDatasourceRequest) (*pb.ShowDatasourceResponse, error) {
	listMsg, err := service.ShowDatasource(ctx, req)
	if err != nil {
		return &pb.ShowDatasourceResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.ShowDatasourceResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: listMsg,
	}}, nil
}

func (s *Server) UpsertStructMigrateTask(ctx context.Context, req *pb.UpsertStructMigrateTaskRequest) (*pb.UpsertStructMigrateTaskResponse, error) {
	showMsg, err := service.UpsertStructMigrateTask(ctx, req)
	if err != nil {
		return &pb.UpsertStructMigrateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.UpsertStructMigrateTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: showMsg,
	}}, nil
}

func (s *Server) DeleteStructMigrateTask(ctx context.Context, req *pb.DeleteStructMigrateTaskRequest) (*pb.DeleteStructMigrateTaskResponse, error) {
	delMsg, err := service.DeleteStructMigrateTask(ctx, req)
	if err != nil {
		return &pb.DeleteStructMigrateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.DeleteStructMigrateTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: delMsg,
	}}, nil
}

func (s *Server) ShowStructMigrateTask(ctx context.Context, req *pb.ShowStructMigrateTaskRequest) (*pb.ShowStructMigrateTaskResponse, error) {
	delMsg, err := service.ShowStructMigrateTask(ctx, req)
	if err != nil {
		return &pb.ShowStructMigrateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.ShowStructMigrateTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: delMsg,
	}}, nil
}

func (s *Server) UpsertStmtMigrateTask(ctx context.Context, req *pb.UpsertStmtMigrateTaskRequest) (*pb.UpsertStmtMigrateTaskResponse, error) {
	showMsg, err := service.UpsertStmtMigrateTask(ctx, req)
	if err != nil {
		return &pb.UpsertStmtMigrateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.UpsertStmtMigrateTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: showMsg,
	}}, nil
}

func (s *Server) DeleteStmtMigrateTask(ctx context.Context, req *pb.DeleteStmtMigrateTaskRequest) (*pb.DeleteStmtMigrateTaskResponse, error) {
	delMsg, err := service.DeleteStmtMigrateTask(ctx, req)
	if err != nil {
		return &pb.DeleteStmtMigrateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.DeleteStmtMigrateTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: delMsg,
	}}, nil
}

func (s *Server) ShowStmtMigrateTask(ctx context.Context, req *pb.ShowStmtMigrateTaskRequest) (*pb.ShowStmtMigrateTaskResponse, error) {
	delMsg, err := service.ShowStmtMigrateTask(ctx, req)
	if err != nil {
		return &pb.ShowStmtMigrateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.ShowStmtMigrateTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: delMsg,
	}}, nil
}

func (s *Server) UpsertCsvMigrateTask(ctx context.Context, req *pb.UpsertCsvMigrateTaskRequest) (*pb.UpsertCsvMigrateTaskResponse, error) {
	showMsg, err := service.UpsertCsvMigrateTask(ctx, req)
	if err != nil {
		return &pb.UpsertCsvMigrateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.UpsertCsvMigrateTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: showMsg,
	}}, nil
}

func (s *Server) DeleteCsvMigrateTask(ctx context.Context, req *pb.DeleteCsvMigrateTaskRequest) (*pb.DeleteCsvMigrateTaskResponse, error) {
	delMsg, err := service.DeleteCsvMigrateTask(ctx, req)
	if err != nil {
		return &pb.DeleteCsvMigrateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.DeleteCsvMigrateTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: delMsg,
	}}, nil
}

func (s *Server) ShowCsvMigrateTask(ctx context.Context, req *pb.ShowCsvMigrateTaskRequest) (*pb.ShowCsvMigrateTaskResponse, error) {
	delMsg, err := service.ShowCsvMigrateTask(ctx, req)
	if err != nil {
		return &pb.ShowCsvMigrateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.ShowCsvMigrateTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: delMsg,
	}}, nil
}

func (s *Server) UpsertSqlMigrateTask(ctx context.Context, req *pb.UpsertSqlMigrateTaskRequest) (*pb.UpsertSqlMigrateTaskResponse, error) {
	showMsg, err := service.UpsertSqlMigrateTask(ctx, req)
	if err != nil {
		return &pb.UpsertSqlMigrateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.UpsertSqlMigrateTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: showMsg,
	}}, nil
}

func (s *Server) DeleteSqlMigrateTask(ctx context.Context, req *pb.DeleteSqlMigrateTaskRequest) (*pb.DeleteSqlMigrateTaskResponse, error) {
	delMsg, err := service.DeleteSqlMigrateTask(ctx, req)
	if err != nil {
		return &pb.DeleteSqlMigrateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.DeleteSqlMigrateTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: delMsg,
	}}, nil
}

func (s *Server) ShowSqlMigrateTask(ctx context.Context, req *pb.ShowSqlMigrateTaskRequest) (*pb.ShowSqlMigrateTaskResponse, error) {
	delMsg, err := service.ShowSqlMigrateTask(ctx, req)
	if err != nil {
		return &pb.ShowSqlMigrateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.ShowSqlMigrateTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: delMsg,
	}}, nil
}

func (s *Server) UpsertDataCompareTask(ctx context.Context, req *pb.UpsertDataCompareTaskRequest) (*pb.UpsertDataCompareTaskResponse, error) {
	showMsg, err := service.UpsertDataCompareTask(ctx, req)
	if err != nil {
		return &pb.UpsertDataCompareTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.UpsertDataCompareTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: showMsg,
	}}, nil
}

func (s *Server) DeleteDataCompareTask(ctx context.Context, req *pb.DeleteDataCompareTaskRequest) (*pb.DeleteDataCompareTaskResponse, error) {
	delMsg, err := service.DeleteDataCompareTask(ctx, req)
	if err != nil {
		return &pb.DeleteDataCompareTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.DeleteDataCompareTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: delMsg,
	}}, nil
}

func (s *Server) ShowDataCompareTask(ctx context.Context, req *pb.ShowDataCompareTaskRequest) (*pb.ShowDataCompareTaskResponse, error) {
	delMsg, err := service.ShowDataCompareTask(ctx, req)
	if err != nil {
		return &pb.ShowDataCompareTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.ShowDataCompareTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: delMsg,
	}}, nil
}
