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

func (s *Server) UpsertDataMigrateTask(ctx context.Context, req *pb.UpsertDataMigrateTaskRequest) (*pb.UpsertDataMigrateTaskResponse, error) {
	showMsg, err := service.UpsertDataMigrateTask(ctx, req)
	if err != nil {
		return &pb.UpsertDataMigrateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.UpsertDataMigrateTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: showMsg,
	}}, nil
}

func (s *Server) DeleteDataMigrateTask(ctx context.Context, req *pb.DeleteDataMigrateTaskRequest) (*pb.DeleteDataMigrateTaskResponse, error) {
	delMsg, err := service.DeleteDataMigrateTask(ctx, req)
	if err != nil {
		return &pb.DeleteDataMigrateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.DeleteDataMigrateTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: delMsg,
	}}, nil
}

func (s *Server) ShowDataMigrateTask(ctx context.Context, req *pb.ShowDataMigrateTaskRequest) (*pb.ShowDataMigrateTaskResponse, error) {
	delMsg, err := service.ShowDataMigrateTask(ctx, req)
	if err != nil {
		return &pb.ShowDataMigrateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.ShowDataMigrateTaskResponse{Response: &pb.Response{
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
