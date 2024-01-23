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

// UpsertRule implements MasterServer.UpsertTaskMigrateRule
func (s *Server) UpsertRule(ctx context.Context, req *pb.UpsertRuleRequest) (*pb.UpsertRuleResponse, error) {
	createMsg, err := service.UpsertRule(ctx, req)
	if err != nil {
		return &pb.UpsertRuleResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}

	return &pb.UpsertRuleResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: createMsg,
	}}, nil
}

func (s *Server) DeleteRule(ctx context.Context, req *pb.DeleteRuleRequest) (*pb.DeleteRuleResponse, error) {
	err := service.DeleteRule(ctx, req)
	if err != nil {
		return &pb.DeleteRuleResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.DeleteRuleResponse{Response: &pb.Response{
		Result: openapi.ResponseResultStatusSuccess,
	}}, nil
}

func (s *Server) ShowRule(ctx context.Context, req *pb.ShowRuleRequest) (*pb.ShowRuleResponse, error) {
	showMsg, err := service.ShowRule(ctx, req)
	if err != nil {
		return &pb.ShowRuleResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.ShowRuleResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: showMsg,
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
