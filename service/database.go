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
	"fmt"
	"strings"

	"github.com/wentaojin/dbms/database/mysql"
	"github.com/wentaojin/dbms/database/oracle"
	"github.com/wentaojin/dbms/model/common"
	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/model/datasource"
	"github.com/wentaojin/dbms/utils/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func UpsertDatabase(etcdClient *clientv3.Client, defaultDatabaseDBMSKey string, req *pb.UpsertDatabaseRequest) (string, error) {
	db := &model.Database{
		Host:          req.Database.Host,
		Port:          req.Database.Port,
		Username:      req.Database.Username,
		Password:      req.Database.Password,
		Schema:        req.Database.Schema,
		SlowThreshold: req.Database.SlowThreshold,
	}
	jsonStr, err := stringutil.MarshalJSON(db)
	if err != nil {
		return jsonStr, err
	}

	err = model.CreateDatabaseSchema(db)
	if err != nil {
		return jsonStr, err
	}

	_, err = etcdutil.PutKey(etcdClient, defaultDatabaseDBMSKey, jsonStr, clientv3.WithPrevKV())
	if err != nil {
		return jsonStr, err
	}
	return jsonStr, nil
}

func DeleteDatabase(etcdClient *clientv3.Client, defaultDatabaseDBMSKey string) error {
	_, err := etcdutil.DeleteKey(etcdClient, defaultDatabaseDBMSKey, clientv3.WithPrevKV())
	if err != nil {
		return err
	}
	return nil
}

func ShowDatabase(etcdClient *clientv3.Client, defaultDatabaseDBMSKey string) (string, error) {
	kvResp, err := etcdutil.GetKey(etcdClient, defaultDatabaseDBMSKey, clientv3.WithPrefix())
	if err != nil {
		return string(kvResp.Kvs[0].Value), err
	}
	return string(kvResp.Kvs[0].Value), nil
}

func UpsertDatasource(ctx context.Context, req *pb.UpsertDatasourceRequest) (string, error) {
	var ds []*datasource.Datasource

	for _, r := range req.Datasource {
		if !strings.EqualFold(r.String(), "") {
			dataS := &datasource.Datasource{
				DatasourceName: r.DatasourceName,
				DbType:         r.DbType,
				Username:       r.Username,
				Password:       r.Password,
				Host:           r.Host,
				Port:           r.Port,
				ConnectParams:  r.ConnectParams,
				ConnectCharset: r.ConnectCharset,
				ConnectStatus:  r.ConnectStatus,
				ServiceName:    r.ServiceName,
				PdbName:        r.PdbName,
				Entity:         &common.Entity{Comment: r.Comment},
			}
			switch {
			case strings.EqualFold(r.DbType, constant.DatabaseTypeOracle):
				err := oracle.PingDatabaseConnection(dataS, "")
				if err != nil {
					return "", err
				}
			case strings.EqualFold(r.DbType, constant.DatabaseTypeMySQL):
				err := mysql.PingDatabaseConnection(dataS)
				if err != nil {
					return "", err
				}
			case strings.EqualFold(r.DbType, constant.DatabaseTypeTiDB):
				err := mysql.PingDatabaseConnection(dataS)
				if err != nil {
					return "", err
				}
			default:
				return "", fmt.Errorf("datasource [%v] database type [%v] is not support, please contact author or reselect", r.DatasourceName, r.DbType)
			}

			ds = append(ds, dataS)
		}
	}

	if len(ds) > 0 {
		dataS, err := model.GetIDatasourceRW().CreateDatasource(ctx, ds)
		if err != nil {
			return "", err
		}

		jsonStr, err := stringutil.MarshalJSON(dataS)
		if err != nil {
			return jsonStr, err
		}

		return jsonStr, nil
	}
	return "", fmt.Errorf("the datasource can't be null, please configure datasource")
}

func ShowDatasource(ctx context.Context, req *pb.ShowDatasourceRequest) (string, error) {
	if !strings.EqualFold(req.DatasourceName, "") {
		var dataS []*datasource.Datasource
		data, err := model.GetIDatasourceRW().GetDatasource(ctx, req.DatasourceName)
		if err != nil {
			return "", err
		}
		dataS = append(dataS, data)
		jsonStr, err := stringutil.MarshalJSON(dataS)
		if err != nil {
			return jsonStr, err
		}
		return jsonStr, nil
	}
	dataS, err := model.GetIDatasourceRW().ListDatasource(ctx, req.GetPage(), req.PageSize)
	if err != nil {
		return "", err
	}
	jsonStr, err := stringutil.MarshalJSON(dataS)
	if err != nil {
		return jsonStr, err
	}
	return jsonStr, nil
}

func DeleteDatasource(ctx context.Context, req *pb.DeleteDatasourceRequest) error {
	err := model.GetIDatasourceRW().DeleteDatasource(ctx, req.DatasourceName)
	if err != nil {
		return err
	}
	return nil
}
