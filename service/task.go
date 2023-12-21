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
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/etcdutil"
	"github.com/wentaojin/dbms/utils/stringutil"
	"github.com/wentaojin/dbms/worker"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func UpsertTask(etcdClient *clientv3.Client, req *pb.OperateTaskRequest) error {
	t := &worker.Task{Name: req.TaskName, Express: req.Express}
	_, err := etcdutil.PutKey(etcdClient, stringutil.StringBuilder(constant.DefaultScheduledTaskSubmitPrefixKey, req.TaskName), t.String(), clientv3.WithPrevKV())
	if err != nil {
		return err
	}
	return nil
}

func DeleteTask(etcdClient *clientv3.Client, req *pb.OperateTaskRequest) error {
	_, err := etcdutil.DeleteKey(etcdClient, stringutil.StringBuilder(constant.DefaultScheduledTaskSubmitPrefixKey, req.TaskName), clientv3.WithPrevKV())
	if err != nil {
		return err
	}
	return nil
}

func KillTask(etcdClient *clientv3.Client, req *pb.OperateTaskRequest) error {
	t := &worker.Task{Name: req.TaskName, Express: req.Express}
	_, err := etcdutil.PutKey(etcdClient, stringutil.StringBuilder(constant.DefaultScheduledTaskKillPrefixKey, req.TaskName), t.String(), clientv3.WithPrevKV())
	if err != nil {
		return err
	}
	return nil
}
