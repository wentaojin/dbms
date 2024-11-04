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

	"github.com/robfig/cron/v3"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/etcdutil"
	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type Crontab struct {
	ctx         context.Context
	discoveries *etcdutil.Discovery
	etcdClient  *clientv3.Client
	cron        *cron.Cron
}

func NewServiceCrontab(ctx context.Context, etcdCli *clientv3.Client, discoveries *etcdutil.Discovery, cron *cron.Cron) *Crontab {
	return &Crontab{
		ctx:         ctx,
		etcdClient:  etcdCli,
		discoveries: discoveries,
		cron:        cron,
	}
}

func (c *Crontab) Load(expressPrefixKey string) error {
	// according to the configuring prefix key, and get key information from etcd
	expressKeyResp, err := etcdutil.GetKey(c.etcdClient, expressPrefixKey, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	switch {
	case len(expressKeyResp.Kvs) == 0:
		logger.Warn("the crontab task is not exist, waiting add the task", zap.String("express key prefix", expressPrefixKey))
	default:
		for _, resp := range expressKeyResp.Kvs {
			keyS := stringutil.StringSplit(stringutil.BytesToString(resp.Key), constant.StringSeparatorSlash)
			taskName := keyS[len(keyS)-1]

			var expr *Express
			err = stringutil.UnmarshalJSON(resp.Value, &expr)
			if err != nil {
				return fmt.Errorf("the task [%s] load value unmarshal failed, disable delete task, error: %v", taskName, err)
			}

			_, err = c.cron.AddJob(expr.Express, NewCronjob(c.etcdClient, c.discoveries, expr.TaskName, expr.AssignHost))
			if err != nil {
				return err
			}
			logger.Warn("the crontab task load running", zap.String("crontab", resp.String()))
		}
	}
	return nil
}

// Close used for close service
func (c *Crontab) Close() error {
	return c.etcdClient.Close()
}
