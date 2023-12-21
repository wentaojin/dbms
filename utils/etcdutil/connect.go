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
package etcdutil

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/wentaojin/dbms/model"

	"sync/atomic"

	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/utils/constant"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Connect used for watch meta database conn
// if the database conn information occur change, then modify database conn
type Connect struct {
	etcdClient *clientv3.Client
	logLevel   string
	// the used for database connection ready
	dbConnReady *atomic.Bool
}

func NewServiceConnect(etcdCli *clientv3.Client, logLevel string, dbConnReady *atomic.Bool) *Connect {
	return &Connect{
		etcdClient:  etcdCli,
		logLevel:    logLevel,
		dbConnReady: dbConnReady,
	}
}

func (c *Connect) Connect(prefixKey string) error {
	for !c.dbConnReady.Load() {
		// according to the configuring prefix key, and get key information from etcd
		keyResp, err := GetKey(c.etcdClient, prefixKey, clientv3.WithPrefix())
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
				return fmt.Errorf("json unmarshal [%v] to struct database faild: [%v]", string(keyResp.Kvs[0].Value), err)
			}

			err = model.CreateDatabase(dbCfg, c.logLevel)
			if err != nil {
				return err
			}

			// before database connect success, api request disable service
			c.dbConnReady.Store(true)

			logger.Info("database open connect", zap.String("dbconn", dbCfg.String()))
		}
	}
	return nil
}

// if the database key is change, and the program currently is running, maybe the program will crash,
// this is because the database connect error. so only applicable to scenarios where there are no tasks running in the program, and you need to wait 2-3 minutes

// Watch used for watch database key change
func (c *Connect) Watch(prefixKey string) error {
	logger.Info("database conn watch starting", zap.String("key prefix", prefixKey))
	watchCh := WatchKey(c.etcdClient, prefixKey, clientv3.WithPrefix())
	for {
		select {
		case <-c.etcdClient.Ctx().Done():
			logger.Error("database conn watch cancel", zap.String("key prefix", prefixKey))
			return nil
		default:
			for wresp := range watchCh {
				if wresp.Err() != nil {
					logger.Error("database conn watch failed", zap.String("key prefix", prefixKey), zap.Error(wresp.Err()))
					// skip, only log
					//return fmt.Errorf("discovery node watch failed, error: [%v]", wresp.Err())
				}
				for _, ev := range wresp.Events {
					switch ev.Type {
					// modify or add
					case mvccpb.PUT:
						for i := 1; i <= constant.DefaultInstanceServiceRetryCounts; i++ {
							var dbCfg *model.Database
							err := json.Unmarshal(ev.Kv.Value, &dbCfg)
							if err != nil {
								if i == constant.DefaultInstanceServiceRetryCounts {
									return fmt.Errorf("json unmarshal [%v] to struct database faild: [%v]", string(ev.Kv.Value), err)
								} else {
									// retry
									time.Sleep(constant.DefaultInstanceServiceRetryInterval)
									continue
								}
							}

							err = model.CreateDatabase(dbCfg, c.logLevel)
							if err != nil && i == constant.DefaultInstanceServiceRetryCounts {
								return err
							} else if err == nil {
								logger.Info("database connect hot update", zap.String("dbconn", dbCfg.String()))

								// before database connect success, api request disable service
								c.dbConnReady.Store(true)
								// break for loop
								break
							} else {
								// retry
								time.Sleep(constant.DefaultInstanceServiceRetryInterval)
								continue
							}
						}

					// delete
					case mvccpb.DELETE:
						// TODO: ignore, don't know how to deal with it now. if the program occur restart, maybe the program will crash
					}
				}
			}
		}
	}
}

// Close used for close service
func (c *Connect) Close() error {
	return c.etcdClient.Close()
}
