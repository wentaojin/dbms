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

	"github.com/wentaojin/dbms/utils/stringutil"

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
	addrRole   string
	logLevel   string
	// the used for database connection ready
	dbConnReady *atomic.Bool
}

func NewServiceConnect(etcdCli *clientv3.Client, addrRole, logLevel string, dbConnReady *atomic.Bool) *Connect {
	return &Connect{
		etcdClient:  etcdCli,
		addrRole:    addrRole,
		logLevel:    logLevel,
		dbConnReady: dbConnReady,
	}
}

func (c *Connect) Connect(prefixKey string) error {
	// according to the configuring prefix key, and get key information from etcd
	keyResp, err := GetKey(c.etcdClient, prefixKey, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	switch {
	case len(keyResp.Kvs) > 1:
		return fmt.Errorf("get key [%v] values is over one record from etcd server, it's panic, need check and fix, records are [%v]", prefixKey, keyResp.Kvs)
	case len(keyResp.Kvs) == 1:
		// open database conn
		for i := 1; i <= constant.DefaultInstanceServiceRetryCounts; i++ {
			var dbCfg *model.Database
			err = json.Unmarshal(keyResp.Kvs[0].Value, &dbCfg)
			if err != nil {
				if i == constant.DefaultInstanceServiceRetryCounts {
					return fmt.Errorf("json unmarshal [%v] to struct database faild: [%v]", stringutil.BytesToString(keyResp.Kvs[0].Value), err)
				} else {
					// retry
					logger.Warn("json unmarshal bytes to struct database failed, retrying",
						zap.String("bytes", stringutil.BytesToString(keyResp.Kvs[0].Value)),
						zap.Int("current retry counts", i),
						zap.Int("max retry counts", constant.DefaultInstanceServiceRetryCounts),
						zap.Duration("retry interval", constant.DefaultInstanceServiceRetryInterval))

					time.Sleep(constant.DefaultInstanceServiceRetryInterval)
					continue
				}
			}

			err = model.CreateDatabaseConnection(dbCfg, c.addrRole, c.logLevel)
			if err != nil && i == constant.DefaultInstanceServiceRetryCounts {
				return err
			} else if err == nil {
				logger.Info("database open init connect", zap.String("dbconn", dbCfg.String()))

				// before database connect success, api request disable service
				c.dbConnReady.Store(true)
				// break for loop
				break
			} else {
				// retry
				logger.Warn("database create connection failed, retrying",
					zap.String("connection info", dbCfg.String()),
					zap.Int("current retry counts", i),
					zap.Int("max retry counts", constant.DefaultInstanceServiceRetryCounts),
					zap.Duration("retry interval", constant.DefaultInstanceServiceRetryInterval))
				time.Sleep(constant.DefaultInstanceServiceRetryInterval)
				continue
			}
		}
	default:
		logger.Warn("database conn info is not exist, watch starting", zap.String("database key prefix", prefixKey))
	}
	return nil
}

// if the database key is change, and the program currently is running, maybe the program will crash,
// this is because the database connect error. so only applicable to scenarios where there are no tasks running in the program, and you need to wait 2-3 minutes

// Watch used for watch database key change
func (c *Connect) Watch(prefixKey string) error {
	err := c.Connect(prefixKey)
	if err != nil {
		return err
	}

	watchCh := WatchKey(c.etcdClient, prefixKey, clientv3.WithPrefix())
	for {
		select {
		case <-c.etcdClient.Ctx().Done():
			logger.Error("database conn watch cancel", zap.String("database key prefix", prefixKey))
			return nil
		default:
			for wresp := range watchCh {
				if wresp.Err() != nil {
					logger.Error("database conn watch failed", zap.String("database key prefix", prefixKey), zap.Error(wresp.Err()))
					// skip, only log
					//return fmt.Errorf("discovery node watch failed, error: [%v]", wresp.Err())
				}
				for _, ev := range wresp.Events {
					switch ev.Type {
					// modify or add
					case mvccpb.PUT:
						for i := 1; i <= constant.DefaultInstanceServiceRetryCounts; i++ {
							var dbCfg *model.Database
							err = json.Unmarshal(ev.Kv.Value, &dbCfg)
							if err != nil {
								if i == constant.DefaultInstanceServiceRetryCounts {
									return fmt.Errorf("json unmarshal [%v] to struct database faild: [%v]", stringutil.BytesToString(ev.Kv.Value), err)
								} else {
									// retry
									logger.Warn("json unmarshal bytes to struct database failed, retrying",
										zap.String("bytes", stringutil.BytesToString(ev.Kv.Value)),
										zap.Int("current retry counts", i),
										zap.Int("max retry counts", constant.DefaultInstanceServiceRetryCounts),
										zap.Duration("retry interval", constant.DefaultInstanceServiceRetryInterval))

									time.Sleep(constant.DefaultInstanceServiceRetryInterval)
									continue
								}
							}

							err = model.CreateDatabaseConnection(dbCfg, c.addrRole, c.logLevel)
							if err != nil && i == constant.DefaultInstanceServiceRetryCounts {
								return err
							} else if err == nil {
								logger.Info("database connect watch success update", zap.String("dbconn", dbCfg.String()))

								// before database connect success, api request disable service
								c.dbConnReady.Store(true)
								// break for loop
								break
							} else {
								// retry
								logger.Warn("database create connection failed, retrying",
									zap.String("connection info", dbCfg.String()),
									zap.Int("current retry counts", i),
									zap.Int("max retry counts", constant.DefaultInstanceServiceRetryCounts),
									zap.Duration("retry interval", constant.DefaultInstanceServiceRetryInterval))
								time.Sleep(constant.DefaultInstanceServiceRetryInterval)
								continue
							}
						}

					// delete
					case mvccpb.DELETE:
						c.dbConnReady.Store(false)
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
