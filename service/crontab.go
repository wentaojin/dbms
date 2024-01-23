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

	"github.com/wentaojin/dbms/utils/etcdutil"

	"github.com/robfig/cron/v3"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/logger"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type Entry struct {
	EntryID cron.EntryID
}

func (e *Entry) String() string {
	jsonByte, _ := stringutil.MarshalJSON(e)
	return jsonByte
}

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
		logger.Warn("crontab info is not exist, watch starting", zap.String("express key prefix", expressPrefixKey))
	default:
		for _, resp := range expressKeyResp.Kvs {
			keyS := stringutil.StringSplit(string(resp.Key), constant.StringSeparatorSlash)
			taskName := keyS[len(keyS)-1]

			_, err = c.cron.AddJob(string(resp.Value), NewCronjob(c.ctx, c.etcdClient, c.discoveries, taskName))
			if err != nil {
				return err
			}
			logger.Info("crontab load running", zap.String("crontab", resp.String()))
		}
	}
	return nil
}

func (c *Crontab) Watch(expressPrefixKey, entryPrefixKey string) error {
	err := c.Load(expressPrefixKey)
	if err != nil {
		return err
	}

	watchCh := etcdutil.WatchKey(c.etcdClient, expressPrefixKey, clientv3.WithPrefix())
	for {
		select {
		case <-c.etcdClient.Ctx().Done():
			logger.Error("crontab watch cancel", zap.String("express key prefix", expressPrefixKey))
			return nil
		default:
			for wresp := range watchCh {
				if wresp.Err() != nil {
					logger.Error("crontab watch failed", zap.String("express key prefix", expressPrefixKey), zap.Error(wresp.Err()))
					// skip, only log
					//return fmt.Errorf("discovery node watch failed, error: [%v]", wresp.Err())
				}
				for _, ev := range wresp.Events {
					switch ev.Type {
					// modify or add
					case mvccpb.PUT:
						// if the value is equal between before and now, then skip
						if strings.EqualFold(string(ev.Kv.Value), string(ev.PrevKv.Value)) {
							continue
						}

						keyS := stringutil.StringSplit(string(ev.Kv.Key), constant.StringSeparatorSlash)
						taskName := keyS[len(keyS)-1]

						entryKeyResp, err := etcdutil.GetKey(c.etcdClient, stringutil.StringBuilder(entryPrefixKey, taskName))
						if err != nil {
							return err
						}
						switch {
						case len(entryKeyResp.Kvs) > 1:
							return fmt.Errorf("get key [%v] values is over one record from etcd server, it's panic, need check and fix, records are [%v]", stringutil.StringBuilder(entryPrefixKey, taskName), entryKeyResp.Kvs)
						case len(entryKeyResp.Kvs) == 1:
							var oldEntry *Entry
							err = stringutil.UnmarshalJSON(entryKeyResp.Kvs[0].Value, &oldEntry)
							if err != nil {
								return err
							}
							c.cron.Remove(oldEntry.EntryID)

							newEntryID, err := c.cron.AddJob(string(ev.Kv.Value), NewCronjob(c.ctx, c.etcdClient, c.discoveries, taskName))
							if err != nil {
								return err
							}
							newEntry := &Entry{EntryID: newEntryID}
							_, err = etcdutil.PutKey(c.etcdClient, stringutil.StringBuilder(entryPrefixKey, taskName), newEntry.String())
							if err != nil {
								return err
							}
							logger.Info("crontab watch running",
								zap.String("crontab", ev.Kv.String()),
								zap.Any("old entry id", oldEntry.EntryID),
								zap.Any("new entry id", newEntryID))
						default:
							newEntryID, err := c.cron.AddJob(string(ev.Kv.Value), NewCronjob(c.ctx, c.etcdClient, c.discoveries, taskName))
							if err != nil {
								return err
							}
							newEntry := &Entry{EntryID: newEntryID}
							_, err = etcdutil.PutKey(c.etcdClient, stringutil.StringBuilder(entryPrefixKey, taskName), newEntry.String())
							if err != nil {
								return err
							}
							logger.Info("crontab watch running",
								zap.String("crontab", ev.Kv.String()),
								zap.Any("old entry id", "not exist"),
								zap.Any("new entry id", newEntryID))
							logger.Info("crontab watch running", zap.String("crontab", ev.Kv.String()))
						}
					// delete
					case mvccpb.DELETE:
						keyS := stringutil.StringSplit(string(ev.Kv.Key), constant.StringSeparatorSlash)
						taskName := keyS[len(keyS)-1]
						entryKeyResp, err := etcdutil.GetKey(c.etcdClient, stringutil.StringBuilder(entryPrefixKey, taskName))
						if err != nil {
							return err
						}
						if len(entryKeyResp.Kvs) == 0 {
							continue
						}
						var oldEntry *Entry
						err = stringutil.UnmarshalJSON(entryKeyResp.Kvs[0].Value, &oldEntry)
						if err != nil {
							return err
						}
						c.cron.Remove(oldEntry.EntryID)
					}
				}
			}
		}
	}
}

// Close used for close service
func (c *Crontab) Close() error {
	return c.etcdClient.Close()
}
