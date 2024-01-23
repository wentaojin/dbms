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
package etcd

import (
	"context"
	"fmt"

	"github.com/wentaojin/dbms/logger"
	"go.uber.org/zap"

	"github.com/wentaojin/dbms/utils/stringutil"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Locker struct {
	lockPath        string
	leaseID         clientv3.LeaseID
	etcdClient      *clientv3.Client
	cancelKeepAlive context.CancelFunc
	isLocked        bool
}

func NewLocker(name string, etcdClient *clientv3.Client) *Locker {
	return &Locker{
		lockPath:   stringutil.StringBuilder(DefaultScheduledTaskLockPrefixKey, name),
		etcdClient: etcdClient}
}

func (l *Locker) Lock() (err error) {
	var (
		grantResp         *clientv3.LeaseGrantResponse
		keepAliveRespChan <-chan *clientv3.LeaseKeepAliveResponse
		txnResp           *clientv3.TxnResponse
		txn               clientv3.Txn
	)

	// automatic renewal
	if grantResp, err = l.etcdClient.Lease.Grant(context.TODO(), DefaultDistributedLockLease); err != nil {
		return
	}
	keepCtx, keepCancel := context.WithCancel(context.TODO())
	if keepAliveRespChan, err = l.etcdClient.KeepAlive(keepCtx, grantResp.ID); err != nil {
		goto Rollback
	}

	// consumer lease
	go func() {
		var (
			keepAliveResp *clientv3.LeaseKeepAliveResponse
		)
		for {
			select {
			case keepAliveResp = <-keepAliveRespChan:
				if keepAliveResp == nil {
					logger.Warn("stop to keep lease alive",
						zap.String("lock path", l.lockPath), zap.Any("leaseID", grantResp.ID))
					goto end
				}
			}
		}
	end:
	}()

	// transaction (set if not exist) implements locking
	txn = l.etcdClient.Txn(context.TODO())
	txn.If(clientv3.Compare(clientv3.CreateRevision(l.lockPath), "=", 0)).
		Then(clientv3.OpPut(l.lockPath, "", clientv3.WithLease(grantResp.ID)))
	if txnResp, err = txn.Commit(); err != nil {
		goto Rollback
	}
	if !txnResp.Succeeded {
		err = fmt.Errorf("attempt to lock [%s], but failed", l.lockPath)
		goto Rollback
	}

	l.leaseID = grantResp.ID
	l.cancelKeepAlive = keepCancel
	l.isLocked = true

	return

	// revoke lock resource
Rollback:
	logger.Warn("rollback lock resource",
		zap.String("lock path", l.lockPath), zap.Any("leaseID", grantResp.ID), zap.Error(err))
	keepCancel()
	// Failed to delete the lease. The lease will be automatically revoked again after a few seconds
	_, err = l.etcdClient.Revoke(context.TODO(), grantResp.ID)
	return
}

func (l *Locker) UnLock() error {
	logger.Warn("unlock resource", zap.String("lock path", l.lockPath))
	if l.isLocked {
		l.cancelKeepAlive()
		_, err := l.etcdClient.Revoke(context.TODO(), l.leaseID)
		if err != nil {
			return err
		}
		l.isLocked = false
	}
	return nil
}
