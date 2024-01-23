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
	"context"
	"crypto/tls"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// DefaultDialTimeout is the maximum amount of time a dial will wait for a
	// connection to setup. 30s is long enough for most of the network conditions.
	DefaultDialTimeout = 30 * time.Second

	// DefaultRequestTimeout 10s is long enough for most of etcd clusters.
	DefaultRequestTimeout = 10 * time.Second

	// DefaultAutoSyncIntervalDuration is the auto sync interval duration for etcd
	DefaultAutoSyncIntervalDuration = 30 * time.Second
)

// CreateClient creates an etcd client with some default config items.
func CreateClient(ctx context.Context, endpoints []string, tlsCfg *tls.Config) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Context:          ctx,
		Endpoints:        endpoints,
		DialTimeout:      DefaultDialTimeout,
		AutoSyncInterval: DefaultAutoSyncIntervalDuration,
		TLS:              tlsCfg,
	})
}

// ListMembers returns a list of internal etcd members.
func ListMembers(client *clientv3.Client) (*clientv3.MemberListResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	defer cancel()
	return client.MemberList(ctx)
}

// AddMember adds an etcd member.
func AddMember(client *clientv3.Client, peerAddrs []string) (*clientv3.MemberAddResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	defer cancel()
	return client.MemberAdd(ctx, peerAddrs)
}

// RemoveMember removes an etcd member by the given id.
func RemoveMember(client *clientv3.Client, id uint64) (*clientv3.MemberRemoveResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	defer cancel()
	return client.MemberRemove(ctx, id)
}

// PutKey puts key-value in the etcd server
func PutKey(client *clientv3.Client, key, value string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	defer cancel()
	return client.Put(ctx, key, value, opts...)
}

// GetKey gets key-value in the etcd server
func GetKey(client *clientv3.Client, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	defer cancel()
	return client.Get(ctx, key, opts...)
}

// DeleteKey delete key-value in the etcd server
func DeleteKey(client *clientv3.Client, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	defer cancel()
	return client.Delete(ctx, key, opts...)
}

// WatchKey watch key in the etcd server
func WatchKey(client *clientv3.Client, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	// Setting the context with timeout, the etcd watch function would watch failed,
	// Maybe the watch is a permanent watcher and does not support setting timeout context.
	// Current setting client context from parent context
	// ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	return client.Watch(client.Ctx(), key, opts...)
}

// TxnKey put key txn
func TxnKey(client *clientv3.Client, ops ...clientv3.Op) (*clientv3.TxnResponse, error) {
	txn := client.Txn(client.Ctx())
	txn.Then(ops...)
	return txn.Commit()
}
