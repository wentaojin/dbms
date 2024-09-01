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
	"fmt"
	"strings"

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/logger"
	"go.uber.org/zap"

	"go.etcd.io/etcd/client/v3/concurrency"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// DefaultLeaderElectionTTLSecond is the duration that non-leader candidates will
	// wait to force acquire leadership
	DefaultLeaderElectionTTLSecond = 30
	// DefaultMasterLeaderPrefixKey is the dbms-master leader election key prefix
	DefaultMasterLeaderPrefixKey = "/dbms-master/leader"
)

// Election implements the leader election based on etcd
type Election struct {
	// Lock is the resource that will be used for locking
	EtcdClient *clientv3.Client

	// LeaseDuration is the duration that non-leader candidates will
	// wait to force acquire leadership. This is measured against time of
	// last observed ack.
	//
	// A client needs to wait a full LeaseDuration without observing a change to
	// the record before it can attempt to take over. When all clients are
	// shutdown and a new set of clients are started with different names against
	// the same leader record, they must wait the full LeaseDuration before
	// attempting to acquire the lease. Thes LeaseDuration should be as short as
	// possible (within your tolerance for clock skew rate) to avoid a possible
	// long waits in the scenario.
	//
	// Core clients default this value to 15 seconds.
	LeaseTTL int

	// Callbacks are callbacks that are triggered during certain lifecycle
	// events of the LeaderElector
	Callbacks Callbacks

	// Prefix is the election leader key prefix
	Prefix string

	// Identity is the election instance identity
	Identity string

	session  *concurrency.Session
	election *concurrency.Election
}

type Callbacks struct {
	// OnStartedLeading is called when a LeaderElector client starts leading
	OnStartedLeading func(ctx context.Context) error
	// OnStoppedLeading is called when a LeaderElector client stops leading
	OnStoppedLeading func(ctx context.Context) error
	// OnNewLeader is called when the client observes a leader that is
	// not the previously observed leader. This includes the first observed
	// leader when the client starts.
	OnNewLeader func(identity string)
}

func NewElection(e *Election) (*Election, error) {
	session, err := concurrency.NewSession(e.EtcdClient, concurrency.WithTTL(e.LeaseTTL))
	if err != nil {
		return nil, fmt.Errorf("etcd election create session failed: [%v]", err)
	}

	election := concurrency.NewElection(session, e.Prefix)

	e.session = session
	e.election = election
	return e, nil
}

func (e *Election) Run(ctx context.Context) (errs []error) {
	defer func() {
		if err := e.Callbacks.OnStoppedLeading(ctx); err != nil {
			errs = append(errs, err)
		}
	}()

	go e.observe(ctx)

	// campaign leader, become leader instance will be return, no campaign leader will be blocking here
	if err := e.election.Campaign(ctx, e.Identity); err != nil {
		errs = append(errs, err)
		return errs
	}

	// leader election success, running
	err := e.Callbacks.OnStartedLeading(ctx)
	if err != nil {
		errs = append(errs, err)
		return errs
	}

	return errs
}

// observe leader change
func (e *Election) observe(ctx context.Context) {
	if e.Callbacks.OnNewLeader == nil {
		return
	}

	ch := e.election.Observe(ctx)
	for {
		select {
		case <-ctx.Done():
			logger.Error("election node observe cancel", zap.String("identity", e.Identity))
			return
		case <-e.session.Done(): // Once the contract renewal is unstable, return
			logger.Error("election node session done", zap.String("identity", e.Identity))
			return
		case resp, ok := <-ch:
			if !ok {
				return
			}

			if len(resp.Kvs) == 0 {
				continue
			}

			leader := stringutil.BytesToString(resp.Kvs[0].Value)
			if leader != e.Identity {
				go e.Callbacks.OnNewLeader(leader)
			}
		}
	}
}

func (e *Election) Leader(ctx context.Context) (string, error) {
	resp, err := e.election.Leader(ctx)
	if err != nil {
		return "", err
	}
	return stringutil.BytesToString(resp.Kvs[0].Value), nil
}

// CurrentIsLeader judge current node whether is leader
func (e *Election) CurrentIsLeader(ctx context.Context) (bool, error) {
	leader, err := e.Leader(ctx)
	if err != nil {
		return false, err
	}
	if strings.EqualFold(leader, e.Identity) {
		return true, nil
	}
	return false, nil
}

func (e *Election) Close() {
	if e.session != nil {
		e.session.Close()
	}
}
