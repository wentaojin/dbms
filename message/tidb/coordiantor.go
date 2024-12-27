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
package tidb

import (
	"sync"

	"github.com/wentaojin/dbms/logger"
	"go.uber.org/zap"
)

// global ddl coordinator
var ddlCoordinator *DdlCoordinator

type DdlCoordinator struct {
	mu sync.Mutex `json:"-"`

	// ticdc open protocol, DDL message events will be distributed to all partitions
	// That is, if any partition receives the DDL Event, all other partitions need to receive it, indicating that the DDL event is complete
	CoordNums int `json:"coordNums"`

	// ddl coordinator
	// ddl -> partitions
	// 1. Determine whether the ddl event is received completely, len([]int32) == coordNums
	// 2. Receive the complete ddl event and append the ddl task queue
	Coordinators map[string][]int `json:"Coordinators"`

	// store ddl max commitTs information
	DdlWithMaxCommitTs *DDLChangedEvent `json:"ddlWithMaxCommits"`

	// ddl task queue, stores ddl events in order of commitTs
	Ddls DDLChangedEvents `json:"Ddls"`
}

func NewDdlCoordinator() *DdlCoordinator {
	return &DdlCoordinator{
		Coordinators: make(map[string][]int),
	}
}

func (d *DdlCoordinator) SetCoords(coords int) {
	d.CoordNums = coords
}

func (d *DdlCoordinator) IsDDLFlush(key *DDLChangedEvent) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	// DDL Event is received completely, only one DDL Event is retained
	if len(d.Coordinators[key.String()]) == d.CoordNums {
		d.appendDDL(key)
		return true
	}
	return false
}

func (d *DdlCoordinator) IsResolvedFlush(resolvedTs uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	// whether DDL coordination exists. If yes, it is true; if no, it is false.
	return len(d.Coordinators) == 0
}

func (d *DdlCoordinator) Append(key *DDLChangedEvent, value int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	// In special cases (node ​​failure, network partition, etc.), the same version of Event may be sent multiple times
	// The DDL coordinator only guarantees to receive 1 DDL Event for the same version (to avoid duplicate reception)
	event := key.String()
	if vals, ok := d.Coordinators[event]; ok {
		if isContain(value, vals) {
			logger.Warn("ddl event duplicate reception",
				zap.Int("partition", value),
				zap.String("events", key.String()),
				zap.String("message_action", "ignore"))
			return
		}
	}
	d.Coordinators[event] = append(d.Coordinators[event], value)
}

func (d *DdlCoordinator) Remove(key *DDLChangedEvent) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.Coordinators, key.String())
	return nil
}

func (d *DdlCoordinator) Get(key *DDLChangedEvent) []int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.Coordinators[key.String()]
}

func (d *DdlCoordinator) Len(key *DDLChangedEvent) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.Coordinators[key.String()])
}

// append DDL wait to be handled, only consider the constraint among DDLs.
// for DDL a / b received in the order, a.CommitTs < b.CommitTs should be true.
func (d *DdlCoordinator) appendDDL(ddl *DDLChangedEvent) {
	// A rename tables DDL job contains multiple DDL events with same CommitTs.
	// So to tell if a DDL is redundant or not, we must check the equivalence of
	// the current DDL and the DDL with max CommitTs.
	if ddl == d.DdlWithMaxCommitTs {
		logger.Warn("ignore redundant ddl, the ddl is equal to ddlWithMaxCommitTs",
			zap.Uint64("commitTs", ddl.CommitTs),
			zap.String("events", ddl.String()))
		return
	}

	d.Ddls.Add(ddl)
	d.DdlWithMaxCommitTs = ddl
}

func (d *DdlCoordinator) GetFrontDDL() *DDLChangedEvent {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.Ddls.Len() > 0 {
		return d.Ddls[0]
	}
	return nil
}

func (d *DdlCoordinator) PopDDL() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.Ddls.Len() > 0 {
		d.Ddls = d.Ddls[1:]
	}
}

func isContain(val int, values []int) bool {
	for _, v := range values {
		if v == val {
			return true
		}
	}
	return false
}
