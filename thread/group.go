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
package thread

import (
	"fmt"
	"sync"
	"time"

	"github.com/wentaojin/dbms/utils/constant"
)

type token struct{}

// A Group is reference to errgroup package
type Group struct {
	wg sync.WaitGroup

	sem chan token

	ResultC chan Result
}

// A Result is from the group
type Result struct {
	Task     interface{}
	Duration string // unit: seconds
	Error    error
}

func (g *Group) done() {
	if g.sem != nil {
		<-g.sem
	}
	g.wg.Done()
}

func NewGroup() *Group {
	return &Group{}
}

// Wait blocks until all function calls from the Go method have returned, then
// returns the first non-nil error (if any) from them.
func (g *Group) Wait() {
	g.wg.Wait()
	close(g.sem)
	close(g.ResultC)
}

// Go calls the given function in a new goroutine.
// It blocks until the new goroutine can be added without the number of
// active goroutines in the group exceeding the configured limit.
//
// The first call to return a non-nil error cancels the group's context, if the
// group was created by calling WithContext. The error will be returned by Wait.
func (g *Group) Go(job interface{}, fn func(job interface{}) error) {
	if g.sem != nil {
		g.sem <- token{}
	}

	g.wg.Add(1)
	go func(job interface{}) {
		defer g.done()

		startTime := time.Now()

		if err := fn(job); err != nil {
			g.ResultC <- Result{
				Task:     job,
				Duration: fmt.Sprintf("%f", time.Now().Sub(startTime).Seconds()),
				Error:    err,
			}
		}
	}(job)
}

// SetLimit limits the number of active goroutines in this group to at most n.
// A negative value indicates no limit.
//
// Any subsequent call to the Go method will block until it can add an active
// goroutine without exceeding the configured limit.
//
// The limit must not be modified while any goroutines in the group are active.
func (g *Group) SetLimit(n int) {
	if n < 0 {
		g.sem = nil
		g.ResultC = make(chan Result, constant.DefaultMigrateTaskQueueSize)
		return
	}
	if len(g.sem) != 0 {
		panic(fmt.Errorf("errgroup: modify limit while %v goroutines in the group are still active", len(g.sem)))
	}
	g.sem = make(chan token, n)

	// Avoid slow database operations that cause synchronization blocking of other processes, the channel buffer is set to 2 times the number of threads.
	g.ResultC = make(chan Result, n*2)

}
