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
package errconcurrent

import (
	"fmt"
	"sync"
)

type token struct{}

// A Group is reference to errgroup package
type Group struct {
	wg sync.WaitGroup

	sem chan token

	mu *sync.Mutex

	Results []Result
}

// A Result is from the group
type Result struct {
	Task interface{}
	Err  error
}

func (g *Group) done() {
	if g.sem != nil {
		<-g.sem
	}
	g.wg.Done()
}

// NewGroup returns a new Group and an associated Context derived from ctx.
//
// The derived Context is canceled the first time a function passed to Go
// returns a non-nil error or the first time Wait returns, whichever occurs
// first.
func NewGroup() *Group {
	return &Group{
		mu: new(sync.Mutex),
	}
}

// Wait blocks until all function calls from the Go method have returned, then
// returns the first non-nil error (if any) from them.
func (g *Group) Wait() []Result {
	g.wg.Wait()
	close(g.sem)
	return g.Results
}

// Go calls the given function in a new goroutine.
// It blocks until the new goroutine can be added without the number of
// active goroutines in the group exceeding the configured limit.
//
// The first call to return a non-nil error cancels the group's context, if the
// group was created by calling WithContext. The error will be returned by Wait.
func (g *Group) Go(t interface{}, f func(t interface{}) error) {
	if g.sem != nil {
		g.sem <- token{}
	}

	g.wg.Add(1)
	go func(t interface{}) {
		defer g.done()

		if err := f(t); err != nil {
			g.mu.Lock()
			g.Results = append(g.Results, Result{
				Task: t,
				Err:  err,
			})
			g.mu.Unlock()
		}
	}(t)
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
		return
	}
	if len(g.sem) != 0 {
		panic(fmt.Errorf("errgroup: modify limit while %v goroutines in the group are still active", len(g.sem)))
	}
	g.sem = make(chan token, n)
}
