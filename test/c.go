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
package main

import (
	"context"
	"fmt"
	"golang.org/x/sync/errgroup"
	"time"
)

// Task represents a unit of work.
type Task struct {
	ID int
}

// Run executes the task and prints its ID.
func (t *Task) Run() {
	fmt.Printf("consume: %v\n", t.ID)
}

// TaskNum defines the number of tasks to produce.
const TaskNum int = 300000

// TaskChannel is the buffered channel for tasks.
var taskCh = make(chan Task, 10)

// producer generates tasks and sends them to the task channel.
func producer(ctx context.Context, prodConc int, wo chan<- Task) error {
	// Create an errgroup with concurrency limit.
	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(prodConc)
	// Start the producers.
	for i := 0; i <= TaskNum; i++ {
		idx := i
		g.Go(func() error {
			select {
			case <-ctx.Done():
				fmt.Println("Producer exiting due to context cancellation.")
				return ctx.Err()
			default:
				ts := Task{ID: idx}
				wo <- ts
				return nil
			}
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

// consumer receives tasks from the task channel and processes them.
func consumer(ctx context.Context, consConc int, ro <-chan Task) error {
	// Start the consumers with concurrency limit.
	cg, _ := errgroup.WithContext(ctx)
	cg.SetLimit(consConc)

	// Start the consumers.
	for r := range ro {
		c := r
		cg.Go(func() error {
			select {
			case <-ctx.Done():
				fmt.Println("Consumer exiting due to context cancellation.")
				return ctx.Err()
			default:
				if c.ID != 0 {
					c.Run()
				}
				return nil
			}
		})
	}
	return cg.Wait()
}

func main() {
	// Create a context with a cancel function.
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(100 * time.Millisecond)
		// Cancel the context after some time to simulate shutdown.
		cancel()
	}()
	// Call the Exec function with the context and concurrency limit.
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(taskCh)
		err := producer(gCtx, 3, taskCh)
		if err != nil {
			return err
		}
		return nil
	})

	g.Go(func() error {
		err := consumer(gCtx, 3, taskCh)
		if err != nil {
			return err
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		panic(err)
	}

	fmt.Println("Main function exited.")
}
