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
	"time"

	"github.com/wentaojin/dbms/pool"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := pool.NewPool(ctx, 100,
		pool.WithTaskQueueSize(1024),
		pool.WithResultCallback(func(r pool.Result) {
			if r.Error != nil {
				panic(fmt.Errorf("task attr [%s] task stage [%v] workerID [%v] job [%v] failed [%v]", r.Task.Group, r.Thread, r.Task.Job, r.Error))
			} else {
				fmt.Printf("task attr [%s] task stage [%v] workerID [%v] job [%v] success.\n", r.Task.Group, r.Thread, r.Task.Job)
			}
		}),
		pool.WithExecuteTimeout(1*time.Second),
		pool.WithExecuteTask(func(ctx context.Context, t pool.Task) error {
			if t.Job.(int) == 5 {
				//time.Sleep(2 * time.Second)
				//fmt.Println(111)
				return fmt.Errorf("task meet error")
			}
			return nil
		}))

	defer p.Release()

	for i := 0; i < 100; i++ {
		p.AddTask(pool.Task{
			Name:  fmt.Sprintf("task%d", i),
			Group: "test",
			Job:   i,
		})
	}

	p.Wait()

	fmt.Println(1111)
}
