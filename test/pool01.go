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

	"github.com/wentaojin/dbms/errconcurrent"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := runT(ctx); err != nil {
		panic(err)
	}
	fmt.Println(2222)

	go func() {
		fmt.Println("Go routine done")
		cancel()
	}()
	<-ctx.Done()
}

func runT(ctx context.Context) error {
	cancelCtx, cancel := context.WithCancel(ctx)
	fmt.Printf("startTime: %v\n", time.Now())

	go func() {
		time.Sleep(10 * time.Second)
		cancel()
		fmt.Printf("endTime: %v\n", time.Now())
	}()

	g := errconcurrent.NewGroup()
	g.SetLimit(2)

	for i := 0; i < 20; i++ {
		select {
		case <-cancelCtx.Done():
			goto Loop
		default:
			g.Go(i, func(task interface{}) error {
				idx := task.(int)
				fmt.Printf("分配：%d\n", idx)
				if idx == 9 {
					return fmt.Errorf("error")
				}
				err := pt(cancelCtx, idx)
				if err != nil {
					return err
				}
				return nil
			})
		}
	}
Loop:

	for _, r := range g.Wait() {
		if r.Err != nil {
			fmt.Printf("task: %d, error: %v\n", r.Task, r.Err)
		}
	}

	fmt.Println(1111)

	return nil
}
func pt(ctx context.Context, idx int) error {
	time.Sleep(2 * time.Second)
	fmt.Printf("运行完成：%d\n", idx)
	return nil
}
