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
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := run(ctx); err != nil {
		panic(err)
	}
	fmt.Println(2222)

	go func() {
		fmt.Println("Go routine done")
		cancel()
	}()
	<-ctx.Done()
}

func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	fmt.Printf("startTime: %v\n", time.Now())

	go func() {
		time.Sleep(10 * time.Second)
		cancel()
		fmt.Printf("endTime: %v\n", time.Now())
	}()

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(2)

	for i := 0; i < 20; i++ {
		select {
		case <-gCtx.Done():
			goto Loop
		default:
			idx := i
			g.Go(func() error {
				fmt.Printf("分配：%d\n", idx)
				select {
				case <-gCtx.Done():
					return gCtx.Err()
				default:
					err := t(gCtx, idx)
					if err != nil {
						return err
					}
					return nil
				}
			})
		}
	}
Loop:

	if err := g.Wait(); !errors.Is(err, context.Canceled) {
		return err
	}

	fmt.Println(1111)

	return nil
}
func t(ctx context.Context, idx int) error {
	time.Sleep(2 * time.Second)
	fmt.Printf("运行完成：%d\n", idx)
	return nil
}
