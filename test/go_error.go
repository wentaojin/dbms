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
)

func observe(ctx context.Context, ch chan int) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case resp, ok := <-ch:
			if !ok {
				return nil
			}
			if resp == 10 {
				return fmt.Errorf("observe error, resp: [%d]", resp)
			}
		}
	}
}

func paras(ch chan int) {
	for {
		for i := 0; i < 100; i++ {
			ch <- i
		}
	}
}

func x(ctx context.Context) error {
	ch := make(chan int)

	go func() {
		err := func() error {
			err := observe(ctx, ch)
			if err != nil {

				return err
			}
			return nil
		}()
		if err != nil {
			return
		}
	}()

	go paras(ch)

	return nil
}

func main() {
	ctx := context.Background()
	err := x(ctx)
	if err != nil {
		panic(err)
	}
	select {}
}
