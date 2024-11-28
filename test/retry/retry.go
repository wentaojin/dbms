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
	"fmt"

	"github.com/wentaojin/dbms/thread"
)

func main() {
	if err := x(); err != nil {
		panic(err)
	}

	fmt.Println(234)
}

func x() error {
	g := thread.NewGroup()
	g.SetLimit(2)

	go func() {
		for i := 0; i < 1000; i++ {
			g.Go(i, func(j interface{}) error {
				x := j.(int)
				if x == 20 {
					return fmt.Errorf("error 20")
				}
				return nil
			})
		}
		g.Wait()
	}()

	for res := range g.ResultC {
		if err := thread.Retry(
			&thread.RetryConfig{
				MaxRetries: thread.DefaultThreadErrorMaxRetries,
				Delay:      thread.DefaultThreadErrorRereyDelay,
			},
			func(err error) bool {
				return true
			},
			func() error {
				if res.Error != nil {
					return res.Error
				}
				return nil
			}); err != nil {
			return err
		}

	}

	fmt.Println(11111)
	return nil
}
