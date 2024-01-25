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
)

func main() {
	// 创建一个最外层的上下文
	outerCtx, outerCancel := context.WithCancel(context.Background())

	// 使用context.WithTimeout创建一个带有超时的上下文
	timeout := 5 * time.Second
	ctx, _ := context.WithTimeout(outerCtx, timeout)

	// 启动一个 goroutine 模拟任务
	go func() {
		select {
		case <-ctx.Done():
			fmt.Println("Task canceled due to outer context cancel")
		case <-time.After(2 * time.Second):
			fmt.Println("Task completed successfully")
		}
	}()

	// 模拟外部的取消操作
	outerCancel()

	// 延迟关闭上下文
	//defer cancel()

	// 等待一段时间以查看上下文是否已被取消
	select {
	case <-ctx.Done():
		fmt.Println("Timeout context canceled due to outer context cancel")
	case <-time.After(1 * time.Second):
		fmt.Println("Timeout context is still active")
	}
}
