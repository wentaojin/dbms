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
	"golang.org/x/sync/errgroup"
)

type IDataMigrateProcessor interface {
	MigrateRead() error
	MigrateProcess() error
	MigrateApply() error
}

func IDataMigrate(p IDataMigrateProcessor) error {
	//g, ctx := errgroup.WithContext(ctx)
	g := &errgroup.Group{}
	g.Go(func() error {
		//select {
		//case <-ctx.Done():
		//	return ctx.Err()
		//default:
		err := p.MigrateProcess()
		if err != nil {
			return err
		}
		return nil
		//}
	})

	g.Go(func() error {
		//select {
		//case <-ctx.Done():
		//	return ctx.Err()
		//default:
		err := p.MigrateApply()
		if err != nil {
			return err
		}
		return nil
		//}
	})

	err := p.MigrateRead()
	if err != nil {
		return err
	}

	err = g.Wait()
	if err != nil {
		return err
	}

	return nil
}

type Migrate struct {
	ctx          context.Context
	ReadChannel  chan [][]int
	WriteChannel chan []int
}

func NewMigrate(ctx context.Context) *Migrate {
	readChannel := make(chan [][]int, 1024)
	writeChannel := make(chan []int, 1024)
	return &Migrate{
		ctx:          ctx,
		ReadChannel:  readChannel,
		WriteChannel: writeChannel}
}

func (m *Migrate) MigrateRead() error {
	batchSize := 10

	var (
		reads [][]int
		tmp   []int
	)

	for i := 0; i < 100; i++ {
		tmp = append(tmp, i)
		if len(tmp) == batchSize {
			reads = append(reads, tmp)
			m.ReadChannel <- reads
			tmp = make([]int, 0)
		}
	}

	if len(tmp) > 0 {
		reads = append(reads, tmp)
		m.ReadChannel <- reads
	}

	close(m.ReadChannel)
	return nil
}

func (m *Migrate) MigrateProcess() error {
	for data := range m.ReadChannel {
		for _, d := range data {
			m.WriteChannel <- d
		}
	}

	close(m.WriteChannel)
	return nil
}

func (m *Migrate) MigrateApply() error {
	g := &errgroup.Group{}
	g.SetLimit(2)

	for dataC := range m.WriteChannel {
		vals := dataC
		g.Go(func() error {
			fmt.Println(vals)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool(ctx); err != nil {
		panic(err)
	}
	fmt.Println(2222)

	go func() {
		fmt.Println("Go routine done")
		cancel()
	}()
	<-ctx.Done()
}

func pool(ctx context.Context) error {
	cancelCtx, cancel := context.WithCancel(ctx)
	fmt.Printf("startTime: %v\n", time.Now())

	go func() {
		time.Sleep(10 * time.Second)
		cancel()
		fmt.Printf("endTime: %v\n", time.Now())
	}()

	g := errconcurrent.NewGroup()
	g.SetLimit(2)
	for i := 0; i < 10; i++ {
		select {
		case <-cancelCtx.Done():
			goto BreakLoop
		default:
			g.Go(i, func(t interface{}) error {
				fmt.Printf("分配：%d\n", t)
				time.Sleep(2 * time.Second)
				err := IDataMigrate(NewMigrate(cancelCtx))
				if err != nil {
					return err
				}
				return nil
			})
		}
	}
BreakLoop:
	fmt.Println("the task data migrate stage chunk has be canceled")

	for _, ts := range g.Wait() {
		if ts.Err != nil {
			fmt.Printf("task: %v, error: %v\n", ts.Task, ts.Err)
		}
	}
	return nil
}
