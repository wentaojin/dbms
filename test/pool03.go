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

	"github.com/wentaojin/dbms/errconcurrent"
)

func main() {

	ctx := context.Background()

	g := errconcurrent.NewGroup()
	g.SetLimit(2)

	for i := 0; i < 1000000000; i++ {
		g.Go(i, func(t interface{}) error {
			select {
			case <-ctx.Done():
				return nil
			default:
				readChannel := make(chan [][]int, 1024)
				writeChannel := make(chan []int, 1024)
				err := IDataMigrate(&Migrate{
					ctx:          ctx,
					ReadChannel:  readChannel,
					WriteChannel: writeChannel})
				if err != nil {
					return err
				}
				return nil
			}
		})
	}

	for _, r := range g.Wait() {
		if r.Err != nil {
			fmt.Println(r.Err)
		}
	}
}

type IDataMigrateProcessor interface {
	MigrateRead() error
	MigrateProcess() error
	MigrateApply() error
}

func IDataMigrate(p IDataMigrateProcessor) error {
	//g, ctx := errgroup.WithContext(ctx)
	g := &errgroup.Group{}

	g.Go(func() error {
		err := p.MigrateRead()
		if err != nil {
			return err
		}
		return nil
	})

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

	err := g.Wait()
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
