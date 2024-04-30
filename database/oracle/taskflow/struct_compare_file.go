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
package taskflow

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/golang/snappy"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/utils/stringutil"
)

type StructCompareFile struct {
	Ctx        context.Context `json:"-"`
	Mutex      *sync.Mutex     `json:"-"`
	CompFile   *os.File        `json:"-"`
	CompWriter *bufio.Writer   `json:"-"`
	TaskName   string          `json:"taskName"`
	TaskFlow   string          `json:"taskFlow"`
	OutputDir  string          `json:"outputDir"`
}

func NewStructCompareFile(ctx context.Context,
	taskName, taskFlow, outputDir string) *StructCompareFile {
	return &StructCompareFile{
		Ctx:       ctx,
		TaskName:  taskName,
		TaskFlow:  taskFlow,
		OutputDir: outputDir,
		Mutex:     &sync.Mutex{},
	}
}

func (s *StructCompareFile) InitFile() error {
	err := s.initOutputCompareFile()
	if err != nil {
		return err
	}
	return nil
}

func (s *StructCompareFile) SyncFile() error {
	var (
		err          error
		migrateTasks []*task.StructCompareTask
	)
	// get migrate task tables
	migrateTasks, err = model.GetIStructCompareTaskRW().FindStructCompareTask(s.Ctx, &task.StructCompareTask{
		TaskName: s.TaskName, TaskStatus: constant.TaskDatabaseStatusNotEqual})
	if err != nil {
		return err
	}
	if len(migrateTasks) == 0 {
		fmt.Printf("the struct compare task all of the table struct are equal, current not exist not equal table records.\n")
		return nil
	}

	var sqlComp strings.Builder
	for _, mt := range migrateTasks {
		if !strings.EqualFold(mt.CompareDetail, "") {
			desCompDetails, err := stringutil.Decrypt(mt.CompareDetail, []byte(constant.DefaultDataEncryptDecryptKey))
			if err != nil {
				return err
			}
			compareDetail, err := snappy.Decode(nil, []byte(desCompDetails))
			if err != nil {
				return err
			}
			sqlComp.WriteString(stringutil.BytesToString(compareDetail) + "\n\n")
		}
	}

	if !strings.EqualFold(sqlComp.String(), "") {
		_, err = s.writeCompareFile(sqlComp.String())
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *StructCompareFile) writeCompareFile(str string) (int, error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.CompWriter.WriteString(str)
}

func (s *StructCompareFile) initOutputCompareFile() error {
	outCompFile, err := os.OpenFile(filepath.Join(s.OutputDir, fmt.Sprintf("struct_compare_%s.sql", stringutil.StringLower(s.TaskName))), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	s.CompWriter, s.CompFile = bufio.NewWriter(outCompFile), outCompFile
	return nil
}

func (s *StructCompareFile) Close() error {
	if s.CompFile != nil {
		err := s.CompWriter.Flush()
		if err != nil {
			return err
		}
		err = s.CompFile.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
