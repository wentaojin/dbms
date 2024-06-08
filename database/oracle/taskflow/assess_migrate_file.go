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

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type AssessMigrateFile struct {
	Ctx        context.Context `json:"-"`
	Mutex      *sync.Mutex     `json:"-"`
	CompFile   *os.File        `json:"-"`
	CompWriter *bufio.Writer   `json:"-"`
	TaskName   string          `json:"taskName"`
	TaskFlow   string          `json:"taskFlow"`
	OutputDir  string          `json:"outputDir"`
}

func NewAssessMigrateFile(ctx context.Context,
	taskName, taskFlow, outputDir string) *AssessMigrateFile {
	return &AssessMigrateFile{
		Ctx:       ctx,
		TaskName:  taskName,
		TaskFlow:  taskFlow,
		OutputDir: outputDir,
		Mutex:     &sync.Mutex{},
	}
}

func (s *AssessMigrateFile) InitFile() error {
	err := s.initOutputAssessFile()
	if err != nil {
		return err
	}
	return nil
}

func (s *AssessMigrateFile) SyncFile() error {
	var (
		err          error
		migrateTasks []*task.AssessMigrateTask
	)
	// get migrate task tables
	migrateTasks, err = model.GetIAssessMigrateTaskRW().QueryAssessMigrateTask(s.Ctx, &task.AssessMigrateTask{
		TaskName: s.TaskName, TaskStatus: constant.TaskDatabaseStatusSuccess})
	if err != nil {
		return err
	}
	if len(migrateTasks) == 0 {
		fmt.Printf("the assess task success records aren't exist, maybe the task running failed or stopped, please checking.\n")
		return nil
	}

	var sqlComp strings.Builder

	for _, mt := range migrateTasks {
		desDelDetails, err := stringutil.Decrypt(mt.AssessDetail, []byte(constant.DefaultDataEncryptDecryptKey))
		if err != nil {
			return err
		}
		sqlComp.WriteString(desDelDetails)
	}

	if !strings.EqualFold(sqlComp.String(), "") {
		var report *Report
		err = stringutil.UnmarshalJSON([]byte(sqlComp.String()), &report)
		if err != nil {
			return err
		}
		err = GenNewHTMLReport(report, s.CompFile)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AssessMigrateFile) initOutputAssessFile() error {
	outCompFile, err := os.OpenFile(filepath.Join(s.OutputDir, fmt.Sprintf("assess_report_%s.html", s.TaskName)), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	s.CompWriter, s.CompFile = bufio.NewWriter(outCompFile), outCompFile
	return nil
}

func (s *AssessMigrateFile) Close() error {
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
