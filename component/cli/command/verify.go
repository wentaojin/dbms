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
package command

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/fatih/color"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/component"
	"github.com/wentaojin/dbms/component/cli/migrate"

	"github.com/wentaojin/dbms/service"

	"github.com/spf13/cobra"
)

type AppVerify struct {
	*App
}

func (a *App) AppVerify() component.Cmder {
	return &AppVerify{App: a}
}

func (a *AppVerify) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "verify",
		Short:            "Operator cluster data compare",
		Long:             `Operator cluster data compare`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppVerify) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

type AppVerifyUpsert struct {
	*AppVerify
	config string
}

func (a *AppVerify) AppVerifyUpsert() component.Cmder {
	return &AppVerifyUpsert{AppVerify: a}
}

func (a *AppVerifyUpsert) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "upsert",
		Short:            "upsert cluster data compare task",
		Long:             `upsert cluster data compare task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.config, "config", "c", "config.toml", "config")
	return cmd
}

func (a *AppVerifyUpsert) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.config, "") {
		return fmt.Errorf("flag parameter [config] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.UpsertDataCompare(a.Server, a.config)
	if err != nil {
		return err
	}
	return nil
}

type AppVerifyDelete struct {
	*AppVerify
	task string
}

func (a *AppVerify) AppVerifyDelete() component.Cmder {
	return &AppVerifyDelete{AppVerify: a}
}

func (a *AppVerifyDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "delete",
		Short:            "delete cluster data compare task",
		Long:             `delete cluster data compare task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "xxx", "delete data compare task")
	return cmd
}

func (a *AppVerifyDelete) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.task, "") {
		return fmt.Errorf("flag parameter [task] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.DeleteDataCompare(a.Server, a.task)
	if err != nil {
		return err
	}
	return nil
}

type AppVerifyGet struct {
	*AppVerify
	task string
}

func (a *AppVerify) AppVerifyGet() component.Cmder {
	return &AppVerifyGet{AppVerify: a}
}

func (a *AppVerifyGet) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "get",
		Short:            "get cluster data compare task",
		Long:             `get cluster data compare task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "xxx", "get data verify task config")
	return cmd
}

func (a *AppVerifyGet) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.GetDataCompare(a.Server, a.task)
	if err != nil {
		return err
	}

	return nil
}

type AppVerifyGen struct {
	*AppVerify
	task         string
	schemaName   string
	tableName    string
	outputDir    string
	ignoreStatus bool
	ignoreVerify bool
}

func (a *AppVerify) AppVerifyGen() component.Cmder {
	return &AppVerifyGen{AppVerify: a}
}

func (a *AppVerifyGen) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "gen",
		Short:            "gen cluster data compare task detail",
		Long:             `gen cluster data compare task detail`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "the data compare task")
	cmd.Flags().StringVarP(&a.schemaName, "schema", "S", "", "the data compare task schema_name_s")
	cmd.Flags().StringVarP(&a.tableName, "table", "T", "", "the data compare task schema table_name_s")
	cmd.Flags().StringVarP(&a.outputDir, "outputDir", "o", "/tmp", "the data compare task output file dir")
	cmd.Flags().BoolVar(&a.ignoreStatus, "ignoreStatus", false, "forces the consistency check of unsuccessful task status and continue output fixed file")
	cmd.Flags().BoolVar(&a.ignoreVerify, "ignoreVerify", false, "forced to ignore chunk failure records to check if there is a repair statement and continue output fixed file")
	return cmd
}

func (a *AppVerifyGen) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	cyan := color.New(color.FgCyan, color.Bold)
	fmt.Printf("Component:    %s\n", cyan.Sprint("dbms-ctl"))
	fmt.Printf("Command:      %s\n", cyan.Sprint("verify"))
	fmt.Printf("Task:         %s\n", cyan.Sprint(a.task))
	fmt.Printf("Action:       %s\n", cyan.Sprint("gen"))
	if strings.EqualFold(a.task, "") || strings.EqualFold(a.outputDir, "") {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("flag parameter [task] and [outputDir] are requirement, can not null"))
		return nil
	}

	if filepath.IsAbs(a.outputDir) {
		abs, err := filepath.Abs(a.outputDir)
		if err != nil {
			return err
		}
		err = stringutil.PathNotExistOrCreate(abs)
		if err != nil {
			return err
		}
	} else {
		err := stringutil.PathNotExistOrCreate(a.outputDir)
		if err != nil {
			return err
		}
	}

	err := service.GenDataCompareTask(context.Background(), a.Server, a.task, a.schemaName, a.tableName, a.outputDir, a.ignoreStatus, a.ignoreVerify)
	if err != nil {
		if errors.Is(err, errors.New(constant.TaskDatabaseStatusEqual)) {
			fmt.Printf("Status:       %s\n", cyan.Sprint("success"))
			fmt.Printf("Response:     %s\n", color.GreenString("the data compare task all of the table records are equal, current not exist not equal table records."))
			return nil
		}
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("the request failed: \n%v", err))
		return nil
	}
	fmt.Printf("Status:       %s\n", cyan.Sprint("success"))
	fmt.Printf("Response:     %s\n", color.GreenString("the data compare task fixed sql file had be output to [%v], please forward to view\n", a.outputDir))
	return nil
}

type AppVerifyScan struct {
	*AppVerify
	task               string
	schemaName         string
	tableName          string
	chunkIDs           []string
	outputDir          string
	force              bool
	callTimeout        int64
	chunkThreads       int
	stream             string
	disableCharsetConv bool
}

func (a *AppVerify) AppVerifyScan() component.Cmder {
	return &AppVerifyScan{AppVerify: a}
}

func (a *AppVerifyScan) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "scan",
		Short:            "scan for unequal chunk records caused by crc32 or md5 data verification methods tasks",
		Long:             `scan for unequal chunk records caused by crc32 or md5 data verification methods tasks`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "the data compare task")
	cmd.Flags().StringVarP(&a.schemaName, "schema_name_s", "S", "", "the data compare task schema_name_s")
	cmd.Flags().StringVarP(&a.tableName, "table_name_s", "T", "", "the data compare task schema table_name_s")
	cmd.Flags().StringVarP(&a.outputDir, "outputDir", "o", "/tmp", "the data compare task output file dir")
	cmd.Flags().StringVar(&a.stream, "stream", "upstream", "refer to the upstream and downstream of the datasource of the data verification task to control whether the data scanning task runs upstream or downstream, options: [upstream / downstream]")
	cmd.Flags().StringArrayVarP(&a.chunkIDs, "chunkIds", "c", nil, "the data compare task table not equal chunk ids")
	cmd.Flags().BoolVarP(&a.force, "force", "f", false, "the data compare task force ignore the task status success check, output file")
	cmd.Flags().Int64Var(&a.callTimeout, "timeout", 36000, "the data compare task query timeout, unit: seconds")
	cmd.Flags().IntVar(&a.chunkThreads, "thread", 5, "the data compare task scan threads")
	cmd.Flags().BoolVar(&a.disableCharsetConv, "disable-charset-conv", false, "the data check process prohibits data character set conversion and retains the original character set scanning")

	return cmd
}

func (a *AppVerifyScan) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	cyan := color.New(color.FgCyan, color.Bold)
	fmt.Printf("Component:    %s\n", cyan.Sprint("dbms-ctl"))
	fmt.Printf("Command:      %s\n", cyan.Sprint("verify"))
	fmt.Printf("Task:         %s\n", cyan.Sprint(a.task))
	fmt.Printf("Action:       %s\n", cyan.Sprint("scan"))
	if strings.EqualFold(a.task, "") || strings.EqualFold(a.outputDir, "") {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("flag parameter [task] and [outputDir] are requirement, can not null"))
		return nil
	}

	if filepath.IsAbs(a.outputDir) {
		abs, err := filepath.Abs(a.outputDir)
		if err != nil {
			return err
		}
		err = stringutil.PathNotExistOrCreate(abs)
		if err != nil {
			return err
		}
	} else {
		err := stringutil.PathNotExistOrCreate(a.outputDir)
		if err != nil {
			return err
		}
	}

	err := service.ScanDataCompareTask(context.Background(),
		a.Server,
		a.task,
		a.schemaName,
		a.tableName, a.chunkIDs, a.outputDir, a.force,
		a.callTimeout, a.chunkThreads, a.stream, a.disableCharsetConv)
	if err != nil {
		if errors.Is(err, errors.New(constant.TaskDatabaseStatusEqual)) {
			fmt.Printf("Status:       %s\n", cyan.Sprint("success"))
			fmt.Printf("Response:     %s\n", color.GreenString("the data compare task all of the table records are equal, current not exist not equal table chunk records."))
			return nil
		}
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("the request failed: %v", err))
		return nil
	}
	fmt.Printf("Status:       %s\n", cyan.Sprint("success"))
	fmt.Printf("Response:     %s\n", color.GreenString("the data compare task scan chunk records had be output to [%v], please forward to view\n", a.outputDir))
	return nil
}
