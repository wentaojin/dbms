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
	"fmt"
	"path/filepath"
	"strings"

	"github.com/fatih/color"
	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/component"
	"github.com/wentaojin/dbms/component/cli/migrate"

	"github.com/wentaojin/dbms/service"

	"github.com/spf13/cobra"
)

type AppStruct struct {
	*App
}

func (a *App) AppStruct() component.Cmder {
	return &AppStruct{App: a}
}

func (a *AppStruct) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "struct",
		Short:            "Operator cluster struct migrate",
		Long:             `Operator cluster struct migrate`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppStruct) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

type AppStructUpsert struct {
	*AppStruct
	config string
}

func (a *AppStruct) AppStructUpsert() component.Cmder {
	return &AppStructUpsert{AppStruct: a}
}

func (a *AppStructUpsert) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "upsert",
		Short:            "upsert cluster struct migrate task",
		Long:             `upsert cluster struct migrate task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.config, "config", "c", "config.toml", "datasource config")
	return cmd
}

func (a *AppStructUpsert) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.config, "") {
		return fmt.Errorf("flag parameter [config] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.UpsertStructMigrate(a.Server, a.config)
	if err != nil {
		return err
	}
	return nil
}

type AppStructDelete struct {
	*AppStruct
	task string
}

func (a *AppStruct) AppStructDelete() component.Cmder {
	return &AppStructDelete{AppStruct: a}
}

func (a *AppStructDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "delete",
		Short:            "delete cluster struct task",
		Long:             `delete cluster struct task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "xxx", "delete struct task task")
	return cmd
}

func (a *AppStructDelete) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.task, "") {
		return fmt.Errorf("flag parameter [task] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.DeleteStructMigrate(a.Server, a.task)
	if err != nil {
		return err
	}
	return nil
}

type AppStructGet struct {
	*AppStruct
	task string
}

func (a *AppStruct) AppStructGet() component.Cmder {
	return &AppStructGet{AppStruct: a}
}

func (a *AppStructGet) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "get",
		Short:            "get cluster struct migrate task",
		Long:             `get cluster struct migrate task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "xxx", "get struct migrate task config")
	return cmd
}

func (a *AppStructGet) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.GetStructMigrate(a.Server, a.task)
	if err != nil {
		return err
	}

	return nil
}

type AppStructGen struct {
	*AppStruct
	task      string
	outputDir string
}

func (a *AppStruct) AppStructGen() component.Cmder {
	return &AppStructGen{AppStruct: a}
}

func (a *AppStructGen) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "gen",
		Short:            "gen cluster struct migrate task file",
		Long:             `gen cluster struct migrate task file`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "the struct migrate task")
	cmd.Flags().StringVarP(&a.outputDir, "outputDir", "o", "/tmp", "the struct migrate task output file dir")
	return cmd
}

func (a *AppStructGen) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	cyan := color.New(color.FgCyan, color.Bold)
	fmt.Printf("Component:    %s\n", cyan.Sprint("dbms-ctl"))
	fmt.Printf("Command:      %s\n", cyan.Sprint("struct"))
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

	err := service.GenStructMigrateTask(context.Background(), a.Server, a.task, a.outputDir)
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("the request failed: %v", err))
		return nil
	}

	fmt.Printf("Status:       %s\n", cyan.Sprint("success"))
	fmt.Printf("Response:     %s\n", color.GreenString("the struct migrate task ddl sql file had be output to [%v], please forward to view\n", a.outputDir))
	return nil
}
