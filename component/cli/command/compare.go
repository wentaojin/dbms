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
	"github.com/fatih/color"
	"strings"

	"github.com/wentaojin/dbms/component"
	"github.com/wentaojin/dbms/component/cli/migrate"

	"github.com/wentaojin/dbms/service"

	"github.com/spf13/cobra"
)

type AppCompare struct {
	*App
}

func (a *App) AppCompare() component.Cmder {
	return &AppCompare{App: a}
}

func (a *AppCompare) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "compare",
		Short:            "Operator cluster struct compare",
		Long:             `Operator cluster struct compare`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppCompare) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

type AppCompareUpsert struct {
	*AppCompare
	config string
}

func (a *AppCompare) AppCompareUpsert() component.Cmder {
	return &AppCompareUpsert{AppCompare: a}
}

func (a *AppCompareUpsert) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "upsert",
		Short:            "upsert cluster struct compare task",
		Long:             `upsert cluster struct compare task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.config, "config", "c", "config.toml", "config")
	return cmd
}

func (a *AppCompareUpsert) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.config, "") {
		return fmt.Errorf("flag parameter [config] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.UpsertStructCompare(a.Server, a.config)
	if err != nil {
		return err
	}
	return nil
}

type AppCompareDelete struct {
	*AppCompare
	task string
}

func (a *AppCompare) AppCompareDelete() component.Cmder {
	return &AppCompareDelete{AppCompare: a}
}

func (a *AppCompareDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "delete",
		Short:            "delete cluster struct compare task",
		Long:             `delete cluster struct compare task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "xxx", "delete struct compare task")
	return cmd
}

func (a *AppCompareDelete) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.task, "") {
		return fmt.Errorf("flag parameter [task] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.DeleteStructCompare(a.Server, a.task)
	if err != nil {
		return err
	}
	return nil
}

type AppCompareGet struct {
	*AppCompare
	task string
}

func (a *AppCompare) AppCompareGet() component.Cmder {
	return &AppCompareGet{AppCompare: a}
}

func (a *AppCompareGet) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "get",
		Short:            "get cluster struct compare task",
		Long:             `get cluster struct compare task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "xxx", "get struct compare task config")
	return cmd
}

func (a *AppCompareGet) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.GetStructCompare(a.Server, a.task)
	if err != nil {
		return err
	}

	return nil
}

type AppCompareGen struct {
	*AppCompare
	task      string
	outputDir string
}

func (a *AppCompare) AppCompareGen() component.Cmder {
	return &AppCompareGen{AppCompare: a}
}

func (a *AppCompareGen) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "gen",
		Short:            "gen cluster struct compare task detail",
		Long:             `gen cluster struct compare task detail`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "the struct compare task")
	cmd.Flags().StringVarP(&a.outputDir, "outputDir", "o", "/tmp", "the struct compare task output file dir")
	return cmd
}

func (a *AppCompareGen) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	cyan := color.New(color.FgCyan, color.Bold)
	fmt.Printf("Component:    %s\n", cyan.Sprint("dbms-ctl"))
	fmt.Printf("Command:      %s\n", cyan.Sprint("compare"))
	fmt.Printf("Task:         %s\n", cyan.Sprint(a.task))
	fmt.Printf("Action:       %s\n", cyan.Sprint("gen"))

	if strings.EqualFold(a.task, "") || strings.EqualFold(a.outputDir, "") {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("flag parameter [task] and [outputDir] are requirement, can not null"))
		return nil
	}

	err := service.GenStructCompareTask(context.Background(), a.Server, a.task, a.outputDir)
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("the request failed: %v", err))
		return nil
	}

	fmt.Printf("Status:       %s\n", cyan.Sprint("success"))
	fmt.Printf("Response:     %s\n", color.GreenString("the struct compare task ddl sql file had be output to [%v], please forward to view\n", a.outputDir))
	return nil
}
