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
	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/component"
	"github.com/wentaojin/dbms/component/cli/migrate"
	"github.com/wentaojin/dbms/service"
	"strings"
)

type AppScan struct {
	*App
}

func (a *App) AppScan() component.Cmder {
	return &AppScan{App: a}
}

func (a *AppScan) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "scan",
		Short:            "Operator cluster data scan",
		Long:             `Operator cluster data scan`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppScan) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

type AppScanUpsert struct {
	*AppScan
	config string
}

func (a *AppScan) AppScanUpsert() component.Cmder {
	return &AppScanUpsert{AppScan: a}
}

func (a *AppScanUpsert) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "upsert",
		Short:            "upsert cluster data scan task",
		Long:             `upsert cluster data scan task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.config, "config", "c", "config.toml", "config")
	return cmd
}

func (a *AppScanUpsert) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.config, "") {
		return fmt.Errorf("flag parameter [config] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.UpsertDataScan(a.Server, a.config)
	if err != nil {
		return err
	}
	return nil
}

type AppScanDelete struct {
	*AppScan
	task string
}

func (a *AppScan) AppScanDelete() component.Cmder {
	return &AppScanDelete{AppScan: a}
}

func (a *AppScanDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "delete",
		Short:            "delete cluster data scan task",
		Long:             `delete cluster data scan task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "xxx", "delete data scan task")
	return cmd
}

func (a *AppScanDelete) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.task, "") {
		return fmt.Errorf("flag parameter [task] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.DeleteDataScan(a.Server, a.task)
	if err != nil {
		return err
	}
	fmt.Printf("Success Delete data scan Task [%v]！！！\n", a.task)
	return nil
}

type AppScanGet struct {
	*AppScan
	task string
}

func (a *AppScan) AppScanGet() component.Cmder {
	return &AppScanGet{AppScan: a}
}

func (a *AppScanGet) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "get",
		Short:            "get cluster data scan task config",
		Long:             `get cluster data scan task config`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "xxx", "get data scan task config")
	return cmd
}

func (a *AppScanGet) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.GetDataScan(a.Server, a.task)
	if err != nil {
		return err
	}

	return nil
}

type AppScanGen struct {
	*AppScan
	task      string
	outputDir string
}

func (a *AppScan) AppScanGen() component.Cmder {
	return &AppScanGen{AppScan: a}
}

func (a *AppScanGen) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "gen",
		Short:            "gen cluster data scan task detail",
		Long:             `gen cluster data scan task detail`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "the data scan task")
	cmd.Flags().StringVarP(&a.outputDir, "outputDir", "o", "/tmp", "the data scan task output file dir")
	return cmd
}

func (a *AppScanGen) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}
	if strings.EqualFold(a.task, "") || strings.EqualFold(a.outputDir, "") {
		return fmt.Errorf("flag parameter [task] and [outputDir] are requirement, can not null")
	}

	err := service.GenDataScanTask(context.Background(), a.Server, a.task, a.outputDir)
	if err != nil {
		return err
	}
	return nil
}
