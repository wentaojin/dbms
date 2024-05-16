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
	"fmt"
	"strings"

	"github.com/wentaojin/dbms/component"
	"github.com/wentaojin/dbms/component/ctl/migrate"

	"github.com/spf13/cobra"
)

type AppCsv struct {
	*App
}

func (a *App) AppCsv() component.Cmder {
	return &AppCsv{App: a}
}

func (a *AppCsv) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "csv",
		Short:            "Operator cluster csv migrate",
		Long:             `Operator cluster csv migrate`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppCsv) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

type AppCsvUpsert struct {
	*AppCsv
	config string
}

func (a *AppCsv) AppCsvUpsert() component.Cmder {
	return &AppCsvUpsert{AppCsv: a}
}

func (a *AppCsvUpsert) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "upsert",
		Short:            "upsert cluster csv migrate task",
		Long:             `upsert cluster csv migrate task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.config, "config", "c", "config.toml", "datasource config")
	return cmd
}

func (a *AppCsvUpsert) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.config, "") {
		return fmt.Errorf("flag parameter [config] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.UpsertCsvMigrate(a.Server, a.config)
	if err != nil {
		return err
	}
	return nil
}

type AppCsvDelete struct {
	*AppCsv
	task string
}

func (a *AppCsv) AppCsvDelete() component.Cmder {
	return &AppCsvDelete{AppCsv: a}
}

func (a *AppCsvDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "delete",
		Short:            "delete cluster csv migrate task",
		Long:             `delete cluster csv migrate task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "xxx", "delete struct task task")
	return cmd
}

func (a *AppCsvDelete) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.task, "") {
		return fmt.Errorf("flag parameter [task] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.DeleteCsvMigrate(a.Server, a.task)
	if err != nil {
		return err
	}
	fmt.Printf("Success Delete Csv Migrate Task [%v]！！！\n", a.task)
	return nil
}

type AppCsvGet struct {
	*AppCsv
	task string
}

func (a *AppCsv) AppCsvGet() component.Cmder {
	return &AppCsvGet{AppCsv: a}
}

func (a *AppCsvGet) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "get",
		Short:            "get cluster csv migrate task",
		Long:             `get cluster csv migrate task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppCsvGet) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.GetCsvMigrate(a.Server, a.task)
	if err != nil {
		return err
	}

	return nil
}
