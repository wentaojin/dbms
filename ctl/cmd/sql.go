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
package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/ctl/migrate"
)

type AppSql struct {
	*App
}

func (a *App) AppSql() Cmder {
	return &AppSql{App: a}
}

func (a *AppSql) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "data",
		Short:            "Operator cluster sql migrate",
		Long:             `Operator cluster sql migrate`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppSql) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

type AppSqlUpsert struct {
	*AppSql
	config string
}

func (a *AppSql) AppSqlUpsert() Cmder {
	return &AppSqlUpsert{AppSql: a}
}

func (a *AppSqlUpsert) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "upsert",
		Short:            "upsert cluster sql migrate task",
		Long:             `upsert cluster sql migrate task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.config, "config", "c", "config.toml", "datasource config")
	return cmd
}

func (a *AppSqlUpsert) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.config, "") {
		return fmt.Errorf("flag parameter [config] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.UpsertSqlMigrate(a.Server, a.config)
	if err != nil {
		return err
	}
	return nil
}

type AppSqlDelete struct {
	*AppSql
	task string
}

func (a *AppSql) AppSqlDelete() Cmder {
	return &AppSqlDelete{AppSql: a}
}

func (a *AppSqlDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "delete",
		Short:            "delete cluster sql migrate task",
		Long:             `delete cluster sql migrate task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "xxx", "delete struct task task")
	return cmd
}

func (a *AppSqlDelete) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.task, "") {
		return fmt.Errorf("flag parameter [task] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.DeleteSqlMigrate(a.Server, a.task)
	if err != nil {
		return err
	}
	fmt.Printf("Success Delete Sql Migrate Task [%v]！！！\n", a.task)
	return nil
}

type AppSqlGet struct {
	*AppSql
	task string
}

func (a *AppSql) AppSqlGet() Cmder {
	return &AppSqlGet{AppSql: a}
}

func (a *AppSqlGet) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "get",
		Short:            "get cluster sql migrate task",
		Long:             `get cluster sql migrate task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppSqlGet) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.GetSqlMigrate(a.Server, a.task)
	if err != nil {
		return err
	}

	return nil
}
