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

type AppStatement struct {
	*App
}

func (a *App) AppStatement() Cmder {
	return &AppStatement{App: a}
}

func (a *AppStatement) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "stmt",
		Short:            "Operator cluster statement migrate",
		Long:             `Operator cluster statement migrate`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppStatement) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

type AppStatementUpsert struct {
	*AppStatement
	config string
}

func (a *AppStatement) AppStatementUpsert() Cmder {
	return &AppStatementUpsert{AppStatement: a}
}

func (a *AppStatementUpsert) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "upsert",
		Short:            "upsert cluster statement migrate task",
		Long:             `upsert cluster statement migrate task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.config, "config", "c", "config.toml", "datasource config")
	return cmd
}

func (a *AppStatementUpsert) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.config, "") {
		return fmt.Errorf("flag parameter [config] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.UpsertStmtMigrate(a.Server, a.config)
	if err != nil {
		return err
	}
	return nil
}

type AppStatementDelete struct {
	*AppStatement
	task string
}

func (a *AppStatement) AppStatementDelete() Cmder {
	return &AppStatementDelete{AppStatement: a}
}

func (a *AppStatementDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "delete",
		Short:            "delete cluster statement migrate task",
		Long:             `delete cluster statement migrate task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "xxx", "delete struct task task")
	return cmd
}

func (a *AppStatementDelete) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.task, "") {
		return fmt.Errorf("flag parameter [task] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.DeleteStmtMigrate(a.Server, a.task)
	if err != nil {
		return err
	}
	fmt.Printf("Success Delete statement migrate Task [%v]！！！\n", a.task)
	return nil
}

type AppStatementGet struct {
	*AppStatement
	task string
}

func (a *AppStatement) AppStatementGet() Cmder {
	return &AppStatementGet{AppStatement: a}
}

func (a *AppStatementGet) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "get",
		Short:            "get cluster statement migrate task",
		Long:             `get cluster statement migrate task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppStatementGet) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.GetStmtMigrate(a.Server, a.task)
	if err != nil {
		return err
	}

	return nil
}
