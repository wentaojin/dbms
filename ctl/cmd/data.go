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

type AppData struct {
	*App
}

func (a *App) AppData() Cmder {
	return &AppData{App: a}
}

func (a *AppData) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "data",
		Short:            "Operator cluster data migrate",
		Long:             `Operator cluster data migrate`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppData) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

type AppDataUpsert struct {
	*AppData
	config string
}

func (a *AppData) AppDataUpsert() Cmder {
	return &AppDataUpsert{AppData: a}
}

func (a *AppDataUpsert) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "upsert",
		Short:            "upsert cluster data migrate task",
		Long:             `upsert cluster data migrate task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.config, "config", "c", "config.toml", "datasource config")
	return cmd
}

func (a *AppDataUpsert) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.config, "") {
		return fmt.Errorf("flag parameter [config] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.UpsertDataMigrate(a.Server, a.config)
	if err != nil {
		return err
	}
	return nil
}

type AppDataDelete struct {
	*AppData
	task string
}

func (a *AppData) AppDataDelete() Cmder {
	return &AppDataDelete{AppData: a}
}

func (a *AppDataDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "delete",
		Short:            "delete cluster data migrate task",
		Long:             `delete cluster data migrate task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "xxx", "delete struct task task")
	return cmd
}

func (a *AppDataDelete) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.task, "") {
		return fmt.Errorf("flag parameter [task] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.DeleteDataMigrate(a.Server, a.task)
	if err != nil {
		return err
	}
	fmt.Printf("Success Delete Data Migrate Task [%v]！！！\n", a.task)
	return nil
}

type AppDataGet struct {
	*AppData
	task string
}

func (a *AppData) AppDataGet() Cmder {
	return &AppDataGet{AppData: a}
}

func (a *AppDataGet) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "get",
		Short:            "get cluster data migrate task",
		Long:             `get cluster data migrate task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppDataGet) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.GetDataMigrate(a.Server, a.task)
	if err != nil {
		return err
	}

	return nil
}
