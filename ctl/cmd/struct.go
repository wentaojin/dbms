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

	"github.com/wentaojin/dbms/ctl/migrate"

	"github.com/spf13/cobra"
)

type AppStruct struct {
	*App
}

func (a *App) AppStruct() Cmder {
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

func (a *AppStruct) AppStructUpsert() Cmder {
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
	name string
}

func (a *AppStruct) AppStructDelete() Cmder {
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
	cmd.Flags().StringVarP(&a.name, "name", "n", "xxx", "delete struct task name")
	return cmd
}

func (a *AppStructDelete) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.name, "") {
		return fmt.Errorf("flag parameter [name] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.DeleteStructMigrate(a.Server, a.name)
	if err != nil {
		return err
	}
	fmt.Printf("Success Delete Struct Migrate Task [%v]！！！\n", a.name)
	return nil
}

type AppStructGet struct {
	*AppStruct
	name string
}

func (a *AppStruct) AppStructGet() Cmder {
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
	return cmd
}

func (a *AppStructGet) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	resp, err := migrate.GetStructMigrate(a.Server, a.name)
	if err != nil {
		return err
	}

	fmt.Println(resp)
	return nil
}
