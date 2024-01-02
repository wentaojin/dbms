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

	"github.com/wentaojin/dbms/ctl/database"

	"github.com/spf13/cobra"
)

type AppDatabase struct {
	*App
}

func (a *App) AppDatabase() Cmder {
	return &AppDatabase{App: a}
}

func (a *AppDatabase) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "database",
		Short:            "Operator cluster database",
		Long:             `Operator cluster database`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppDatabase) RunE(cmd *cobra.Command, args []string) error {

	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

type AppDatabaseUpsert struct {
	*AppDatabase
	config string
}

func (a *AppDatabase) AppDatabaseUpsert() Cmder {
	return &AppDatabaseUpsert{AppDatabase: a}
}

func (a *AppDatabaseUpsert) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "upsert",
		Short:            "upsert cluster database",
		Long:             `upsert cluster database`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.config, "config", "c", "config.toml", "database config")
	return cmd
}

func (a *AppDatabaseUpsert) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.config, "") {
		return fmt.Errorf("flag parameter [config] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := database.Upsert(a.Server, a.config)
	if err != nil {
		return err
	}
	return nil
}

type AppDatabaseDelete struct {
	*AppDatabase
	force bool
}

func (a *AppDatabase) AppDatabaseDelete() Cmder {
	return &AppDatabaseDelete{AppDatabase: a}
}

func (a *AppDatabaseDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "delete",
		Short:            "delete cluster database",
		Long:             `delete cluster database`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().BoolVarP(&a.force, "force", "f", false, "force delete")
	return cmd
}

func (a *AppDatabaseDelete) RunE(cmd *cobra.Command, args []string) error {
	if !a.force {
		return fmt.Errorf("flag parameter [force] is false, please again check, if confirm delete, please set force delete")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := database.Delete(a.Server)
	if err != nil {
		return err
	}

	fmt.Printf("Success Delete Database！！！\n")

	return nil
}

type AppDatabaseGet struct {
	*AppDatabase
}

func (a *AppDatabase) AppDatabaseGet() Cmder {
	return &AppDatabaseGet{AppDatabase: a}
}

func (a *AppDatabaseGet) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "get",
		Short:            "get cluster database",
		Long:             `get cluster database`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppDatabaseGet) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := database.Get(a.Server)
	if err != nil {
		return err
	}

	return nil
}
