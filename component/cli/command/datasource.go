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
	"github.com/wentaojin/dbms/component/cli/datasource"

	"github.com/spf13/cobra"
)

type AppDatasource struct {
	*App
}

func (a *App) AppDatasource() component.Cmder {
	return &AppDatasource{App: a}
}

func (a *AppDatasource) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "datasource",
		Short:            "Operator cluster datasource",
		Long:             `Operator cluster datasource`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppDatasource) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

type AppDatasourceUpsert struct {
	*AppDatasource
	config string
}

func (a *AppDatasource) AppDatasourceUpsert() component.Cmder {
	return &AppDatasourceUpsert{AppDatasource: a}
}

func (a *AppDatasourceUpsert) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "upsert",
		Short:            "upsert cluster datasource",
		Long:             `upsert cluster datasource`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.config, "config", "c", "config.toml", "datasource config")
	return cmd
}

func (a *AppDatasourceUpsert) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.config, "") {
		return fmt.Errorf("flag parameter [config] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := datasource.Upsert(a.Server, a.config)
	if err != nil {
		return err
	}
	return nil
}

type AppDatasourceDelete struct {
	*AppDatasource
	name []string
}

func (a *AppDatasource) AppDatasourceDelete() component.Cmder {
	return &AppDatasourceDelete{AppDatasource: a}
}

func (a *AppDatasourceDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "delete",
		Short:            "delete cluster datasource",
		Long:             `delete cluster datasource`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringSliceVarP(&a.name, "name", "n", []string{}, "delete datasource name")
	return cmd
}

func (a *AppDatasourceDelete) RunE(cmd *cobra.Command, args []string) error {
	if len(a.name) == 0 {
		return fmt.Errorf("flag parameter [name] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := datasource.Delete(a.Server, a.name)
	if err != nil {
		return err
	}
	return nil
}

type AppDatasourceGet struct {
	*AppDatasource
	name     string
	page     uint64
	pageSize uint64
}

func (a *AppDatasource) AppDatasourceGet() component.Cmder {
	return &AppDatasourceGet{AppDatasource: a}
}

func (a *AppDatasourceGet) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "get",
		Short:            "get cluster datasource config",
		Long:             `get cluster datasource config`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.name, "name", "n", "xxx", "get datasource config")
	return cmd
}

func (a *AppDatasourceGet) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	if strings.EqualFold(a.name, "") {
		return fmt.Errorf("flag parameter [name] is requirement, can not null")
	}

	err := datasource.Get(a.Server, a.name, a.page, a.pageSize)
	if err != nil {
		return err
	}

	return nil
}
