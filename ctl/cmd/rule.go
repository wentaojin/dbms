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

type AppRule struct {
	*App
}

func (a *App) AppRule() Cmder {
	return &AppRule{App: a}
}

func (a *AppRule) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "rule",
		Short:            "Operator cluster task rule",
		Long:             `Operator cluster task rule`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppRule) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

type AppRuleUpsert struct {
	*AppRule
	config string
}

func (a *AppRule) AppRuleUpsert() Cmder {
	return &AppRuleUpsert{AppRule: a}
}

func (a *AppRuleUpsert) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "upsert",
		Short:            "upsert cluster task rule",
		Long:             `upsert cluster task rule`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.config, "config", "c", "config.toml", "datasource config")
	return cmd
}

func (a *AppRuleUpsert) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.config, "") {
		return fmt.Errorf("flag parameter [config] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.UpsertTaskRule(a.Server, a.config)
	if err != nil {
		return err
	}
	return nil
}

type AppRuleDelete struct {
	*AppRule
	name string
}

func (a *AppRule) AppRuleDelete() Cmder {
	return &AppRuleDelete{AppRule: a}
}

func (a *AppRuleDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "delete",
		Short:            "delete cluster task rule",
		Long:             `delete cluster task rule`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.name, "name", "n", "xxx", "delete struct task name")
	return cmd
}

func (a *AppRuleDelete) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.name, "") {
		return fmt.Errorf("flag parameter [name] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.DeleteTaskRule(a.Server, a.name)
	if err != nil {
		return err
	}
	fmt.Printf("Success Delete Task Rule [%v]！！！\n", a.name)
	return nil
}

type AppRuleGet struct {
	*AppRule
	name string
}

func (a *AppRule) AppRuleGet() Cmder {
	return &AppRuleGet{AppRule: a}
}

func (a *AppRuleGet) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "get",
		Short:            "get cluster task rule",
		Long:             `get cluster task rule`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppRuleGet) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	resp, err := migrate.GetTaskRule(a.Server, a.name)
	if err != nil {
		return err
	}

	fmt.Println(resp)
	return nil
}
