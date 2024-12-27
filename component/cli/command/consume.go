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

	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/component"
	"github.com/wentaojin/dbms/component/cli/migrate"
)

type AppConsume struct {
	*App
}

func (a *App) AppConsume() component.Cmder {
	return &AppConsume{App: a}
}

func (a *AppConsume) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "consume",
		Short:            "Operator cluster cdc consume",
		Long:             `Operator cluster cdc consume`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppConsume) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

type AppConsumeUpsert struct {
	*AppConsume
	config string
}

func (a *AppConsume) AppConsumeUpsert() component.Cmder {
	return &AppConsumeUpsert{AppConsume: a}
}

func (a *AppConsumeUpsert) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "upsert",
		Short:            "upsert cluster cdc consume task",
		Long:             `upsert cluster cdc consume task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.config, "config", "c", "config.toml", "config")
	return cmd
}

func (a *AppConsumeUpsert) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.config, "") {
		return fmt.Errorf("flag parameter [config] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.UpsertCdcConsume(a.Server, a.config)
	if err != nil {
		return err
	}
	return nil
}

type AppConsumeDelete struct {
	*AppConsume
	task string
}

func (a *AppConsume) AppConsumeDelete() component.Cmder {
	return &AppConsumeDelete{AppConsume: a}
}

func (a *AppConsumeDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "delete",
		Short:            "delete cluster cdc consume task",
		Long:             `delete cluster cdc consume task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "xxx", "delete cdc consume task")
	return cmd
}

func (a *AppConsumeDelete) RunE(cmd *cobra.Command, args []string) error {
	if strings.EqualFold(a.task, "") {
		return fmt.Errorf("flag parameter [task] is requirement, can not null")
	}

	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.DeleteCdcConsume(a.Server, a.task)
	if err != nil {
		return err
	}
	return nil
}

type AppConsumeGet struct {
	*AppConsume
	task string
}

func (a *AppConsume) AppConsumeGet() component.Cmder {
	return &AppConsumeGet{AppConsume: a}
}

func (a *AppConsumeGet) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "get",
		Short:            "get cluster cdc consume task",
		Long:             `get cluster cdc consume task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "xxx", "get cdc consume task config")
	return cmd
}

func (a *AppConsumeGet) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}

	err := migrate.GetCdcConsume(a.Server, a.task)
	if err != nil {
		return err
	}

	return nil
}

type AppConsumeRewrite struct {
	*AppConsume
	task        string
	topic       string
	ddlDigest   string
	rewriteText string
}

func (a *AppConsume) AppConsumeRewrite() component.Cmder {
	return &AppConsumeRewrite{AppConsume: a}
}

func (a *AppConsumeRewrite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "rewrite",
		Short:            "rewrite cluster cdc consume task ddl",
		Long:             "rewrite cluster cdc consume task ddl",
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "xxx", "configure cdc consume task name (required)")
	cmd.Flags().StringVar(&a.topic, "topic", "topic", "configure cdc consume task topic name (required)")
	cmd.Flags().StringVarP(&a.ddlDigest, "digest", "g", "", "configure cdc consume task origin ddl text digest (required)")
	cmd.Flags().StringVarP(&a.rewriteText, "rewrite-text", "r", "", "configure cdc consume task ddl digest rewrite text")

	return cmd
}

func (a *AppConsumeRewrite) RunE(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		if err := cmd.Help(); err != nil {
			return err
		}
	}
	if !requireAllFlags(a.Server, a.task, a.topic, a.ddlDigest) {
		return fmt.Errorf("the flags reqiure [server、task、topic、ddl-digest] can not be null, please configure")
	}

	err := migrate.RewriteCdcConsume(a.Server, a.task, a.topic, a.ddlDigest, a.rewriteText)
	if err != nil {
		return err
	}
	return nil
}

func requireAllFlags(flags ...string) bool {
	for _, f := range flags {
		if strings.EqualFold(f, "") {
			return false
		}
	}
	return true
}
