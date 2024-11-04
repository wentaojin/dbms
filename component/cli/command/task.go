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
	"strings"

	"github.com/fatih/color"
	"github.com/wentaojin/dbms/service"

	"github.com/wentaojin/dbms/component"

	"github.com/wentaojin/dbms/utils/constant"

	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/openapi"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type AppTask struct {
	*App
}

func (a *App) AppTask() component.Cmder {
	return &AppTask{App: a}
}

func (a *AppTask) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "task",
		Short:            "Operator cluster task",
		Long:             `Operator cluster task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppTask) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

type AppTaskStart struct {
	*AppTask
	task       string
	assginHost string
}

func (a *AppTask) AppTaskStart() component.Cmder {
	return &AppTaskStart{AppTask: a}
}

func (a *AppTaskStart) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "start",
		Short:            "start cluster task",
		Long:             `start cluster task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "operate task name")
	cmd.Flags().StringVarP(&a.assginHost, "hostIP", "H", "", "configure assign host")
	return cmd
}

func (a *AppTaskStart) RunE(cmd *cobra.Command, args []string) error {
	cyan := color.New(color.FgCyan, color.Bold)
	fmt.Printf("Component:    %s\n", cyan.Sprint("dbms-ctl"))
	fmt.Printf("Command:      %s\n", cyan.Sprint("task"))
	fmt.Printf("Action:       %s\n", cyan.Sprint("start"))
	fmt.Printf("Task:         %s\n", cyan.Sprint(a.task))

	if strings.EqualFold(a.task, "") {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("operate task flag [task] can't be null, please setting"))
		return nil
	}

	bodyReq := make(map[string]interface{})
	bodyReq["taskName"] = a.task
	bodyReq["operate"] = constant.TaskOperationStart
	bodyReq["express"] = ""
	bodyReq["assignHost"] = a.assginHost

	jsonStr, err := stringutil.MarshalJSON(bodyReq)
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("error marshal JSON: %v", err))
		return nil
	}
	resp, err := openapi.Request(openapi.RequestPOSTMethod, stringutil.StringBuilder(stringutil.WrapScheme(a.Server, false), openapi.DBMSAPIBasePath, openapi.APITaskPath), []byte(jsonStr))
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("the request failed: %v", err))
		return nil
	}

	var jsonData map[string]interface{}
	err = stringutil.UnmarshalJSON(resp, &jsonData)
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("error decoding JSON: %v", err))
		return nil
	}

	formattedJSON, err := stringutil.MarshalIndentJSON(stringutil.FormatJSONFields(jsonData))
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("error decoding JSON: %v", err))
		return nil
	}
	fmt.Printf("Status:       %s\n", cyan.Sprint("success"))
	fmt.Printf("Response:     %s\n", formattedJSON)
	return nil
}

type AppTaskStop struct {
	*AppTask
	task string
}

func (a *AppTask) AppTaskStop() component.Cmder {
	return &AppTaskStop{AppTask: a}
}

func (a *AppTaskStop) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "stop",
		Short:            "stop cluster task",
		Long:             `stop cluster task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "operate task name")
	return cmd
}

func (a *AppTaskStop) RunE(cmd *cobra.Command, args []string) error {
	cyan := color.New(color.FgCyan, color.Bold)
	fmt.Printf("Component:    %s\n", cyan.Sprint("dbms-ctl"))
	fmt.Printf("Command:      %s\n", cyan.Sprint("task"))
	fmt.Printf("Action:       %s\n", cyan.Sprint("stop"))
	fmt.Printf("Task:         %s\n", cyan.Sprint(a.task))

	if strings.EqualFold(a.task, "") {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("operate task flag [task] can't be null, please setting"))
		return nil
	}

	bodyReq := make(map[string]interface{})
	bodyReq["taskName"] = a.task
	bodyReq["operate"] = constant.TaskOperationStop
	bodyReq["express"] = ""
	bodyReq["assignHost"] = ""

	jsonStr, err := stringutil.MarshalJSON(bodyReq)
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("error marshal JSON: %v", err))
		return nil
	}
	resp, err := openapi.Request(openapi.RequestPOSTMethod, stringutil.StringBuilder(stringutil.WrapScheme(a.Server, false), openapi.DBMSAPIBasePath, openapi.APITaskPath), []byte(jsonStr))
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("the request failed: %v", err))
		return nil
	}

	var jsonData map[string]interface{}
	err = stringutil.UnmarshalJSON(resp, &jsonData)
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("error decoding JSON: %v", err))
		return nil
	}

	formattedJSON, err := stringutil.MarshalIndentJSON(stringutil.FormatJSONFields(jsonData))
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("error decoding JSON: %v", err))
		return nil
	}
	fmt.Printf("Status:       %s\n", cyan.Sprint("success"))
	fmt.Printf("Response:     %s\n", formattedJSON)
	return nil
}

type AppTaskDelete struct {
	*AppTask
	task string
}

func (a *AppTask) AppTaskDelete() component.Cmder {
	return &AppTaskDelete{AppTask: a}
}

func (a *AppTaskDelete) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "delete",
		Short:            "delete cluster non-crontab task",
		Long:             `delete cluster non-crontab task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "operate task name")
	return cmd
}

func (a *AppTaskDelete) RunE(cmd *cobra.Command, args []string) error {
	cyan := color.New(color.FgCyan, color.Bold)
	fmt.Printf("Component:    %s\n", cyan.Sprint("dbms-ctl"))
	fmt.Printf("Command:      %s\n", cyan.Sprint("task"))
	fmt.Printf("Action:       %s\n", cyan.Sprint("delete"))
	fmt.Printf("Task:         %s\n", cyan.Sprint(a.task))

	if strings.EqualFold(a.task, "") {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("operate task flag [task] can't be null, please setting"))
		return nil
	}
	bodyReq := make(map[string]interface{})
	bodyReq["taskName"] = a.task
	bodyReq["operate"] = constant.TaskOperationDelete
	bodyReq["express"] = ""
	bodyReq["assignHost"] = ""

	jsonStr, err := stringutil.MarshalJSON(bodyReq)
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("error marshal JSON: %v", err))
		return nil
	}
	resp, err := openapi.Request(openapi.RequestPOSTMethod, stringutil.StringBuilder(stringutil.WrapScheme(a.Server, false), openapi.DBMSAPIBasePath, openapi.APITaskPath), []byte(jsonStr))
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("the request failed: %v", err))
		return nil
	}

	var jsonData map[string]interface{}
	err = stringutil.UnmarshalJSON(resp, &jsonData)
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("error decoding JSON: %v", err))
		return nil
	}

	formattedJSON, err := stringutil.MarshalIndentJSON(stringutil.FormatJSONFields(jsonData))
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("error decoding JSON: %v", err))
		return nil
	}
	fmt.Printf("Status:       %s\n", cyan.Sprint("success"))
	fmt.Printf("Response:     %s\n", formattedJSON)
	return nil
}

type AppTaskStatus struct {
	*AppTask
	task string
	last int
}

func (a *AppTask) AppTaskStatus() component.Cmder {
	return &AppTaskStatus{AppTask: a}
}

func (a *AppTaskStatus) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "status",
		Short:            "get cluster task status log information",
		Long:             `get cluster task status log information`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "configure the task name")
	cmd.Flags().IntVarP(&a.last, "last", "l", 5, "configure the last N task log records")
	return cmd
}

func (a *AppTaskStatus) RunE(cmd *cobra.Command, args []string) error {
	cyan := color.New(color.FgCyan, color.Bold)
	fmt.Printf("Component:    %s\n", cyan.Sprint("dbms-ctl"))
	fmt.Printf("Command:      %s\n", cyan.Sprint("task"))
	fmt.Printf("Action:       %s\n", cyan.Sprint("status"))
	fmt.Printf("Task:         %s\n", cyan.Sprint(a.task))

	if strings.EqualFold(a.task, "") {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("operate task flag [task] can't be null, please setting"))
		return nil
	}

	status, err := service.StatusTask(context.TODO(), a.task, a.Server, a.last)
	if err != nil {
		return err
	}

	formattedJSON, err := stringutil.MarshalIndentJSON(status)
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("error decoding JSON: %v", err))
		return nil
	}
	fmt.Printf("Status:       %s\n", cyan.Sprint("success"))
	fmt.Printf("Response:     %s\n", formattedJSON)
	return nil
}

type AppTaskList struct {
	*AppTask
	task string
}

func (a *AppTask) AppTaskList() component.Cmder {
	return &AppTaskList{AppTask: a}
}

func (a *AppTaskList) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "list",
		Short:            "list cluster task information",
		Long:             `list cluster task information`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "configure the task name")
	return cmd
}

func (a *AppTaskList) RunE(cmd *cobra.Command, args []string) error {
	cyan := color.New(color.FgCyan, color.Bold)
	fmt.Printf("Component:    %s\n", cyan.Sprint("dbms-ctl"))
	fmt.Printf("Command:      %s\n", cyan.Sprint("task"))
	fmt.Printf("Action:       %s\n", cyan.Sprint("list"))
	if strings.EqualFold(a.task, "") {
		fmt.Printf("Task:         %s\n", cyan.Sprint("all"))
	} else {
		fmt.Printf("Task:         %s\n", cyan.Sprint(a.task))
	}

	tasks, err := service.ListTask(context.TODO(), a.task, a.Server)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		fmt.Printf("Status:       %s\n", cyan.Sprint("success"))
		fmt.Printf("Response:     %s\n", "the task record is empty, please confirm whether had submit task or reconfigure the task name")
	}

	formattedJSON, err := stringutil.MarshalIndentJSON(tasks)
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("error decoding JSON: %v", err))
		return nil
	}
	fmt.Printf("Status:       %s\n", cyan.Sprint("success"))
	fmt.Printf("Response:     %s\n", formattedJSON)
	return nil
}

type AppTaskCrontab struct {
	*AppTask
}

func (a *AppTask) AppTaskCrontab() component.Cmder {
	return &AppTaskCrontab{AppTask: a}
}

func (a *AppTaskCrontab) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "crontab",
		Short:            "crontab cluster task",
		Long:             `crontab cluster task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	return cmd
}

func (a *AppTaskCrontab) RunE(cmd *cobra.Command, args []string) error {
	if err := cmd.Help(); err != nil {
		return err
	}
	return nil
}

type AppTaskCrontabSubmit struct {
	*AppTaskCrontab
	task       string
	express    string
	assignHost string
}

func (a *AppTaskCrontab) AppTaskCrontabSubmit() component.Cmder {
	return &AppTaskCrontabSubmit{AppTaskCrontab: a}
}

func (a *AppTaskCrontabSubmit) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "submit",
		Short:            "submit the crontab cluster task",
		Long:             `submit the crontab cluster task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "operate task name")
	cmd.Flags().StringVarP(&a.express, "express", "e", "", "crontab task setting")
	cmd.Flags().StringVarP(&a.assignHost, "hostIP", "H", "", "crontab task assign host")
	return cmd
}

func (a *AppTaskCrontabSubmit) RunE(cmd *cobra.Command, args []string) error {
	cyan := color.New(color.FgCyan, color.Bold)
	fmt.Printf("Component:    %s\n", cyan.Sprint("dbms-ctl"))
	fmt.Printf("Command:      %s\n", cyan.Sprint("task crontab"))
	fmt.Printf("Action:       %s\n", cyan.Sprint("submit"))
	fmt.Printf("Task:         %s\n", cyan.Sprint(a.task))

	if strings.EqualFold(a.task, "") {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("operate task flag [task] can't be null, please setting"))
		return nil
	}

	if strings.EqualFold(a.express, "") {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("operate task flag [express] can't be null, please setting"))
		return nil
	}

	bodyReq := make(map[string]interface{})
	bodyReq["taskName"] = a.task
	bodyReq["operate"] = constant.TaskOperationCrontabSubmit
	bodyReq["express"] = a.express
	bodyReq["assignHost"] = a.assignHost

	jsonStr, err := stringutil.MarshalJSON(bodyReq)
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("error marshal JSON: %v", err))
		return nil
	}
	resp, err := openapi.Request(openapi.RequestPOSTMethod, stringutil.StringBuilder(stringutil.WrapScheme(a.Server, false), openapi.DBMSAPIBasePath, openapi.APITaskPath), []byte(jsonStr))
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("the request failed: %v", err))
		return nil
	}

	var jsonData map[string]interface{}
	err = stringutil.UnmarshalJSON(resp, &jsonData)
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("error decoding JSON: %v", err))
		return nil
	}

	formattedJSON, err := stringutil.MarshalIndentJSON(stringutil.FormatJSONFields(jsonData))
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("error decoding JSON: %v", err))
		return nil
	}
	fmt.Printf("Status:       %s\n", cyan.Sprint("success"))
	fmt.Printf("Response:     %s\n", formattedJSON)
	return nil
}

type AppTaskCrontabClear struct {
	*AppTaskCrontab
	task string
}

func (a *AppTaskCrontab) AppTaskCrontabClear() component.Cmder {
	return &AppTaskCrontabClear{AppTaskCrontab: a}
}

func (a *AppTaskCrontabClear) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "clear",
		Short:            "clear the crontab cluster task",
		Long:             `clear the crontab cluster task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "operate task name")
	return cmd
}

func (a *AppTaskCrontabClear) RunE(cmd *cobra.Command, args []string) error {
	cyan := color.New(color.FgCyan, color.Bold)
	fmt.Printf("Component:    %s\n", cyan.Sprint("dbms-ctl"))
	fmt.Printf("Command:      %s\n", cyan.Sprint("task crontab"))
	fmt.Printf("Action:       %s\n", cyan.Sprint("clear"))
	fmt.Printf("Task:         %s\n", cyan.Sprint(a.task))

	if strings.EqualFold(a.task, "") {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("operate task flag [task] can't be null, please setting"))
		return nil
	}
	bodyReq := make(map[string]interface{})
	bodyReq["taskName"] = a.task
	bodyReq["operate"] = constant.TaskOperationCrontabClear
	bodyReq["express"] = ""
	bodyReq["assignHost"] = ""

	jsonStr, err := stringutil.MarshalJSON(bodyReq)
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("error marshal JSON: %v", err))
		return nil
	}
	resp, err := openapi.Request(openapi.RequestPOSTMethod, stringutil.StringBuilder(stringutil.WrapScheme(a.Server, false), openapi.DBMSAPIBasePath, openapi.APITaskPath), []byte(jsonStr))
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("the request failed: %v", err))
		return nil
	}

	var jsonData map[string]interface{}
	err = stringutil.UnmarshalJSON(resp, &jsonData)
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("error decoding JSON: %v", err))
		return nil
	}

	formattedJSON, err := stringutil.MarshalIndentJSON(stringutil.FormatJSONFields(jsonData))
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("error decoding JSON: %v", err))
		return nil
	}
	fmt.Printf("Status:       %s\n", cyan.Sprint("success"))
	fmt.Printf("Response:     %s\n", formattedJSON)
	return nil
}

type AppTaskCrontabDisplay struct {
	*AppTaskCrontab
	task string
}

func (a *AppTaskCrontab) AppTaskCrontabDisplay() component.Cmder {
	return &AppTaskCrontabDisplay{AppTaskCrontab: a}
}

func (a *AppTaskCrontabDisplay) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "display",
		Short:            "display the crontab cluster task",
		Long:             `display the crontab cluster task`,
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVarP(&a.task, "task", "t", "", "operate task name")
	return cmd
}

func (a *AppTaskCrontabDisplay) RunE(cmd *cobra.Command, args []string) error {
	var task string
	if strings.EqualFold(a.task, "") {
		task = "all"
	} else {
		task = a.task
	}
	cyan := color.New(color.FgCyan, color.Bold)
	fmt.Printf("Component:    %s\n", cyan.Sprint("dbms-ctl"))
	fmt.Printf("Command:      %s\n", cyan.Sprint("task crontab"))
	fmt.Printf("Action:       %s\n", cyan.Sprint("display"))
	fmt.Printf("Task:         %s\n", cyan.Sprint(task))

	cronTasks, err := service.DisplayCronTask(context.TODO(), a.Server, a.task)
	if err != nil {
		return err
	}
	jsonData := make(map[string]interface{})
	jsonData["tasks"] = cronTasks

	formattedJSON, err := stringutil.MarshalIndentJSON(stringutil.FormatJSONFields(jsonData))
	if err != nil {
		fmt.Printf("Status:       %s\n", cyan.Sprint("failed"))
		fmt.Printf("Response:     %s\n", color.RedString("error decoding JSON: %v", err))
		return nil
	}
	fmt.Printf("Status:       %s\n", cyan.Sprint("success"))
	fmt.Printf("Response:     %s\n", formattedJSON)
	return nil
}
