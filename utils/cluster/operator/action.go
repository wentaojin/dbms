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
package operator

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/wentaojin/dbms/utils/executor"
	"github.com/wentaojin/dbms/utils/module"

	"github.com/wentaojin/dbms/logger/printer"

	"github.com/wentaojin/dbms/utils/ctxt"
	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/pingcap/errors"
	"github.com/wentaojin/dbms/utils/cluster"
	"golang.org/x/sync/errgroup"
)

var (
	actionPrevMsgs = map[string]string{
		"start":   "Starting",
		"stop":    "Stopping",
		"enable":  "Enabling",
		"disable": "Disabling",
	}
	actionPostMsgs = map[string]string{}
)

func init() {
	for action := range actionPrevMsgs {
		actionPostMsgs[action] = cases.Title(language.English).String(action)
	}
}

// Start the cluster.
func Start(
	ctx context.Context,
	cluster *cluster.Topology,
	options *Options,
	tlsCfg *tls.Config,
) error {
	roleFilter := stringutil.NewStringSet(options.Roles...)
	nodeFilter := stringutil.NewStringSet(options.Nodes...)
	components := cluster.ComponentsByStartOrder()
	components = FilterComponent(components, roleFilter)
	systemdMode := string(cluster.GlobalOptions.SystemdMode)

	for _, comp := range components {
		insts := FilterInstance(comp.Instances(), nodeFilter)
		err := StartComponent(ctx, insts, options, tlsCfg, systemdMode)
		if err != nil {
			return errors.Annotatef(err, "failed to start %s", comp.ComponentName())
		}
	}
	return nil
}

// StartComponent start the instances.
func StartComponent(ctx context.Context, instances []cluster.Instance, options *Options, tlsCfg *tls.Config, systemdMode string) error {
	if len(instances) == 0 {
		return nil
	}

	logger := ctx.Value(printer.ContextKeyLogger).(*printer.Logger)
	name := instances[0].ComponentName()
	logger.Infof("Starting component %s", name)

	// start instances in serial for Raft related components
	// eg: PD has more strict restrictions on the capacity expansion process,
	// that is, there should be only one node in the peer-join stage at most
	// ref https://github.com/tikv/pd/blob/d38b36714ccee70480c39e07126e3456b5fb292d/server/join/join.go#L179-L191
	if options.Operation == ScaleOutOperation {
		switch name {
		case cluster.ComponentDBMSMaster:
			return serialStartInstances(ctx, instances, options, tlsCfg, systemdMode)
		}
	}

	errg, nctx := errgroup.WithContext(ctx)

	for _, ins := range instances {
		inst := ins
		errg.Go(func() error {
			return startInstance(nctx, inst, options.OptTimeout, tlsCfg, systemdMode)
		})
	}

	return errg.Wait()
}

// Stop the cluster.
func Stop(
	ctx context.Context,
	cluster *cluster.Topology,
	options *Options,
) error {
	roleFilter := stringutil.NewStringSet(options.Roles...)
	nodeFilter := stringutil.NewStringSet(options.Nodes...)
	components := cluster.ComponentsByStopOrder()
	components = FilterComponent(components, roleFilter)

	for _, comp := range components {
		insts := FilterInstance(comp.Instances(), nodeFilter)
		err := StopComponent(
			ctx,
			cluster,
			insts,
			options,
		)
		if err != nil && !options.Force {
			return errors.Annotatef(err, "failed to stop %s", comp.ComponentName())
		}
	}
	return nil
}

// Enable will enable/disable the cluster
func Enable(
	ctx context.Context,
	topo *cluster.Topology,
	options *Options,
	isEnable bool,
) error {
	roleFilter := stringutil.NewStringSet(options.Roles...)
	nodeFilter := stringutil.NewStringSet(options.Nodes...)
	components := topo.ComponentsByStartOrder()
	components = FilterComponent(components, roleFilter)
	systemdMode := string(topo.GlobalOptions.SystemdMode)

	for _, comp := range components {
		insts := FilterInstance(comp.Instances(), nodeFilter)
		err := EnableComponent(ctx, insts, options, isEnable, systemdMode)
		if err != nil {
			return errors.Annotatef(err, "failed to enable/disable %s", comp.ComponentName())
		}
	}
	return nil
}

// Restart the cluster.
func Restart(
	ctx context.Context,
	cluster *cluster.Topology,
	options *Options,
	tlsCfg *tls.Config,
) error {
	err := Stop(ctx, cluster, options)
	if err != nil {
		return errors.Annotatef(err, "failed to stop")
	}

	err = Start(ctx, cluster, options, tlsCfg)
	if err != nil {
		return errors.Annotatef(err, "failed to start")
	}

	return nil
}

// Destroy the cluster.
func Destroy(
	ctx context.Context,
	topo *cluster.Topology,
	options *Options,
) error {
	coms := topo.ComponentsByStopOrder()

	instCount := map[string]int{}
	topo.IterInstance(func(inst cluster.Instance) {
		instCount[inst.InstanceManageHost()]++
	})
	for _, com := range coms {
		insts := com.Instances()
		err := DestroyComponent(ctx, insts, topo, options)
		if err != nil && !options.Force {
			return fmt.Errorf("failed to destroy instance %s, error detail: %v", com.ComponentName(), err)
		}
	}

	gOpts := topo.GlobalOptions

	// Delete all global deploy directory
	for host := range instCount {
		if err := DeleteGlobalDirs(ctx, host, gOpts); err != nil {
			return nil
		}
	}

	// after all things done, try to remove SSH public key
	for host := range instCount {
		if err := DeletePublicKey(ctx, host); err != nil {
			return nil
		}
	}

	return nil
}

// EnableComponent enable/disable the instances
func EnableComponent(ctx context.Context, instances []cluster.Instance, options *Options, isEnable bool, systemdMode string) error {
	if len(instances) == 0 {
		return nil
	}

	logger := ctx.Value(printer.ContextKeyLogger).(*printer.Logger)
	name := instances[0].ComponentName()
	if isEnable {
		logger.Infof("Enabling component %s", name)
	} else {
		logger.Infof("Disabling component %s", name)
	}

	errg, _ := errgroup.WithContext(ctx)

	for _, ins := range instances {
		inst := ins
		errg.Go(func() error {
			err := enableInstance(ctx, inst, options.OptTimeout, isEnable, systemdMode)
			if err != nil {
				return err
			}
			return nil
		})
	}

	return errg.Wait()
}

// StopComponent stop the instances.
func StopComponent(ctx context.Context,
	topo *cluster.Topology,
	instances []cluster.Instance,
	options *Options,
) error {
	if len(instances) == 0 {
		return nil
	}
	logger := ctx.Value(printer.ContextKeyLogger).(*printer.Logger)
	name := instances[0].ComponentName()
	logger.Infof("Stopping component %s", name)
	systemdMode := string(topo.GlobalOptions.SystemdMode)
	errg, nctx := errgroup.WithContext(ctx)

	for _, ins := range instances {
		inst := ins
		errg.Go(func() error {
			err := stopInstance(nctx, inst, options.OptTimeout, systemdMode)
			if err != nil {
				return err
			}
			return nil
		})
	}

	return errg.Wait()
}

func stopInstance(ctx context.Context, ins cluster.Instance, timeout uint64, systemdMode string) error {
	e := ctxt.GetInner(ctx).Get(ins.InstanceManageHost())
	logger := ctx.Value(printer.ContextKeyLogger).(*printer.Logger)
	logger.Infof("\tStopping instance %s", ins.InstanceManageHost())

	if strings.EqualFold(ins.OS(), "darwin") {
		if err := launchctl(ctx, e, ins.ServiceName(), "stop", timeout, systemdMode); err != nil {
			return toFailedActionError(err, "stop", ins.InstanceManageHost(), ins.ServiceName(), ins.InstanceLogDir())
		}
	} else {
		if err := systemctl(ctx, e, ins.ServiceName(), "stop", timeout, systemdMode); err != nil {
			return toFailedActionError(err, "stop", ins.InstanceManageHost(), ins.ServiceName(), ins.InstanceLogDir())
		}
	}

	logger.Infof("\tStop %s %s success", ins.ComponentName(), ins.InstanceName())

	return nil
}

func serialStartInstances(ctx context.Context, instances []cluster.Instance, options *Options, tlsCfg *tls.Config, systemdMode string) error {
	for _, ins := range instances {
		if err := startInstance(ctx, ins, options.OptTimeout, tlsCfg, systemdMode); err != nil {
			return err
		}
	}
	return nil
}

func startInstance(ctx context.Context, ins cluster.Instance, timeout uint64, tlsCfg *tls.Config, systemdMode string) error {
	e := ctxt.GetInner(ctx).Get(ins.InstanceManageHost())
	logger := ctx.Value(printer.ContextKeyLogger).(*printer.Logger)
	logger.Infof("\tStarting instance %s", ins.InstanceName())

	if strings.EqualFold(ins.OS(), "darwin") {
		if err := launchctl(ctx, e, ins.ServiceName(), "start", timeout, systemdMode); err != nil {
			return toFailedActionError(err, "start", ins.InstanceManageHost(), ins.ServiceName(), ins.InstanceLogDir())
		}
	} else {
		if err := systemctl(ctx, e, ins.ServiceName(), "start", timeout, systemdMode); err != nil {
			return toFailedActionError(err, "start", ins.InstanceManageHost(), ins.ServiceName(), ins.InstanceLogDir())
		}
	}

	// Check ready.
	if err := ins.ServiceReady(ctx, e, timeout); err != nil {
		return toFailedActionError(err, "start", ins.InstanceManageHost(), ins.ServiceName(), ins.InstanceLogDir())
	}

	logger.Infof("\tStart instance %s success", ins.InstanceName())

	return nil
}

func restartInstance(ctx context.Context, ins cluster.Instance, timeout uint64, tlsCfg *tls.Config, systemdMode string) error {
	e := ctxt.GetInner(ctx).Get(ins.InstanceManageHost())
	logger := ctx.Value(printer.ContextKeyLogger).(*printer.Logger)
	logger.Infof("\tRestarting instance %s", ins.InstanceName())

	if err := systemctl(ctx, e, ins.ServiceName(), "restart", timeout, systemdMode); err != nil {
		return toFailedActionError(err, "restart", ins.InstanceManageHost(), ins.ServiceName(), ins.InstanceLogDir())
	}

	// Check ready.
	if err := ins.ServiceReady(ctx, e, timeout); err != nil {
		return toFailedActionError(err, "restart", ins.InstanceManageHost(), ins.ServiceName(), ins.InstanceLogDir())
	}

	logger.Infof("\tRestart instance %s success", ins.InstanceName())

	return nil
}

func enableInstance(ctx context.Context, ins cluster.Instance, timeout uint64, isEnable bool, systemdMode string) error {
	e := ctxt.GetInner(ctx).Get(ins.InstanceManageHost())
	logger := ctx.Value(printer.ContextKeyLogger).(*printer.Logger)

	action := "disable"
	if isEnable {
		action = "enable"
	}
	logger.Infof("\t%s instance %s", actionPrevMsgs[action], ins.InstanceName())

	// Enable/Disable by systemd.
	if strings.EqualFold(ins.OS(), "darwin") {
		if err := launchctl(ctx, e, ins.ServiceName(), action, timeout, systemdMode); err != nil {
			return toFailedActionError(err, action, ins.InstanceManageHost(), ins.ServiceName(), ins.InstanceLogDir())
		}
	} else {
		if err := systemctl(ctx, e, ins.ServiceName(), action, timeout, systemdMode); err != nil {
			return toFailedActionError(err, action, ins.InstanceManageHost(), ins.ServiceName(), ins.InstanceLogDir())
		}
	}

	logger.Infof("\t%s instance %s success", actionPostMsgs[action], ins.InstanceName())

	return nil
}

func launchctl(ctx context.Context, executor executor.Executor, service string, action string, timeout uint64, scope string) error {
	logger := ctx.Value(printer.ContextKeyLogger).(*printer.Logger)
	c := module.LaunchdModuleConfig{
		Unit:        service,
		Action:      action,
		Timeout:     time.Second * time.Duration(timeout),
		SystemdMode: scope,
	}
	systemd := module.NewLaunchdModule(c)
	stdout, stderr, err := systemd.Execute(ctx, executor)

	if len(stdout) > 0 {
		fmt.Println(string(stdout))
	}
	if len(stderr) > 0 {
		logger.Errorf(string(stderr))
	}
	if len(stderr) > 0 && action == "stop" {
		logger.Errorf(string(stderr))
	}
	return err
}

func systemctl(ctx context.Context, executor executor.Executor, service string, action string, timeout uint64, scope string) error {
	logger := ctx.Value(printer.ContextKeyLogger).(*printer.Logger)
	c := module.SystemdModuleConfig{
		Unit:         service,
		ReloadDaemon: true,
		Action:       action,
		Timeout:      time.Second * time.Duration(timeout),
		Scope:        scope,
	}
	systemd := module.NewSystemdModule(c)
	stdout, stderr, err := systemd.Execute(ctx, executor)

	if len(stdout) > 0 {
		fmt.Println(string(stdout))
	}
	if len(stderr) > 0 && !bytes.Contains(stderr, []byte("Created symlink ")) && !bytes.Contains(stderr, []byte("Removed symlink ")) {
		logger.Errorf(string(stderr))
	}
	if len(stderr) > 0 && action == "stop" {
		// ignore "unit not loaded" error, as this means the unit is not
		// exist, and that's exactly what we want
		// NOTE: there will be a potential bug if the unit name is set
		// wrong and the real unit still remains started.
		if bytes.Contains(stderr, []byte(" not loaded.")) {
			logger.Warnf(string(stderr))
			return nil // reset the error to avoid exiting
		}
		logger.Errorf(string(stderr))
	}
	return err
}

// toFailedActionError formats the errror msg for failed action
func toFailedActionError(err error, action string, host, service, logDir string) error {
	return errors.Annotatef(err,
		"failed to %s: %s %s, please check the instance's log(%s) for more detail.",
		action, host, service, logDir,
	)
}

func executeSSHCommand(ctx context.Context, action, host, command string) error {
	if command == "" {
		return nil
	}
	e, found := ctxt.GetInner(ctx).GetExecutor(host)
	if !found {
		return fmt.Errorf("no executor")
	}
	logger := ctx.Value(printer.ContextKeyLogger).(*printer.Logger)
	logger.Infof("\t%s on %s", action, host)
	stdout, stderr, err := e.Execute(ctx, command, false)
	if err != nil {
		return errors.Annotatef(err, "stderr: %s", string(stderr))
	}
	logger.Infof("\t%s", stdout)
	return nil
}
