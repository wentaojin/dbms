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
package systemd

import (
	"bytes"
	"path"
	"text/template"

	"github.com/wentaojin/dbms/utils/cluster/embed"

	"github.com/wentaojin/dbms/utils/stringutil"
)

// Config represent the data to generate systemd config
type Config struct {
	ServiceName        string
	User               string
	DeployDir          string
	SystemMode         string
	GrantCapNetRaw     bool
	DisableSendSigkill bool
	// Takes one of no, on-success, on-failure, on-abnormal, on-watchdog, on-abort, or always.
	// The Template set as always if this is not setted.
	Restart string
}

// NewSystemdConfig returns a Config with given arguments
func NewSystemdConfig(service, user, deployDir string) *Config {
	return &Config{
		ServiceName: service,
		User:        user,
		DeployDir:   deployDir,
	}
}

func (c *Config) WithDisableSendSigkill(action bool) *Config {
	c.DisableSendSigkill = action
	return c
}

// WithRestart Takes one of no, on-success, on-failure, on-abnormal, on-watchdog, on-abort, or always.
//
//	// The Template set as always if this is not setted.
func (c *Config) WithRestart(action string) *Config {
	c.Restart = action
	return c
}

func (c *Config) WithSystemMode(mode string) *Config {
	c.SystemMode = mode
	return c
}

// ConfigToFile write config content to specific path
func (c *Config) ConfigToFile(file string) error {
	config, err := c.Config()
	if err != nil {
		return err
	}
	return stringutil.WriteFile(file, config, 0755)
}

// Config generate the config file data.
func (c *Config) Config() ([]byte, error) {
	fp := path.Join("template", "systemd", "systemd.service.tmpl")
	tpl, err := embed.ReadTemplate(fp)
	if err != nil {
		return nil, err
	}
	return c.ConfigWithTemplate(string(tpl))
}

// ConfigWithTemplate generate the system config content by tpl
func (c *Config) ConfigWithTemplate(tpl string) ([]byte, error) {
	tmpl, err := template.New("systemd").Parse(tpl)
	if err != nil {
		return nil, err
	}

	content := bytes.NewBufferString("")
	if err := tmpl.Execute(content, c); err != nil {
		return nil, err
	}

	return content.Bytes(), nil
}
