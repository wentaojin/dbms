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
package launchd

import (
	"bytes"
	"github.com/wentaojin/dbms/utils/cluster/embed"
	"github.com/wentaojin/dbms/utils/stringutil"
	"path"
	"text/template"
)

// https://developer.apple.com/library/archive/documentation/MacOSX/Conceptual/BPSystemStartup/Chapters/CreatingLaunchdJobs.html#//apple_ref/doc/uid/TP40001762-104142
// Config represent the data to generate systemd config
type Config struct {
	Label       string
	ServiceName string
	User        string
	DeployDir   string
}

// NewLaunchdConfig returns a Config with given arguments
func NewLaunchdConfig(label, service, user, deployDir string) *Config {
	return &Config{
		Label:       label,
		ServiceName: service,
		User:        user,
		DeployDir:   deployDir,
	}
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
	fp := path.Join("template", "launchd", "launchd.plist.tmpl")
	tpl, err := embed.ReadTemplate(fp)
	if err != nil {
		return nil, err
	}
	return c.ConfigWithTemplate(string(tpl))
}

// ConfigWithTemplate generate the system config content by tpl
func (c *Config) ConfigWithTemplate(tpl string) ([]byte, error) {
	tmpl, err := template.New("launchd").Parse(tpl)
	if err != nil {
		return nil, err
	}

	content := bytes.NewBufferString("")
	if err := tmpl.Execute(content, c); err != nil {
		return nil, err
	}

	return content.Bytes(), nil
}
