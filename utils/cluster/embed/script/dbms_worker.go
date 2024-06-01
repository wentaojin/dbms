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
package script

import (
	"bytes"
	"html/template"
	"path"

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/utils/cluster/embed"
)

// DBMSWorkerScript represent the data to generate TiDB config
type DBMSWorkerScript struct {
	WorkerAddr string
	Join       string

	DeployDir string
	LogDir    string

	InstanceNumaNode string

	Endpoints []*DBMSMasterScript
}

// ConfigToFile write config content to specific path
func (c *DBMSWorkerScript) ConfigToFile(file string) error {
	fp := path.Join("template", "script", "run_dbms-worker.sh.tmpl")
	tpl, err := embed.ReadTemplate(fp)
	if err != nil {
		return err
	}
	tmpl, err := template.New("dbms-worker").Parse(string(tpl))
	if err != nil {
		return err
	}

	content := bytes.NewBufferString("")
	if err := tmpl.Execute(content, c); err != nil {
		return err
	}

	return stringutil.WriteFile(file, content.Bytes(), 0755)
}
