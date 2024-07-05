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
package stringutil

// sudo list
var (
	topologyUserSudoPrivileges = []string{
		"ALL=(root) NOPASSWD: /usr/bin/echo *",
		"ALL=(root) NOPASSWD: /bin/bash -c mkdir -p *",
		"ALL=(root) NOPASSWD: /bin/bash -c chmod *",
		"ALL=(root) NOPASSWD: /bin/bash -c test *",
		"ALL=(root) NOPASSWD: /bin/bash -c mv *",
		"ALL=(root) NOPASSWD: /bin/systemctl daemon-reload",
		"ALL=(root) NOPASSWD: /bin/systemctl list *",
		"ALL=(root) NOPASSWD: /bin/systemctl list-dependencies *",
		"ALL=(root) NOPASSWD: /bin/systemctl list-unit-files *",
		"ALL=(root) NOPASSWD: /bin/systemctl link *",
		"ALL=(root) NOPASSWD: /bin/systemctl * [dbms-master|dbms-worker|dbms-cluster|dbms-ctl]*",
		"ALL=(root) NOPASSWD: /usr/bin/bash -c systemctl daemon-reload",
		"ALL=(root) NOPASSWD: /usr/bin/bash -c systemctl list-unit-files *",
		"ALL=(root) NOPASSWD: /usr/bin/bash -c systemctl status firewalld.service",
		"ALL=(root) NOPASSWD: /usr/bin/bash -c systemctl daemon-reload && systemctl * [dbms-cluster|dbms-worker|dbms-cluster|dbms-ctl]*",
		"ALL=(root) NOPASSWD: /usr/bin/bash -c systemctl * [dbms-cluster|dbms-worker|dbms-cluster|dbms-ctl]*",
		"ALL=(root) NOPASSWD: /usr/bin/bash -c systemctl enable *dbms*",
		"ALL=(root) NOPASSWD: /usr/bin/bash -c systemctl show *dbms*",
		"ALL=(root) NOPASSWD: /usr/bin/bash -c systemctl stop *dbms*",
		"ALL=(root) NOPASSWD: /usr/bin/bash -c systemctl start *dbms*",
		"ALL=(root) NOPASSWD: /usr/bin/bash -c systemctl status *dbms*",
		"ALL=(root) NOPASSWD: /usr/bin/bash -c systemctl disable *dbms*",
		"ALL=(root) NOPASSWD: /usr/bin/bash -c systemctl restart *dbms*",
		"ALL=(root) NOPASSWD: /bin/systemctl enable *dbms*",
		"ALL=(root) NOPASSWD: /bin/systemctl show *dbms*",
		"ALL=(root) NOPASSWD: /bin/systemctl stop *dbms*",
		"ALL=(root) NOPASSWD: /bin/systemctl start *dbms*",
		"ALL=(root) NOPASSWD: /bin/systemctl status *dbms*",
		"ALL=(root) NOPASSWD: /bin/systemctl disable *dbms*",
		"ALL=(root) NOPASSWD: /bin/systemctl restart *dbms*",
		"ALL=(root) NOPASSWD: /bin/systemctl link *",
		"ALL=(root) NOPASSWD: /usr/bin/bash -c rm -rf *",
		"ALL=(root) NOPASSWD: /usr/bin/bash -c tar *",
	}
)

func GetTopologyUserSudoPrivileges(deployUser string) []string {
	var newPrivis []string
	for _, d := range topologyUserSudoPrivileges {
		newPrivis = append(newPrivis, StringBuilder(deployUser, ` `, d))
	}
	return newPrivis
}
