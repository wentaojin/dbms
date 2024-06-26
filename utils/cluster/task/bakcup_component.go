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
package task

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"

	"github.com/pingcap/errors"
	"github.com/wentaojin/dbms/utils/ctxt"
)

// BackupComponent is used to copy all files related the specific version a component
// to the target directory of path
type BackupComponent struct {
	component string
	fromVer   string
	action    string
	host      string
	deployDir string
}

// Execute implements the Task interface
func (c *BackupComponent) Execute(ctx context.Context) error {
	// Copy to remote server
	exec, found := ctxt.GetInner(ctx).GetExecutor(c.host)
	if !found {
		return ErrNoExecutor
	}

	binDir := filepath.Join(c.deployDir, "bin")

	// Make upgrade idempotent
	// The old version has been backup if upgrade abort
	cmd := fmt.Sprintf(`test -d %[2]s || cp -r %[1]s %[2]s`, binDir, binDir+".old."+c.fromVer+"."+c.action)
	_, stderr, err := exec.Execute(ctx, cmd, false)
	if err != nil {
		// ignore error if the source path does not exist, this is possible when
		// there are multiple instances share the same deploy_dir, typical case
		// is imported cluster
		// NOTE: by changing the behaviour to cp instead of mv in line 45, we don't
		// need to check "no such file" anymore, but I'm keeping it here in case
		// we got a better way handling the backups later
		if !(bytes.Contains(stderr, []byte("No such file or directory")) ||
			bytes.Contains(stderr, []byte("File exists"))) {
			return errors.Annotate(err, cmd)
		}
	}
	return nil
}

// Rollback implements the Task interface
func (c *BackupComponent) Rollback(ctx context.Context) error {
	return nil
}

// String implements the fmt.Stringer interface
func (c *BackupComponent) String() string {
	return fmt.Sprintf("BackupComponent: component=%s, currentVersion=%s, remote=%s:%s",
		c.component, c.fromVer, c.host, c.deployDir)
}
