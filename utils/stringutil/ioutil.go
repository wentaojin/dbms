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

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"

	"github.com/otiai10/copy"
	"github.com/pingcap/errors"

	"github.com/BurntSushi/toml"
)

var (
	fileLocks = make(map[string]*sync.Mutex)
	filesLock = sync.Mutex{}
)

// Abs returns the absolute path
func Abs(user, path string) string {
	// trim whitespaces before joining
	user = strings.TrimSpace(user)
	path = strings.TrimSpace(path)
	if !strings.HasPrefix(path, "/") {
		path = filepath.Join("/home", user, path)
	}
	return filepath.Clean(path)
}

// CreateDir used to create dir
func CreateDir(path string) error {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(path, 0755)
		}
		return err
	}
	return nil
}

// RemoveAllDir used to remove dir
func RemoveAllDir(dir string) error {
	err := os.RemoveAll(dir)
	if err != nil {
		return err
	}
	return nil
}

// WriteFile call os.WriteFile, but use max(parent permission,minPerm)
func WriteFile(name string, data []byte, perm os.FileMode) error {
	fi, err := os.Stat(filepath.Dir(name))
	if err == nil {
		perm |= (fi.Mode().Perm() & 0666)
	}
	return os.WriteFile(name, data, perm)
}

// MultiDirAbs returns the absolute path for multi-dir separated by comma
func MultiDirAbs(user, paths string) []string {
	var dirs []string
	for _, path := range strings.Split(paths, ",") {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		dirs = append(dirs, Abs(user, path))
	}
	return dirs
}

// PackagePath return the tar bar path
func PackagePath(mirrorDir, comp string, version string, os string, arch string) string {
	fileName := fmt.Sprintf("%s-%s-%s-%s.tar.gz", comp, version, os, arch)
	return filepath.Join(mirrorDir, fileName)
}

// Copy copies a file or directory from src to dst
func Copy(src, dst string) error {
	// check if src is a directory
	fi, err := os.Stat(src)
	if err != nil {
		return err
	}
	if fi.IsDir() {
		// use copy.Copy to copy a directory
		return copy.Copy(src, dst)
	}

	// for regular files
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}

	err = out.Close()
	if err != nil {
		return err
	}

	err = os.Chmod(dst, fi.Mode())
	if err != nil {
		return err
	}

	// Make sure the created dst's modify time is newer (at least equal) than src
	// this is used to workaround github action virtual filesystem
	ofi, err := os.Stat(dst)
	if err != nil {
		return err
	}
	if fi.ModTime().After(ofi.ModTime()) {
		return os.Chtimes(dst, fi.ModTime(), fi.ModTime())
	}
	return nil
}

// Ternary operator
func Ternary(condition bool, a, b any) any {
	if condition {
		return a
	}
	return b
}

// IsPathNotExist check whether a path is not exist
func IsPathNotExist(path string) bool {
	_, err := os.Stat(path)
	return os.IsNotExist(err)
}

// IsSubDir returns if sub is a sub directory of parent
func IsSubDir(parent, sub string) bool {
	up := ".." + string(os.PathSeparator)

	rel, err := filepath.Rel(parent, sub)
	if err != nil {
		return false
	}
	if !strings.HasPrefix(rel, up) && rel != ".." {
		return true
	}
	return false
}

// Checksum returns the sha1 sum of target file
func Checksum(file string) (string, error) {
	tarball, err := os.OpenFile(file, os.O_RDONLY, 0)
	if err != nil {
		return "", err
	}
	defer tarball.Close()

	sha1Writter := sha1.New()
	if _, err := io.Copy(sha1Writter, tarball); err != nil {
		return "", err
	}

	checksum := hex.EncodeToString(sha1Writter.Sum(nil))
	return checksum, nil
}

// JoinHostPort return host and port
func JoinHostPort(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}

// Merge2TomlConfig merge the config of global.
func Merge2TomlConfig(comp string, global, overwrite map[string]any) ([]byte, error) {
	lhs := MergeConfig(global, overwrite)
	buf := bytes.NewBufferString(fmt.Sprintf(`# WARNING: This file is auto-generated. Do not edit! All your modification will be overwritten!
# You can use 'cluster edit-config' and 'cluster reload' to update the configuration
# All configuration items you want to change can be added to:
# server_configs:
#   %s:
#     aa.b1.c3: value
#     aa.b2.c4: value
`, comp))

	enc := toml.NewEncoder(buf)
	enc.Indent = ""
	err := enc.Encode(lhs)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func fileLock(path string) *sync.Mutex {
	filesLock.Lock()
	defer filesLock.Unlock()

	if _, ok := fileLocks[path]; !ok {
		fileLocks[path] = &sync.Mutex{}
	}

	return fileLocks[path]
}

// SaveFileWithBackup will backup the file before save it.
// e.g., backup meta.yaml as meta-2006-01-02T15:04:05Z07:00.yaml
// backup the files in the same dir of path if backupDir is empty.
func SaveFileWithBackup(path string, data []byte, backupDir string) error {
	fileLock(path).Lock()
	defer fileLock(path).Unlock()

	info, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		return errors.AddStack(err)
	}

	if info != nil && info.IsDir() {
		return errors.Errorf("%s is directory", path)
	}

	// backup file
	if !os.IsNotExist(err) {
		base := filepath.Base(path)
		dir := filepath.Dir(path)

		var backupName string
		timestr := time.Now().Format(time.RFC3339Nano)
		p := strings.Split(base, ".")
		if len(p) == 1 {
			backupName = base + "-" + timestr
		} else {
			backupName = strings.Join(p[0:len(p)-1], ".") + "-" + timestr + "." + p[len(p)-1]
		}

		backupData, err := os.ReadFile(path)
		if err != nil {
			return errors.AddStack(err)
		}

		var backupPath string
		if backupDir != "" {
			backupPath = filepath.Join(backupDir, backupName)
		} else {
			backupPath = filepath.Join(dir, backupName)
		}
		err = os.WriteFile(backupPath, backupData, 0644)
		if err != nil {
			return errors.AddStack(err)
		}
	}

	err = os.WriteFile(path, data, 0644)
	if err != nil {
		return errors.AddStack(err)
	}

	return nil
}

// JoinInt joins a slice of int to string
func JoinInt(nums []int, delim string) string {
	result := ""
	for _, i := range nums {
		result += strconv.Itoa(i)
		result += delim
	}
	return strings.TrimSuffix(result, delim)
}

// OsArch builds an "os/arch" string from input, it converts some similar strings
// to different words to avoid misreading when displaying in terminal
func OsArch(os, arch string) string {
	osFmt := os
	archFmt := arch

	switch arch {
	case "amd64":
		archFmt = "x86_64"
	case "arm64":
		archFmt = "aarch64"
	}

	return fmt.Sprintf("%s/%s", osFmt, archFmt)
}

// PrintTable accepts a matrix of strings and print them as ASCII table to terminal
func PrintTable(rows [][]string, header bool) {
	// Print the table
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	if header {
		addRow(t, rows[0], true)
		border := make([]string, len(rows[0]))
		for i := range border {
			border[i] = strings.Repeat("-", len(rows[0][i]))
		}
		addRow(t, border, false)
		rows = rows[1:]
	}
	for _, row := range rows {
		addRow(t, row, false)
	}

	t.SetStyle(table.Style{
		Name: "dbms-cluster",
		Box: table.BoxStyle{
			BottomLeft:       "",
			BottomRight:      "",
			BottomSeparator:  "",
			Left:             "|",
			LeftSeparator:    "|",
			MiddleHorizontal: "-",
			MiddleSeparator:  "  ",
			MiddleVertical:   "  ",
			PaddingLeft:      "",
			PaddingRight:     "",
			Right:            "",
			RightSeparator:   "",
			TopLeft:          "",
			TopRight:         "",
			TopSeparator:     "",
			UnfinishedRow:    "",
		},
		Format: table.FormatOptions{
			Header: text.FormatDefault,
		},
		Options: table.Options{
			SeparateColumns: true,
		},
	})
	t.Render()
}

func addRow(t table.Writer, rawLine []string, header bool) {
	// Convert []string to []any
	row := make(table.Row, len(rawLine))
	for i, v := range rawLine {
		row[i] = v
	}

	// Add line to the table
	if header {
		t.AppendHeader(row)
	} else {
		t.AppendRow(row)
	}
}

// pre-defined ascii art strings
const (
	ASCIIArtWarning = `
  ██     ██  █████  ██████  ███    ██ ██ ███    ██  ██████
  ██     ██ ██   ██ ██   ██ ████   ██ ██ ████   ██ ██
  ██  █  ██ ███████ ██████  ██ ██  ██ ██ ██ ██  ██ ██   ███
  ██ ███ ██ ██   ██ ██   ██ ██  ██ ██ ██ ██  ██ ██ ██    ██
   ███ ███  ██   ██ ██   ██ ██   ████ ██ ██   ████  ██████
`
)

// MergeConfig merge two or more config into one and unflat them
// config1:
//
//	a.b.a: 1
//	a.b.b: 2
//
// config2:
//
//	a.b.a: 3
//	a.b.c: 4
//
// config3:
//
//	b.c = 5
//
// After MergeConfig(config1, config2, config3):
//
//	a:
//	  b:
//	    a: 3
//	    b: 2
//	    c: 4
//	b:
//	  c: 5
func MergeConfig(orig map[string]any, overwrites ...map[string]any) map[string]any {
	lhs := FoldMap(orig)
	for _, overwrite := range overwrites {
		rhs := FoldMap(overwrite)
		for k, v := range rhs {
			patch(lhs, k, v)
		}
	}
	return lhs
}

// FoldMap convert single layer map to multi-layer
func FoldMap(ms map[string]any) map[string]any {
	// we flatten map first to deal with the case like:
	// a.b:
	//   c.d: xxx
	ms = FlattenMap(ms)
	result := map[string]any{}
	for k, v := range ms {
		key, val := foldKey(k, v)
		patch(result, key, val)
	}
	return result
}

// FlattenMap convert mutil-layer map to single layer
func FlattenMap(ms map[string]any) map[string]any {
	result := map[string]any{}
	for k, v := range ms {
		var sub map[string]any

		if m, ok := v.(map[string]any); ok {
			sub = FlattenMap(m)
		} else if m, ok := v.(map[any]any); ok {
			fixM := map[string]any{}
			for k, v := range m {
				if sk, ok := k.(string); ok {
					fixM[sk] = v
				}
			}
			sub = FlattenMap(fixM)
		} else {
			result[k] = v
			continue
		}

		for sk, sv := range sub {
			result[k+"."+sk] = sv
		}
	}
	return result
}

func foldKey(key string, val any) (string, any) {
	parts := strings.SplitN(key, ".", 2)
	if len(parts) == 1 {
		return key, strKeyMap(val)
	}
	subKey, subVal := foldKey(parts[1], val)
	return parts[0], map[string]any{
		subKey: strKeyMap(subVal),
	}
}

func patch(origin map[string]any, key string, val any) {
	origVal, found := origin[key]
	if !found {
		origin[key] = strKeyMap(val)
		return
	}
	origMap, lhsOk := origVal.(map[string]any)
	valMap, rhsOk := val.(map[string]any)
	if lhsOk && rhsOk {
		for k, v := range valMap {
			patch(origMap, k, v)
		}
	} else {
		// overwrite
		origin[key] = strKeyMap(val)
	}
}

// strKeyMap tries to convert `map[any]any` to `map[string]any`
func strKeyMap(val any) any {
	m, ok := val.(map[any]any)
	if ok {
		ret := map[string]any{}
		for k, v := range m {
			kk, ok := k.(string)
			if !ok {
				return val
			}
			ret[kk] = strKeyMap(v)
		}
		return ret
	}

	rv := reflect.ValueOf(val)
	if rv.Kind() == reflect.Slice {
		var ret []any
		for i := 0; i < rv.Len(); i++ {
			ret = append(ret, strKeyMap(rv.Index(i).Interface()))
		}
		return ret
	}

	return val
}

// IsPathExist check whether a path is exist
func IsPathExist(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

// MkdirAll basically copied from os.MkdirAll, but use max(parent permission,minPerm)
func MkdirAll(path string, minPerm os.FileMode) error {
	// Fast path: if we can tell whether path is a directory or file, stop with success or error.
	dir, err := os.Stat(path)
	if err == nil {
		if dir.IsDir() {
			return nil
		}
		return &os.PathError{Op: "mkdir", Path: path, Err: syscall.ENOTDIR}
	}

	// Slow path: make sure parent exists and then call Mkdir for path.
	i := len(path)
	for i > 0 && os.IsPathSeparator(path[i-1]) { // Skip trailing path separator.
		i--
	}

	j := i
	for j > 0 && !os.IsPathSeparator(path[j-1]) { // Scan backward over element.
		j--
	}

	if j > 1 {
		// Create parent.
		err = MkdirAll(path[:j-1], minPerm)
		if err != nil {
			return err
		}
	}

	perm := minPerm
	fi, err := os.Stat(filepath.Dir(path))
	if err == nil {
		perm |= fi.Mode().Perm()
	}

	// Parent now exists; invoke Mkdir and use its result; inheritance parent perm.
	err = os.Mkdir(path, perm)
	if err != nil {
		// Handle arguments like "foo/." by
		// double-checking that directory doesn't exist.
		dir, err1 := os.Lstat(path)
		if err1 == nil && dir.IsDir() {
			return nil
		}
		return err
	}
	return nil
}
