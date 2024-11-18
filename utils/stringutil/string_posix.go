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
	"fmt"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

// PromptForPassword reads a password input from console
func PromptForPassword(format string, a ...any) string {
	defer fmt.Println("")

	fmt.Printf(format, a...)

	input, err := term.ReadPassword(syscall.Stdin)

	if err != nil {
		return ""
	}
	return strings.TrimSpace(strings.Trim(string(input), "\n"))
}

// GetDiskUsage used for get the current dir disk usage size
func GetDiskUsage(path string) (uint64, uint64, error) {
	var statfs unix.Statfs_t
	if err := unix.Statfs(path, &statfs); err != nil {
		return 0, 0, err
	}
	totalSpace := statfs.Blocks * uint64(statfs.Bsize) / 1024 / 1024
	freeSpace := statfs.Bfree * uint64(statfs.Bsize) / 1024 / 1024
	return totalSpace, freeSpace, nil
}
