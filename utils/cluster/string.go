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
package cluster

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/mod/semver"
)

// FmtVer converts a version string to SemVer format, if the string is not a valid
// SemVer and fails to parse and convert it, an error is raised.
func FmtVer(ver string) (string, error) {
	v := ver

	if !strings.HasPrefix(ver, "v") {
		v = fmt.Sprintf("v%s", ver)
	}
	if !semver.IsValid(v) {
		return v, fmt.Errorf("version %s is not a valid SemVer string", ver)
	}
	return v, nil
}

var (
	clusterNameRegexp = regexp.MustCompile(`^[a-zA-Z0-9\-_\.]+$`)
)

// ValidateClusterNameOrError validates a cluster name and returns error if the name is invalid.
func ValidateClusterNameOrError(n string) error {
	if len(n) == 0 {
		return fmt.Errorf("cluster name must not be empty")
	}
	if !clusterNameRegexp.MatchString(n) {
		return fmt.Errorf("cluster name '%s' is invalid, The cluster name should only contain alphabets, numbers, hyphen (-), underscore (_), and dot (.)", n)
	}
	return nil
}

// CheckCommandArgsAndMayPrintHelp checks whether user passes enough number of arguments.
// If insufficient number of arguments are passed, an error with proper suggestion will be raised.
// When no argument is passed, command help will be printed and no error will be raised.
func CheckCommandArgsAndMayPrintHelp(cmd *cobra.Command, args []string, minArgs int) (shouldContinue bool, err error) {
	if minArgs == 0 {
		return true, nil
	}
	lenArgs := len(args)
	if lenArgs == 0 {
		return false, cmd.Help()
	}
	if lenArgs < minArgs {
		return false, fmt.Errorf("expect at least %d arguments, but received %d arguments, usage:\n%v", minArgs, lenArgs, cmd.UsageString())
	}
	return true, nil
}
