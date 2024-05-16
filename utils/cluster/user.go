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
	"os/user"

	"go.uber.org/zap"

	"github.com/wentaojin/dbms/logger"
)

// CurrentUser returns current login user
func CurrentUser() string {
	u, err := user.Current()
	if err != nil {
		logger.Error("get current user", zap.Error(err))
		return "root"
	}
	return u.Username
}

// UserHome returns home directory of current user
func UserHome() string {
	u, err := user.Current()
	if err != nil {
		logger.Error("get current user home", zap.Error(err))
		return "root"
	}
	return u.HomeDir
}
