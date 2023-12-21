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
package version

import (
	"fmt"
	"runtime"

	"github.com/wentaojin/dbms/logger"
	"go.uber.org/zap"
)

// 版本信息
var (
	Version   = "None"
	BuildTS   = "None"
	GitHash   = "None"
	GitBranch = "None"
)

func RecordAppVersion(app string, cfg string) {
	logger.Info("Welcome to "+app,
		zap.String("Release Version", Version),
		zap.String("Git Commit Hash", GitHash),
		zap.String("Git Branch", GitBranch),
		zap.String("UTC Build Time", BuildTS),
		zap.String("Go Version", runtime.Version()),
		zap.String("config", cfg),
	)
}

func GetRawVersionInfo() string {
	info := ""
	info += fmt.Sprintf("Release Version: %s\n", Version)
	info += fmt.Sprintf("Git Commit Hash: %s\n", GitHash)
	info += fmt.Sprintf("Git Branch: %s\n", GitBranch)
	info += fmt.Sprintf("UTC Build Time: %s\n", BuildTS)
	info += fmt.Sprintf("Go Version: %s\n", runtime.Version())
	return info
}
