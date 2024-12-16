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
package tidb

import (
	"fmt"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
)

// TiDB TiCDC index-value dispatcher update event compatible
// https://docs.pingcap.com/zh/tidb/dev/ticdc-split-update-behavior
// v6.5 [>=v6.5.5] tidb database version greater than v6.5.5 and less than v7.0.0 All versions are supported normally
// v7 版本及以上 [>=v7.1.2] all versions of the tidb database version greater than v7.1.2 can be supported normally
func InspectTiDBConsumeTask(taskName, taskFlow, taskMode string, databaseS database.IDatabase) error {
	version, err := databaseS.GetDatabaseVersion()
	if err != nil {
		return err
	}

	logger.Warn("upstream database version",
		zap.String("task_name", taskName),
		zap.String("task_mode", taskMode),
		zap.String("task_flow", taskFlow),
		zap.String("version", version))

	if stringutil.VersionOrdinal(version) < stringutil.VersionOrdinal("v6.5.5") {
		return fmt.Errorf("the current database version [%s] does not support real-time synchronization based on the message queue + index-value distribution mode. There may be update events that cause data correctness issues. please choose other methods for data synchronization, details: https://docs.pingcap.com/zh/tidb/dev/ticdc-split-update-behavior", version)
	}

	if (stringutil.VersionOrdinal(version) >= stringutil.VersionOrdinal("v6.5.5") && stringutil.VersionOrdinal(version) < stringutil.VersionOrdinal("v7.0.0")) ||
		(stringutil.VersionOrdinal(version) >= stringutil.VersionOrdinal("v7.1.2")) {
		return nil
	}
	return fmt.Errorf("the current database version [%s] does not support real-time synchronization based on the message queue + index-value distribution mode. There may be update events that cause data correctness issues. please choose other methods for data synchronization, details: https://docs.pingcap.com/zh/tidb/dev/ticdc-split-update-behavior", version)
}
