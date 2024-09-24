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
package processor

import (
	"fmt"
	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
	"strings"
)

func InspectPostgresMigrateTask(taskName, taskFlow, taskMode string, databaseS database.IDatabase, connectDBCharsetS, connectDBCharsetT string) (string, error) {
	if strings.EqualFold(taskMode, constant.TaskModeStructMigrate) {
		dbCharsetMapping := constant.MigrateTableStructureDatabaseCharsetMap[taskFlow][stringutil.StringUpper(connectDBCharsetS)]
		if !strings.EqualFold(connectDBCharsetT, dbCharsetMapping) {
			return "", fmt.Errorf("the task [%s] taskflow [%s] charset [%s] mapping [%v] isn't equal with database connect charset [%s], please adjust database connect charset", taskName, taskFlow, connectDBCharsetS, dbCharsetMapping, connectDBCharsetT)
		}
	}

	dbCharsetS, err := databaseS.GetDatabaseCharset()
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(connectDBCharsetS, dbCharsetS) {
		zap.L().Warn("the database charset and oracle config charset",
			zap.String("postgres database charset", dbCharsetS),
			zap.String("postgres config charset", connectDBCharsetS))
		return "", fmt.Errorf("postgres database charset [%v] and postgres config charset [%v] aren't equal, please adjust postgres config charset", dbCharsetS, connectDBCharsetS)
	}
	if _, ok := constant.MigratePostgreSQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(connectDBCharsetS)]; !ok {
		return "", fmt.Errorf("postgres database charset [%v] isn't support, only support charset [%v]", dbCharsetS, stringutil.StringPairKey(constant.MigratePostgreSQLCompatibleCharsetStringConvertMapping))
	}
	if !stringutil.IsContainedString(constant.MigrateDataSupportCharset, stringutil.StringUpper(connectDBCharsetT)) {
		return "", fmt.Errorf("mysql compatible database current config charset [%v] isn't support, support charset [%v]", connectDBCharsetT, stringutil.StringJoin(constant.MigrateDataSupportCharset, ","))
	}

	if strings.EqualFold(taskMode, constant.TaskModeStructMigrate) {
		dbCollation, err := databaseS.GetDatabaseCollation()
		if err != nil {
			return "", err
		}
		if _, ok := constant.MigrateTableStructureDatabaseCollationMap[taskFlow][stringutil.StringUpper(dbCollation)]; !ok {
			return "", fmt.Errorf("postgres database collation [%v] isn't support", dbCollation)
		}

		return dbCollation, nil
	}
	return "", nil
}
