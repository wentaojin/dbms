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
package buildin

import (
	"github.com/wentaojin/dbms/utils/constant"
)

func InitO2MTBuildinDefaultValue() []*BuildinDefaultvalRule {
	var buildinColumDefaultvals []*BuildinDefaultvalRule

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinDefaultvalRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DefaultValueS: constant.BuildInOracleColumnDefaultValueSysdate,
		DefaultValueT: constant.BuildInOracleO2MColumnDefaultValueMap[constant.BuildInOracleColumnDefaultValueSysdate],
	})

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinDefaultvalRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DefaultValueS: constant.BuildInOracleColumnDefaultValueSYSGUID,
		DefaultValueT: constant.BuildInOracleO2MColumnDefaultValueMap[constant.BuildInOracleColumnDefaultValueSYSGUID],
	})

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinDefaultvalRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DefaultValueS: constant.BuildInOracleColumnDefaultValueNULL,
		DefaultValueT: constant.BuildInOracleO2MColumnDefaultValueMap[constant.BuildInOracleColumnDefaultValueNULL],
	})

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinDefaultvalRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DefaultValueS: constant.BuildInOracleColumnDefaultValueSysdate,
		DefaultValueT: constant.BuildInOracleO2MColumnDefaultValueMap[constant.BuildInOracleColumnDefaultValueSysdate],
	})

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinDefaultvalRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DefaultValueS: constant.BuildInOracleColumnDefaultValueSYSGUID,
		DefaultValueT: constant.BuildInOracleO2MColumnDefaultValueMap[constant.BuildInOracleColumnDefaultValueSYSGUID],
	})

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinDefaultvalRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DefaultValueS: constant.BuildInOracleColumnDefaultValueNULL,
		DefaultValueT: constant.BuildInOracleO2MColumnDefaultValueMap[constant.BuildInOracleColumnDefaultValueNULL],
	})

	return buildinColumDefaultvals
}

func InitMT2OBuildinDefaultValue() []*BuildinDefaultvalRule {
	var buildinColumDefaultvals []*BuildinDefaultvalRule

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinDefaultvalRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DefaultValueS: constant.BuildInMySQLColumnDefaultValueCurrentTimestamp,
		DefaultValueT: constant.BuildInMySQLM2OColumnDefaultValueMap[constant.BuildInMySQLColumnDefaultValueCurrentTimestamp],
	})
	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinDefaultvalRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DefaultValueS: constant.BuildInMySQLColumnDefaultValueCurrentTimestamp,
		DefaultValueT: constant.BuildInMySQLM2OColumnDefaultValueMap[constant.BuildInMySQLColumnDefaultValueCurrentTimestamp],
	})

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinDefaultvalRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DefaultValueS: constant.BuildInMySQLColumnDefaultValueNULL,
		DefaultValueT: constant.BuildInOracleO2MColumnDefaultValueMap[constant.BuildInMySQLColumnDefaultValueNULL],
	})

	buildinColumDefaultvals = append(buildinColumDefaultvals, &BuildinDefaultvalRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DefaultValueS: constant.BuildInMySQLColumnDefaultValueNULL,
		DefaultValueT: constant.BuildInOracleO2MColumnDefaultValueMap[constant.BuildInMySQLColumnDefaultValueNULL],
	})

	return buildinColumDefaultvals
}
