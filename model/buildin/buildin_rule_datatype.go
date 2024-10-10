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

func InitO2MBuildinDatatypeRule() []*BuildinDatatypeRule {
	var buildinDataTypeR []*BuildinDatatypeRule
	/*
		O2M Build-IN Compatible Rule
	*/
	// oracle column datatype name
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeNumber,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeNumber],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeBfile,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeBfile],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeChar,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeChar],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeCharacter,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeCharacter],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeClob,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeClob],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeBlob,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeBlob],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeDate,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeDate],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeDecimal,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeDecimal],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeDec,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeDec],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeDoublePrecision,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeDoublePrecision],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeFloat,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeFloat],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeInteger,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeInteger],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeInt,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeInt],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeLong,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeLong],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeLongRAW,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeLongRAW],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeBinaryFloat,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeBinaryFloat],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeBinaryDouble,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeBinaryDouble],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeNchar,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeNchar],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeNcharVarying,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeNcharVarying],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeNclob,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeNclob],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeNumeric,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeNumeric],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeNvarchar2,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeNvarchar2],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeRaw,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeRaw],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeReal,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeReal],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeRowid,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeRowid],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeUrowid,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeUrowid],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeSmallint,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeSmallint],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeVarchar2,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeVarchar2],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeVarchar,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeVarchar],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeXmltype,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeXmltype],
	})

	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp0,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp0],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp1,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp1],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp2,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp2],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp3,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp3],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp4,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp4],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp5,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp5],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp6,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp6],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp7,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp7],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp8,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp8],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp9,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp9],
	})

	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth0,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth0],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth1,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth1],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth2,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth2],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth3,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth3],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth4,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth4],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth5,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth5],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth6,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth6],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth7,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth7],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth8,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth8],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth9,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth9],
	})

	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone0,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone0],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone1,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone1],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone2,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone2],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone3,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone3],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone4,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone4],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone5,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone5],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone6,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone6],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone7,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone7],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone8,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone8],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone9,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone9],
	})

	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone0,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone0],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone1,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone1],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone2,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone2],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone3,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone3],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone4,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone4],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone5,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone5],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone6,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone6],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone7,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone7],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone8,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone8],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone9,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone9],
	})

	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalDay,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalDay],
	})

	return buildinDataTypeR
}

func InitO2TBuildinDatatypeRule() []*BuildinDatatypeRule {
	var buildinDataTypeR []*BuildinDatatypeRule
	/*
		O2T Build-IN Compatible Rule
	*/
	// oracle column datatype name
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeNumber,
		DatatypeNameT: constant.BuildInOracleO2TNumberDatatypeNameMap[constant.BuildInOracleDatatypeNumber],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeBfile,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeBfile],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeChar,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeChar],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeCharacter,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeCharacter],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeClob,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeClob],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeBlob,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeBlob],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeDate,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeDate],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeDecimal,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeDecimal],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeDec,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeDec],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeDoublePrecision,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeDoublePrecision],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeFloat,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeFloat],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeInteger,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeInteger],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeInt,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeInt],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeLong,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeLong],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeLongRAW,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeLongRAW],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeBinaryFloat,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeBinaryFloat],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeBinaryDouble,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeBinaryDouble],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeNchar,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeNchar],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeNcharVarying,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeNcharVarying],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeNclob,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeNclob],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeNumeric,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeNumeric],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeNvarchar2,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeNvarchar2],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeRaw,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeRaw],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeReal,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeReal],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeRowid,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeRowid],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeUrowid,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeUrowid],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeSmallint,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeSmallint],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeVarchar2,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeVarchar2],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeVarchar,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeVarchar],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeXmltype,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeXmltype],
	})

	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp0,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp0],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp1,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp1],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp2,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp2],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp3,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp3],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp4,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp4],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp5,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp5],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp6,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp6],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp7,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp7],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp8,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp8],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestamp9,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestamp9],
	})

	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth0,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth0],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth1,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth1],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth2,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth2],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth3,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth3],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth4,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth4],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth5,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth5],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth6,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth6],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth7,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth7],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth8,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth8],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalYearMonth9,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalYearMonth9],
	})

	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone0,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone0],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone1,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone1],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone2,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone2],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone3,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone3],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone4,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone4],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone5,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone5],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone6,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone6],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone7,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone7],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone8,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone8],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithTimeZone9,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithTimeZone9],
	})

	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone0,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone0],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone1,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone1],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone2,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone2],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone3,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone3],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone4,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone4],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone5,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone5],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone6,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone6],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone7,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone7],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone8,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone8],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeTimestampWithLocalTimeZone9,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeTimestampWithLocalTimeZone9],
	})

	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInOracleDatatypeIntervalDay,
		DatatypeNameT: constant.BuildInOracleO2MDatatypeNameMap[constant.BuildInOracleDatatypeIntervalDay],
	})

	return buildinDataTypeR
}

func InitM2OBuildinDatatypeRule() []*BuildinDatatypeRule {
	var buildinDataTypeR []*BuildinDatatypeRule
	/*
		M2O Build-IN Compatible Rule
	*/
	// mysql column datatype name
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeBigint,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeBigint],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeDecimal,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeDecimal],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeDouble,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeDouble],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeDoublePrecision,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeDoublePrecision],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeFloat,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeFloat],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeInt,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeInt],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeInteger,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeInteger],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeMediumint,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeMediumint],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeNumeric,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeNumeric],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeReal,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeReal],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeSmallint,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeSmallint],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeTinyint,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeTinyint],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeBit,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeBit],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeDate,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeDate],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeDatetime,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeDatetime],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeTimestamp,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeTimestamp],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeTime,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeTime],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeYear,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeYear],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeBlob,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeBlob],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeChar,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeChar],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeLongBlob,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeLongBlob],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeLongText,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeLongText],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeMediumBlob,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeMediumBlob],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeMediumText,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeMediumText],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeText,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeText],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeTinyBlob,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeTinyBlob],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeTinyText,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeTinyText],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeVarchar,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeVarchar],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeBinary,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeBinary],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeMySQL,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeVarbinary,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeVarbinary],
	})
	return buildinDataTypeR
}

func InitT2OBuildinDatatypeRule() []*BuildinDatatypeRule {
	var buildinDataTypeR []*BuildinDatatypeRule
	/*
		T2O Build-IN Compatible Rule
	*/
	// tidb column datatype name
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeBigint,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeBigint],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeDecimal,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeDecimal],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeDouble,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeDouble],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeDoublePrecision,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeDoublePrecision],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeFloat,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeFloat],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeInt,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeInt],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeInteger,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeInteger],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeMediumint,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeMediumint],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeNumeric,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeNumeric],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeReal,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeReal],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeSmallint,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeSmallint],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeTinyint,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeTinyint],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeBit,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeBit],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeDate,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeDate],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeDatetime,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeDatetime],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeTimestamp,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeTimestamp],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeTime,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeTime],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeYear,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeYear],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeBlob,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeBlob],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeChar,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeChar],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeLongBlob,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeLongBlob],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeLongText,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeLongText],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeMediumBlob,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeMediumBlob],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeMediumText,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeMediumText],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeText,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeText],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeTinyBlob,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeTinyBlob],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeTinyText,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeTinyText],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeVarchar,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeVarchar],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeBinary,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeBinary],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypeTiDB,
		DBTypeT:       constant.DatabaseTypeOracle,
		DatatypeNameS: constant.BuildInMySQLDatatypeVarbinary,
		DatatypeNameT: constant.BuildInMySQLM2ODatatypeNameMap[constant.BuildInMySQLDatatypeVarbinary],
	})
	return buildinDataTypeR
}

func InitP2MBuildinDatatypeRule() []*BuildinDatatypeRule {
	var buildinDataTypeR []*BuildinDatatypeRule
	/*
		P2M Build-IN Compatible Rule
	*/
	// postgres column datatype name
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeInteger,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeInteger],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeSmallInt,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeSmallInt],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeBigInt,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeBigInt],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeBit,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeBit],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeBoolean,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeBoolean],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeReal,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeReal],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeDoublePrecision,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeDoublePrecision],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeNumeric,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeNumeric],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeDecimal,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeDecimal],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeMoney,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeMoney],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeCharacter,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeCharacter],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeCharacterVarying,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeCharacterVarying],
	})

	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeDate,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeDate],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeTimeWithoutTimeZone,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeTimeWithoutTimeZone],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeTimestampWithoutTimeZone,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeTimestampWithoutTimeZone],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeInterval,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeInterval],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeBytea,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeBytea],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeText,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeText],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeCidr,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeCidr],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeInet,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeInet],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeMacaddr,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeMacaddr],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeUuid,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeUuid],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeXml,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeXml],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeJson,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeJson],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeTsvector,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeTsvector],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeTsquery,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeTsquery],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeArray,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeArray],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypePoint,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypePoint],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeLine,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeLine],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeLseg,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeLseg],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeBox,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeBox],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypePath,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypePath],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypePolygon,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypePolygon],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeCircle,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeCircle],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeMySQL,
		DatatypeNameS: constant.BuildInPostgresDatatypeTxidSnapshot,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeTxidSnapshot],
	})
	return buildinDataTypeR
}

func InitP2TBuildinDatatypeRule() []*BuildinDatatypeRule {
	var buildinDataTypeR []*BuildinDatatypeRule
	/*
		P2M Build-IN Compatible Rule
	*/
	// postgres column datatype name
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeInteger,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeInteger],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeSmallInt,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeSmallInt],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeBigInt,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeBigInt],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeBit,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeBit],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeBoolean,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeBoolean],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeReal,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeReal],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeDoublePrecision,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeDoublePrecision],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeNumeric,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeNumeric],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeDecimal,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeDecimal],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeMoney,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeMoney],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeCharacter,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeCharacter],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeCharacterVarying,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeCharacterVarying],
	})

	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeDate,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeDate],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeTimeWithoutTimeZone,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeTimeWithoutTimeZone],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeTimestampWithoutTimeZone,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeTimestampWithoutTimeZone],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeInterval,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeInterval],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeBytea,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeBytea],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeText,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeText],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeCidr,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeCidr],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeInet,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeInet],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeMacaddr,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeMacaddr],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeUuid,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeUuid],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeXml,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeXml],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeJson,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeJson],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeTsvector,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeTsvector],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeTsquery,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeTsquery],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeArray,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeArray],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypePoint,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypePoint],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeLine,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeLine],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeLseg,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeLseg],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeBox,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeBox],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypePath,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypePath],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypePolygon,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypePolygon],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeCircle,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeCircle],
	})
	buildinDataTypeR = append(buildinDataTypeR, &BuildinDatatypeRule{
		DBTypeS:       constant.DatabaseTypePostgresql,
		DBTypeT:       constant.DatabaseTypeTiDB,
		DatatypeNameS: constant.BuildInPostgresDatatypeTxidSnapshot,
		DatatypeNameT: constant.BuildInPostgresP2MDatatypeNameMap[constant.BuildInPostgresDatatypeTxidSnapshot],
	})
	return buildinDataTypeR
}

// DatatypeSliceSplit used for the according to splitCounts, split slice
func DatatypeSliceSplit(items []*BuildinDatatypeRule, splitCounts int) [][]*BuildinDatatypeRule {
	subArraySize := len(items) / splitCounts

	result := make([][]*BuildinDatatypeRule, 0)

	for i := 0; i < splitCounts; i++ {
		start := i * subArraySize

		end := start + subArraySize

		if i == splitCounts-1 {
			end = len(items)
		}

		subArray := items[start:end]
		if len(subArray) > 0 {
			result = append(result, subArray)
		}
	}

	return result
}
