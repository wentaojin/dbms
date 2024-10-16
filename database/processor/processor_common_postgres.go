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
	"strings"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
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
			return "", fmt.Errorf("postgres database collation [%v] isn't support, only support collation [c,posix,zh_TW,zh_TW.utf8,zh_CN,zh_CN.utf8,en_US.utf8]", dbCollation)
		}

		return dbCollation, nil
	}
	return "", nil
}

func OptimizerPostgresDataMigrateColumnS(columnName, datatype, datetimePrecision string) (string, error) {
	switch strings.ToUpper(datatype) {
	// numeric type
	case constant.BuildInPostgresDatatypeInteger,
		constant.BuildInPostgresDatatypeSmallInt,
		constant.BuildInPostgresDatatypeBigInt,
		constant.BuildInPostgresDatatypeNumeric,
		constant.BuildInPostgresDatatypeDecimal,
		constant.BuildInPostgresDatatypeMoney,
		constant.BuildInPostgresDatatypeReal,
		constant.BuildInPostgresDatatypeDoublePrecision:
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	// character datatype
	case constant.BuildInPostgresDatatypeCharacter,
		constant.BuildInPostgresDatatypeCharacterVarying,
		constant.BuildInPostgresDatatypeText:
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	// date datatype
	case constant.BuildInPostgresDatatypeDate:
		return stringutil.StringBuilder(`TO_CHAR("`, columnName, `",'yyyy-mm-dd') AS "`, columnName, `"`), nil
	// boolean
	case constant.BuildInPostgresDatatypeBoolean:
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	// time datatype
	case constant.BuildInPostgresDatatypeTimeWithoutTimeZone,
		constant.BuildInPostgresDatatypeTimestampWithoutTimeZone,
		constant.BuildInPostgresDatatypeInterval:
		// dateScaleV, err := strconv.Atoi(datetimePrecision)
		// if err != nil {
		// 	return "", fmt.Errorf("aujust postgres time datatype scale [%s] strconv.Atoi failed: %v", datetimePrecision, err)
		// }
		// if dateScaleV == 0 {
		// 	return stringutil.StringBuilder(`TO_CHAR("`, columnName, `",'yyyy-mm-dd hh24:mi:ss') AS "`, columnName, `"`), nil
		// } else if dateScaleV > 0 && dateScaleV <= 6 {
		// 	return stringutil.StringBuilder(`TO_CHAR("`, columnName,
		// 		`",'yyyy-mm-dd hh24:mi:ss.ff`, strconv.Itoa(dateScaleV), `') AS "`, columnName, `"`), nil
		// } else {
		// 	return stringutil.StringBuilder(`TO_CHAR("`, columnName, `",'yyyy-mm-dd hh24:mi:ss.ff6') AS "`, columnName, `"`), nil
		// }
		// postgres 9.5 ff cannot format
		/*
			marvin=> select "id","n",TO_CHAR("d",'yyyy-mm-dd') AS "d",TO_CHAR("t",'YYYY-MM-DD HH24:MI:SS.FF4') AS "t" from marvin.marvin04
			marvin-> ;
			 id | n |     d      |            t
			----+---+------------+-------------------------
			  1 | t |            |
			  2 | f |            |
			  3 |   |            |
			  4 | t | 2024-10-14 | 2024-10-14 02:44:47.FF4
		*/
		return stringutil.StringBuilder(`TO_CHAR("`, columnName, `",'yyyy-mm-dd hh24:mi:ss.us') AS "`, columnName, `"`), nil

	// geometry datatype
	case constant.BuildInPostgresDatatypePoint,
		constant.BuildInPostgresDatatypeLine,
		constant.BuildInPostgresDatatypeLseg,
		constant.BuildInPostgresDatatypeBox,
		constant.BuildInPostgresDatatypePath,
		constant.BuildInPostgresDatatypePolygon,
		constant.BuildInPostgresDatatypeCircle:
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	// network addr datatype
	case constant.BuildInPostgresDatatypeCidr,
		constant.BuildInPostgresDatatypeInet,
		constant.BuildInPostgresDatatypeMacaddr:
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	// bit datatype
	case constant.BuildInPostgresDatatypeBit:
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	// text search datatype
	case constant.BuildInPostgresDatatypeTsvector,
		constant.BuildInPostgresDatatypeTsquery:
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	// xmltype datatype
	case constant.BuildInOracleDatatypeXmltype:
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	case constant.BuildInPostgresDatatypeBytea,
		constant.BuildInPostgresDatatypeUuid,
		constant.BuildInPostgresDatatypeXml,
		constant.BuildInPostgresDatatypeJson,
		constant.BuildInPostgresDatatypeArray:
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	case constant.BuildInPostgresDatatypeTxidSnapshot:
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	// other datatype
	default:
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	}
}
