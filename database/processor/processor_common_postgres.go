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
	"strconv"
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
		return "", fmt.Errorf("postgres compatible database current config charset [%v] isn't support, support charset [%v]", connectDBCharsetT, stringutil.StringJoin(constant.MigrateDataSupportCharset, ","))
	}

	if strings.EqualFold(taskMode, constant.TaskModeStructMigrate) {
		dbCollation, err := databaseS.GetDatabaseCollation()
		if err != nil {
			return "", err
		}
		if _, ok := constant.MigrateTableStructureDatabaseCollationMap[taskFlow][stringutil.StringUpper(dbCollation)]; !ok {
			return "", fmt.Errorf("postgres database collation [%v] isn't support, only support collation [c,posix,zh_TW,zh_TW.UTF-8,zh_TW.UTF8,zh_CN,zh_CN.UTF-8,zh_CN.UTF8,en_US.UTF-8,en_US.UTF8]", dbCollation)
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
		return columnName, nil
	// character datatype
	case constant.BuildInPostgresDatatypeCharacter,
		constant.BuildInPostgresDatatypeCharacterVarying,
		constant.BuildInPostgresDatatypeText:
		return columnName, nil
	// date datatype
	case constant.BuildInPostgresDatatypeDate:
		return stringutil.StringBuilder(`TO_CHAR(`, columnName, `,'yyyy-mm-dd') AS `, columnName), nil
	// boolean
	case constant.BuildInPostgresDatatypeBoolean:
		return columnName, nil
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
		return stringutil.StringBuilder(`TO_CHAR(`, columnName, `,'yyyy-mm-dd hh24:mi:ss.us') AS `, columnName), nil

	// geometry datatype
	case constant.BuildInPostgresDatatypePoint,
		constant.BuildInPostgresDatatypeLine,
		constant.BuildInPostgresDatatypeLseg,
		constant.BuildInPostgresDatatypeBox,
		constant.BuildInPostgresDatatypePath,
		constant.BuildInPostgresDatatypePolygon,
		constant.BuildInPostgresDatatypeCircle:
		return columnName, nil
	// network addr datatype
	case constant.BuildInPostgresDatatypeCidr,
		constant.BuildInPostgresDatatypeInet,
		constant.BuildInPostgresDatatypeMacaddr:
		return columnName, nil
	// bit datatype
	case constant.BuildInPostgresDatatypeBit:
		return columnName, nil
	// text search datatype
	case constant.BuildInPostgresDatatypeTsvector,
		constant.BuildInPostgresDatatypeTsquery:
		return columnName, nil
	// xmltype datatype
	case constant.BuildInOracleDatatypeXmltype:
		return columnName, nil
	case constant.BuildInPostgresDatatypeBytea,
		constant.BuildInPostgresDatatypeUuid,
		constant.BuildInPostgresDatatypeXml,
		constant.BuildInPostgresDatatypeJson,
		constant.BuildInPostgresDatatypeArray:
		return columnName, nil
	case constant.BuildInPostgresDatatypeTxidSnapshot:
		return columnName, nil
	// other datatype
	default:
		return columnName, nil
	}
}

func OptimizerPostgresCompatibleMigrateMySQLCompatibleDataCompareColumnST(columnNameS, datatypeS string, datetimePrecisionS int64, dataPrecisionS, dataScaleS, dbCharsetSDest, columnNameT, dbCharsetTDest string) (string, string, error) {
	dataPrecisionV, err := strconv.Atoi(dataPrecisionS)
	if err != nil {
		return "", "", fmt.Errorf("aujust postgres compatible database table column [%s] datatype precision [%s] strconv.Atoi failed: %v", columnNameS, dataPrecisionS, err)
	}
	dataScaleV, err := strconv.Atoi(dataScaleS)
	if err != nil {
		return "", "", fmt.Errorf("aujust postgres compatible database table column [%s] scale [%s] strconv.Atoi failed: %v", columnNameS, dataScaleS, err)
	}
	nvlNullDecimalS := stringutil.StringBuilder(`COALESCE("`, columnNameS, `",0)`)
	nvlNullDecimalT := stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`)

	nvlNullStringS := stringutil.StringBuilder(`COALESCE("`, columnNameS, `",'0')`)
	nvlNullStringT := stringutil.StringBuilder(`IFNULL(`, columnNameT, `,'0')`)

	switch strings.ToUpper(datatypeS) {
	// numeric type
	case constant.BuildInPostgresDatatypeInteger,
		constant.BuildInPostgresDatatypeSmallInt,
		constant.BuildInPostgresDatatypeBigInt:
		return nvlNullDecimalS, nvlNullDecimalT, nil

	case constant.BuildInPostgresDatatypeNumeric,
		constant.BuildInPostgresDatatypeDecimal,
		constant.BuildInPostgresDatatypeMoney:
		if dataScaleV == 0 {
			return nvlNullDecimalS, nvlNullDecimalT, nil
		} else if dataScaleV > 0 {
			// TODO: optimizer
			// max decimal(65,30)
			// decimal(65,30) -> number, according to oracle database to_char max 62 digits format, translate to_char(35 position,27 position) -> decimal(65,30)
			toCharMax := 62
			if dataPrecisionV > toCharMax {
				return stringutil.StringBuilder(`TO_CHAR(`, nvlNullDecimalS, `,'FM`, stringutil.PaddingString(35, "9", "0"), `.`, stringutil.PaddingString(27, "9", "0"), `')`), stringutil.StringBuilder(`CAST(`, nvlNullDecimalT, ` AS DECIMAL(62,27))`), nil
			} else {
				toCharPaddingInteger := dataPrecisionV - dataScaleV
				return stringutil.StringBuilder(`TO_CHAR(`, nvlNullDecimalS, `,'FM`, stringutil.PaddingString(toCharPaddingInteger, "9", "0"), `.`, stringutil.PaddingString(dataScaleV, "9", "0"), `')`), stringutil.StringBuilder(`CAST(`, nvlNullDecimalT, ` AS DECIMAL(`, strconv.Itoa(dataPrecisionV), `,`, strconv.Itoa(dataScaleV), `))`), nil
			}
		} else {
			return "", "", fmt.Errorf("the postgres compatible database table column [%s] datatype [%s] data_scale value [%d] cannot less zero, please contact author or reselect", columnNameS, datatypeS, dataScaleV)
		}

	case constant.BuildInPostgresDatatypeReal:
		return stringutil.StringBuilder(`RPAD(TO_CHAR(`, nvlNullDecimalS, `,'FM99999999999999999999999999999999999990.9999990'),8,0)`), stringutil.StringBuilder(`RPAD(CAST(`, nvlNullDecimalT, ` AS DECIMAL(65,7)),8,0)`), nil
	case constant.BuildInPostgresDatatypeDoublePrecision:
		return stringutil.StringBuilder(`RPAD(TO_CHAR(`, nvlNullDecimalS, `,'FM99999999999999999999999999999999999990.999999999999990'),16,0)`), stringutil.StringBuilder(`RPAD(CAST(`, nvlNullDecimalT, ` AS DECIMAL(65,15)),16,0)`), nil
	// character datatype
	case constant.BuildInPostgresDatatypeCharacter,
		constant.BuildInPostgresDatatypeCharacterVarying,
		constant.BuildInPostgresDatatypeText:
		return stringutil.StringBuilder(`MD5(CONVERT_TO(`, nvlNullStringS, `,'`, dbCharsetSDest, `'))`), stringutil.StringBuilder(`MD5(CONVERT(`, nvlNullStringT, ` USING '`, dbCharsetTDest, `'))`), nil
	// date datatype
	case constant.BuildInPostgresDatatypeDate:
		return stringutil.StringBuilder(`COALESCE(TO_CHAR("`, columnNameS, `",'YYYY-MM-DD'),'0')`), stringutil.StringBuilder(`IFNULL(DATE_FORMAT(`, columnNameT, `,'%%Y-%%m-%%d'),'0')`), nil
	// boolean
	case constant.BuildInPostgresDatatypeBoolean:
		return stringutil.StringBuilder(`CASE COALESCE("`, columnNameS, `",false) WHEN true THEN 1 WHEN false THEN 0 ELSE -100001 END AS "`, columnNameS, `"`),
			stringutil.StringBuilder(`CASE IFNULL(`, columnNameT, `,0) WHEN 1 THEN 1 WHEN 0 THEN 0 ELSE `, columnNameT, `END AS `, columnNameT), nil
	// time datatype
	case constant.BuildInPostgresDatatypeTimeWithoutTimeZone,
		constant.BuildInPostgresDatatypeTimestampWithoutTimeZone,
		constant.BuildInPostgresDatatypeInterval:
		if datetimePrecisionS == 0 {
			return nvlNullStringS, nvlNullStringT, nil
		} else {
			return stringutil.StringBuilder(`TO_CHAR("`, columnNameS, `",'yyyy-mm-dd hh24:mi:ss.us') AS "`, columnNameS, `"`), stringutil.StringBuilder(`DATE_FORMAT(`, columnNameT, `,'%%Y-%%m-%%d %%H:%%i:%%s.%%f') AS `, columnNameT), nil
		}
	// geometry datatype
	case constant.BuildInPostgresDatatypePoint,
		constant.BuildInPostgresDatatypeLine,
		constant.BuildInPostgresDatatypeLseg,
		constant.BuildInPostgresDatatypeBox,
		constant.BuildInPostgresDatatypePath,
		constant.BuildInPostgresDatatypePolygon,
		constant.BuildInPostgresDatatypeCircle:
		return nvlNullDecimalS, nvlNullDecimalT, nil
	// network addr datatype
	case constant.BuildInPostgresDatatypeCidr,
		constant.BuildInPostgresDatatypeInet,
		constant.BuildInPostgresDatatypeMacaddr:
		return nvlNullDecimalS, nvlNullDecimalT, nil
	// bit datatype
	case constant.BuildInPostgresDatatypeBit:
		return nvlNullDecimalS, nvlNullDecimalT, nil
	// text search datatype
	case constant.BuildInPostgresDatatypeTsvector,
		constant.BuildInPostgresDatatypeTsquery:
		return nvlNullDecimalS, nvlNullDecimalT, nil
	// xmltype datatype
	case constant.BuildInOracleDatatypeXmltype:
		return nvlNullDecimalS, nvlNullDecimalT, nil
	case constant.BuildInPostgresDatatypeBytea,
		constant.BuildInPostgresDatatypeXml,
		constant.BuildInPostgresDatatypeJson,
		constant.BuildInPostgresDatatypeArray:
		return stringutil.StringBuilder(`MD5(`, nvlNullStringS, `)`), stringutil.StringBuilder(`MD5(`, nvlNullStringT, `)`), nil
	case constant.BuildInPostgresDatatypeUuid:
		return nvlNullDecimalS, nvlNullDecimalT, nil
	case constant.BuildInPostgresDatatypeTxidSnapshot:
		return nvlNullDecimalS, nvlNullDecimalT, nil
	// other datatype
	default:
		return nvlNullDecimalS, nvlNullDecimalT, nil
	}
}
