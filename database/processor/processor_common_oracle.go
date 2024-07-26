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

func InspectOracleMigrateTask(taskName, taskFlow, taskMode string, databaseS database.IDatabase, connectDBCharsetS, connectDBCharsetT string) (string, string, error) {
	if strings.EqualFold(taskMode, constant.TaskModeStructMigrate) {
		dbCharsetMapping := constant.MigrateTableStructureDatabaseCharsetMap[taskFlow][stringutil.StringUpper(connectDBCharsetS)]
		if !strings.EqualFold(connectDBCharsetT, dbCharsetMapping) {
			return "", "", fmt.Errorf("oracle current task [%s] taskflow [%s] charset [%s] mapping [%v] isn't equal with database connect charset [%s], please adjust database connect charset", taskName, taskFlow, connectDBCharsetS, dbCharsetMapping, connectDBCharsetT)
		}
	}

	version, err := databaseS.GetDatabaseVersion()
	if err != nil {
		return version, "", err
	}

	// whether the oracle version can specify table and field collation，if the oracle database version is 12.2 and the above version, it's specify table and field collation, otherwise can't specify
	// oracle database nls_sort/nls_comp value need to be equal, USING_NLS_COMP value is nls_comp
	dbCharsetS, err := databaseS.GetDatabaseCharset()
	if err != nil {
		return version, "", err
	}
	if !strings.EqualFold(connectDBCharsetS, dbCharsetS) {
		zap.L().Warn("oracle charset and oracle config charset",
			zap.String("oracle database charset", dbCharsetS),
			zap.String("oracle config charset", connectDBCharsetS))
		return version, "", fmt.Errorf("oracle database charset [%v] and oracle config charset [%v] aren't equal, please adjust oracle config charset", dbCharsetS, connectDBCharsetS)
	}
	if _, ok := constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(connectDBCharsetS)]; !ok {
		return version, "", fmt.Errorf("oracle database charset [%v] isn't support, only support charset [%v]", dbCharsetS, stringutil.StringPairKey(constant.MigrateOracleCharsetStringConvertMapping))
	}
	if !stringutil.IsContainedString(constant.MigrateDataSupportCharset, stringutil.StringUpper(connectDBCharsetT)) {
		return version, "", fmt.Errorf("mysql compatible database current config charset [%v] isn't support, support charset [%v]", connectDBCharsetT, stringutil.StringJoin(constant.MigrateDataSupportCharset, ","))
	}

	if strings.EqualFold(taskMode, constant.TaskModeStructMigrate) {
		nlsComp, err := databaseS.GetDatabaseCollation()
		if err != nil {
			return version, "", err
		}
		if _, ok := constant.MigrateTableStructureDatabaseCollationMap[taskFlow][stringutil.StringUpper(nlsComp)]; !ok {
			return version, "", fmt.Errorf("oracle database collation nlssort [%v] isn't support", nlsComp)
		}

		if _, ok := constant.MigrateTableStructureDatabaseCollationMap[taskFlow][stringutil.StringUpper(nlsComp)]; !ok {
			return version, "", fmt.Errorf("oracle database collation nlscomp [%v] isn't support", nlsComp)
		}

		return version, nlsComp, nil
	}

	return version, "", nil
}

func OptimizerOracleDataMigrateColumnS(columnName, datatype, dataScale string) (string, error) {
	switch strings.ToUpper(datatype) {
	// numeric type
	case constant.BuildInOracleDatatypeNumber:
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	case constant.BuildInOracleDatatypeDecimal, constant.BuildInOracleDatatypeDec, constant.BuildInOracleDatatypeDoublePrecision,
		constant.BuildInOracleDatatypeFloat, constant.BuildInOracleDatatypeInteger, constant.BuildInOracleDatatypeInt,
		constant.BuildInOracleDatatypeReal, constant.BuildInOracleDatatypeNumeric, constant.BuildInOracleDatatypeBinaryFloat,
		constant.BuildInOracleDatatypeBinaryDouble, constant.BuildInOracleDatatypeSmallint:
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	// character datatype
	case constant.BuildInOracleDatatypeCharacter, constant.BuildInOracleDatatypeLong, constant.BuildInOracleDatatypeNcharVarying,
		constant.BuildInOracleDatatypeVarchar, constant.BuildInOracleDatatypeChar, constant.BuildInOracleDatatypeNchar, constant.BuildInOracleDatatypeVarchar2, constant.BuildInOracleDatatypeNvarchar2, constant.BuildInOracleDatatypeNclob,
		constant.BuildInOracleDatatypeClob:
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	case constant.BuildInOracleDatatypeBfile:
		// bfile can convert blob through to_lob, current keep store bfilename
		// https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/TO_BLOB-bfile.html#GUID-232A1599-53C9-464B-904F-4DBA336B4EBC
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	case constant.BuildInOracleDatatypeRowid, constant.BuildInOracleDatatypeUrowid:
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
		// xmltype datatype
	case constant.BuildInOracleDatatypeXmltype:
		return fmt.Sprintf(` XMLSERIALIZE(CONTENT "%s" AS CLOB) AS "%s"`, columnName, columnName), nil
	// binary datatype
	case constant.BuildInOracleDatatypeBlob, constant.BuildInOracleDatatypeLongRAW, constant.BuildInOracleDatatypeRaw:
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	// time datatype
	case constant.BuildInOracleDatatypeDate:
		return stringutil.StringBuilder(`TO_CHAR("`, columnName, `",'yyyy-mm-dd hh24:mi:ss') AS "`, columnName, `"`), nil
	// other datatype
	default:
		if strings.Contains(datatype, "INTERVAL") {
			return stringutil.StringBuilder(`TO_CHAR("`, columnName, `") AS "`, columnName, `"`), nil
		} else if strings.Contains(datatype, "TIMESTAMP") {
			dataScaleV, err := strconv.Atoi(dataScale)
			if err != nil {
				return "", fmt.Errorf("aujust oracle timestamp datatype scale [%s] strconv.Atoi failed: %v", dataScale, err)
			}
			if dataScaleV == 0 {
				return stringutil.StringBuilder(`TO_CHAR("`, columnName, `",'yyyy-mm-dd hh24:mi:ss') AS "`, columnName, `"`), nil
			} else if dataScaleV > 0 && dataScaleV <= 6 {
				return stringutil.StringBuilder(`TO_CHAR("`, columnName,
					`",'yyyy-mm-dd hh24:mi:ss.ff`, strconv.Itoa(dataScaleV), `') AS "`, columnName, `"`), nil
			} else {
				return stringutil.StringBuilder(`TO_CHAR("`, columnName, `",'yyyy-mm-dd hh24:mi:ss.ff6') AS "`, columnName, `"`), nil
			}
		} else {
			return stringutil.StringBuilder(`"`, columnName, `"`), nil
		}
	}
}

func OptimizerOracleMigrateMYSQLCompatibleDataCompareColumnST(columnNameS, datatypeS string, dataLengthS int64, dataPrecisionS, dataScaleS, dbCharsetSFrom, dbCharsetSDest, columnNameT, dbCharsetTDest string) (string, string, error) {
	dataPrecisionV, err := strconv.Atoi(dataPrecisionS)
	if err != nil {
		return "", "", fmt.Errorf("aujust oracle table column [%s] datatype precision [%s] strconv.Atoi failed: %v", columnNameS, dataPrecisionS, err)
	}
	dataScaleV, err := strconv.Atoi(dataScaleS)
	if err != nil {
		return "", "", fmt.Errorf("aujust oracle table column [%s] datatype scale [%s] strconv.Atoi failed: %v", columnNameS, dataScaleS, err)
	}
	nvlNullDecimalS := stringutil.StringBuilder(`NVL("`, columnNameS, `",0)`)
	nvlNullDecimalT := stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`)
	nvlNullStringS := stringutil.StringBuilder(`NVL("`, columnNameS, `",'0')`)
	nvlNullStringT := stringutil.StringBuilder(`IFNULL(`, columnNameT, `,'0')`)

	switch strings.ToUpper(datatypeS) {
	// numeric type
	case constant.BuildInOracleDatatypeNumber, constant.BuildInOracleDatatypeDecimal, constant.BuildInOracleDatatypeDec, constant.BuildInOracleDatatypeNumeric:
		if dataScaleV == 0 {
			return nvlNullDecimalS, nvlNullDecimalT, nil
		} else if dataScaleV > 0 {
			// dataPrecisionV max value 38
			if dataScaleV < dataPrecisionV {
				inteNums := dataPrecisionV - dataScaleV
				return stringutil.StringBuilder(`TO_CHAR(`, nvlNullDecimalS, `,'FM`, stringutil.PaddingString(inteNums, "9", "0"), `.`, stringutil.PaddingString(dataScaleV, "9", "0"), `')`), stringutil.StringBuilder(`CAST(`, nvlNullDecimalT, ` AS DECIMAL(`, dataPrecisionS, `,`, dataScaleS, `))`), nil
			}

			// TODO:
			// number, number(*), number(38,*) SQL query will be processed into number(38,127). It is impossible to distinguish between manual writing and query processing. If number(38,127) is manually written in the database, dataScaleV should be used normally. >30 logic, no distinction is made here
			// MYSQL compatible database data scale decimal max is 30, and oracle database to_char max 62 digits
			// number、number(*)、number(38,*)、number(38,127)
			if dataScaleV == 127 && dataPrecisionV == 38 {
				// number -> to_char(number(38,24))
				// decimal(65,24)
				return stringutil.StringBuilder(`TO_CHAR(`, nvlNullDecimalS, `,'FM`, stringutil.PaddingString(dataPrecisionV, "9", "0"), `.`, stringutil.PaddingString(62-dataPrecisionV, "9", "0"), `')`), stringutil.StringBuilder(`CAST(`, nvlNullDecimalT, ` AS DECIMAL(65,`, strconv.Itoa(62-dataPrecisionV), `))`), nil
			}

			// oracle dataScale > dataPrecision, eg: number(2,6) indicates that decimals without integers are stored.
			// Regarding precision, scale can also be expressed as follows
			//The precision (p) and scale (s) of fixed-point numbers follow the following rules:
			//1) When the length of the integer part of a number > p-s, Oracle will report an error
			//2) When the length of the decimal part of a number > s, Oracle will round.
			//3) When s(scale) is a negative number, Oracle rounds the s numbers to the left of the decimal point.
			//4) When s > p, p represents the maximum number of digits to the left of the sth digit after the decimal point. If it is greater than p, Oracle will report an error, and the digits to the right of the sth digit after the decimal point will be rounded.
			if dataScaleV == 30 {
				return stringutil.StringBuilder(`TO_CHAR(`, nvlNullDecimalS, `,'FM0.`, stringutil.PaddingString(dataScaleV, "9", "0"), `')`), stringutil.StringBuilder(`CAST(`, nvlNullDecimalT, ` AS DECIMAL(65,30))`), nil
			}

			// normal compare
			if dataScaleV > 30 {
				// max 30
				return stringutil.StringBuilder(`TO_CHAR(`, nvlNullDecimalS, `,'FM0.`, stringutil.PaddingString(30, "9", "0"), `')`), stringutil.StringBuilder(`CAST(`, nvlNullDecimalT, ` AS DECIMAL(65,30))`), nil
			}

			return stringutil.StringBuilder(`TO_CHAR(`, nvlNullDecimalS, `,'FM0.`, stringutil.PaddingString(dataScaleV, "9", "0"), `')`), stringutil.StringBuilder(`CAST(`, nvlNullDecimalT, ` AS DECIMAL(65,`, dataScaleS, `))`), nil
		} else {
			// 8--4
			inteNums := dataPrecisionV - dataScaleV
			return stringutil.StringBuilder(`TO_CHAR(`, nvlNullDecimalS, `,'FM`, stringutil.PaddingString(inteNums, "9", "0")), stringutil.StringBuilder(`CAST(`, nvlNullDecimalT, ` AS DECIMAL(`, strconv.Itoa(inteNums), `,0))`), nil
		}
	case constant.BuildInOracleDatatypeInt, constant.BuildInOracleDatatypeInteger, constant.BuildInOracleDatatypeSmallint:
		return nvlNullDecimalS, nvlNullDecimalT, nil

	case constant.BuildInOracleDatatypeDoublePrecision, constant.BuildInOracleDatatypeBinaryDouble:
		return stringutil.StringBuilder(`RPAD(TO_CHAR(`, nvlNullDecimalS, `,'FM99999999999999999999999999999999999990.999999999999990'),16,0)`), stringutil.StringBuilder(`RPAD(CAST(`, nvlNullDecimalT, ` AS DECIMAL(65,15)),16,0)`), nil

	case constant.BuildInOracleDatatypeFloat, constant.BuildInOracleDatatypeReal, constant.BuildInOracleDatatypeBinaryFloat:
		return stringutil.StringBuilder(`RPAD(TO_CHAR(`, nvlNullDecimalS, `,'FM99999999999999999999999999999999999990.9999990'),8,0)`), stringutil.StringBuilder(`RPAD(CAST(`, nvlNullDecimalT, ` AS DECIMAL(65,7)),8,0)`), nil

	// character datatype
	case constant.BuildInOracleDatatypeCharacter, constant.BuildInOracleDatatypeVarchar, constant.BuildInOracleDatatypeChar, constant.BuildInOracleDatatypeVarchar2:
		// in order to save character length, cut more than varchar2. For character bytes smaller than 32, do not use HASH or destroy them in advance.
		if dataLengthS <= int64(constant.DataCompareMethodCheckMD5ValueLength) {
			return stringutil.StringBuilder(`CONVERT(TO_CLOB(`, nvlNullStringS, `),'`, dbCharsetSDest, `','`, dbCharsetSFrom, `')`), stringutil.StringBuilder(`CONVERT(`, nvlNullStringT, ` USING '`, dbCharsetTDest, `')`), nil
		} else {
			return stringutil.StringBuilder(`UPPER(DBMS_CRYPTO.HASH(CONVERT(TO_CLOB(`, nvlNullStringS, `),'`, dbCharsetSDest, `','`, dbCharsetSFrom, `'),2))`), stringutil.StringBuilder(`UPPER(MD5(CONVERT(`, nvlNullStringT, ` USING '`, dbCharsetTDest, `')))`), nil
		}
	case constant.BuildInOracleDatatypeNcharVarying, constant.BuildInOracleDatatypeNchar, constant.BuildInOracleDatatypeNvarchar2:
		// in order to save character length, cut more than varchar2. For character bytes smaller than 32, do not use HASH or destroy them in advance.
		if dataLengthS <= int64(constant.DataCompareMethodCheckMD5ValueLength) {
			return stringutil.StringBuilder(`CONVERT(TO_CLOB(`, nvlNullStringS, `),'`, dbCharsetSDest, `','`, dbCharsetSFrom, `')`), stringutil.StringBuilder(`CONVERT(`, nvlNullStringT, ` USING '`, dbCharsetTDest, `')`), nil
		} else {
			return stringutil.StringBuilder(`UPPER(DBMS_CRYPTO.HASH(CONVERT(TO_CLOB(`, nvlNullStringS, `),'`, dbCharsetSDest, `','`, dbCharsetSFrom, `'),2))`), stringutil.StringBuilder(`UPPER(MD5(CONVERT(`, nvlNullStringT, ` USING '`, dbCharsetTDest, `')))`), nil
		}
	case constant.BuildInOracleDatatypeClob:
		return stringutil.StringBuilder(`UPPER(DBMS_CRYPTO.HASH(CONVERT(TO_CLOB(`, nvlNullStringS, `),'`, dbCharsetSDest, `','`, dbCharsetSFrom, `'),2))`), stringutil.StringBuilder(`UPPER(MD5(CONVERT(`, nvlNullStringT, ` USING '`, dbCharsetTDest, `')))`), nil
	case constant.BuildInOracleDatatypeNclob:
		return stringutil.StringBuilder(`UPPER(DBMS_CRYPTO.HASH(CONVERT(TO_CLOB(`, nvlNullStringS, `),'`, dbCharsetSDest, `','`, dbCharsetSFrom, `'),2))`), stringutil.StringBuilder(`UPPER(MD5(CONVERT(`, nvlNullStringT, ` USING '`, dbCharsetTDest, `')))`), nil
	case constant.BuildInOracleDatatypeRowid, constant.BuildInOracleDatatypeUrowid:
		return stringutil.StringBuilder(`NVL(ROWIDTOCHAR("`, columnNameS, `"),'0')`), nvlNullStringT, nil
	// xmltype datatype
	case constant.BuildInOracleDatatypeXmltype:
		return stringutil.StringBuilder(`UPPER((DBMS_CRYPTO.HASH(CONVERT(XMLSERIALIZE(CONTENT `, nvlNullStringS, ` AS CLOB),'`, dbCharsetSDest, `','`, dbCharsetSFrom, `'),2)))`), stringutil.StringBuilder(`UPPER(MD5(CONVERT(`, nvlNullStringT, ` USING '`, dbCharsetTDest, `')))`), nil
	// binary datatype
	case constant.BuildInOracleDatatypeBlob, constant.BuildInOracleDatatypeRaw:
		return stringutil.StringBuilder(`UPPER(DBMS_CRYPTO.HASH(TO_BLOB(`, nvlNullStringS, `),2))`), stringutil.StringBuilder(`UPPER(MD5(`, nvlNullStringT, `))`), nil
	case constant.BuildInOracleDatatypeLong, constant.BuildInOracleDatatypeLongRAW, constant.BuildInOracleDatatypeBfile:
		// can't appear, if the column datatype appear long or long raw, it will be disabled checksum
		return stringutil.StringBuilder(`"`, columnNameS, `"`), columnNameT, nil
	// TODO: BFILE COMPARE SUPPORT
	// oracle bfile datatype -> mysql compatible database varchar, so if the data compare meet bfile, it will be disabled checksum
	//case constant.BuildInOracleDatatypeBfile:
	//	return stringutil.StringBuilder(`UPPER(DBMS_CRYPTO.HASH(TO_CLOB("`, columnNameS, `"),2))`), stringutil.StringBuilder(`UPPER(MD5(CONVERT(`, columnNameT, ` USING '`, dbCharsetTDest, `')))`), nil
	// time datatype
	case constant.BuildInOracleDatatypeDate:
		return stringutil.StringBuilder(`NVL(TO_CHAR("`, columnNameS, `",'YYYY-MM-DD HH24:MI:SS'),'0')`), nvlNullStringT, nil
	// other datatype
	default:
		if strings.Contains(datatypeS, "INTERVAL") {
			return stringutil.StringBuilder(`NVL(TO_CHAR("`, columnNameS, `"),'0')`), nvlNullStringT, nil
		} else if strings.Contains(datatypeS, "TIMESTAMP") {
			if dataScaleV == 0 {
				return stringutil.StringBuilder(`NVL(TO_CHAR("`, columnNameS, `",'YYYY-MM-DD HH24:MI:SS'),'0')`), nvlNullStringT, nil
			} else if dataScaleV > 0 && dataScaleV <= 6 {
				return stringutil.StringBuilder(`NVL(TO_CHAR("`, columnNameS, `",'YYYY-MM-DD HH24:MI:SS.FF`, strconv.Itoa(dataScaleV), `'),'0')`), nvlNullStringT, nil
			} else {
				return stringutil.StringBuilder(`NVL(TO_CHAR("`, columnNameS, `",'YYYY-MM-DD HH24:MI:SS.FF6'),'0')`), nvlNullStringT, nil
			}
		} else {
			return nvlNullStringS, nvlNullStringT, nil
		}
	}
}
