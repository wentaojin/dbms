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
package taskflow

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
)

func InspectMigrateTask(databaseS database.IDatabase, connectDBCharsetS, connectDBCharsetT string) (bool, error) {
	oracleDBVersion, err := databaseS.GetDatabaseVersion()
	if err != nil {
		return false, err
	}

	if stringutil.VersionOrdinal(oracleDBVersion) < stringutil.VersionOrdinal(constant.OracleDatabaseTableAndColumnSupportVersion) {
		return false, fmt.Errorf("oracle db version [%v] is less than 11g, can't be using the current platform", oracleDBVersion)
	}

	oracleCollation := false
	if stringutil.VersionOrdinal(oracleDBVersion) >= stringutil.VersionOrdinal(constant.OracleDatabaseTableAndColumnSupportVersion) {
		oracleCollation = true
	}

	dbCharsetS, err := databaseS.GetDatabaseCharset()
	if err != nil {
		return false, err
	}
	if !strings.EqualFold(connectDBCharsetS, dbCharsetS) {
		zap.L().Warn("oracle charset and oracle config charset",
			zap.String("oracle database charset", dbCharsetS),
			zap.String("oracle config charset", connectDBCharsetS))
		return false, fmt.Errorf("oracle database charset [%v] and oracle config charset [%v] aren't equal, please adjust oracle config charset", dbCharsetS, connectDBCharsetS)
	}
	if _, ok := constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(connectDBCharsetS)]; !ok {
		return false, fmt.Errorf("oracle database charset [%v] isn't support, only support charset [%v]", dbCharsetS, stringutil.StringPairKey(constant.MigrateOracleCharsetStringConvertMapping))
	}
	if !stringutil.IsContainedString(constant.MigrateDataSupportCharset, stringutil.StringUpper(connectDBCharsetT)) {
		return false, fmt.Errorf("mysql current config charset [%v] isn't support, support charset [%v]", connectDBCharsetT, stringutil.StringJoin(constant.MigrateDataSupportCharset, ","))
	}
	return oracleCollation, nil
}

func optimizerDataMigrateColumnS(columnName, datatype, dataScale string) (string, error) {
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

func optimizerMYSQLCompatibleDataCompareColumnST(columnNameS, datatypeS string, dataLengthS int64, dataPrecisionS, dataScaleS, dbCharsetSFrom, dbCharsetSDest, columnNameT, dbCharsetTDest string) (string, string, error) {
	dataPrecisionV, err := strconv.Atoi(dataPrecisionS)
	if err != nil {
		return "", "", fmt.Errorf("aujust oracle timestamp datatype precision [%s] strconv.Atoi failed: %v", dataPrecisionS, err)
	}
	dataScaleV, err := strconv.Atoi(dataScaleS)
	if err != nil {
		return "", "", fmt.Errorf("aujust oracle timestamp datatype scale [%s] strconv.Atoi failed: %v", dataScaleS, err)
	}
	switch strings.ToUpper(datatypeS) {
	// numeric type
	case constant.BuildInOracleDatatypeNumber, constant.BuildInOracleDatatypeDecimal, constant.BuildInOracleDatatypeDec, constant.BuildInOracleDatatypeNumeric:
		if dataScaleV == 0 {
			return stringutil.StringBuilder(`NVL("`, columnNameS, `",0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
		} else if dataScaleV > 0 {
			if dataScaleV < dataPrecisionV {
				inteNums := dataPrecisionV - dataScaleV
				return stringutil.StringBuilder(`TO_CHAR("`, columnNameS, `",'FM`, stringutil.PaddingString(inteNums, "9", "0"), `.`, stringutil.PaddingString(dataScaleV, "9", "0"), `')`), stringutil.StringBuilder(`IFNULL(CAST(`, columnNameT, ` AS DECIMAL(`, dataPrecisionS, `,`, dataScaleS, `)),0)`), nil
			}
			// oracle dataScale > dataPrecision
			// MYSQL compatible database data scale decimal max is 30, and oracle database to_char max 62 digits
			if dataScaleV >= 30 {
				return stringutil.StringBuilder(`TO_CHAR("`, columnNameS, `",'FM`, stringutil.PaddingString(dataPrecisionV, "9", "0"), `.`, stringutil.PaddingString(62-dataPrecisionV, "9", "0"), `')`), stringutil.StringBuilder(`IFNULL(CAST(`, columnNameT, ` AS DECIMAL(65,`, strconv.Itoa(62-dataPrecisionV), `)),0)`), nil
			}
			return stringutil.StringBuilder(`TO_CHAR("`, columnNameS, `",'FM`, stringutil.PaddingString(dataPrecisionV, "9", "0"), `.`, stringutil.PaddingString(62-dataPrecisionV, "9", "0"), `')`), stringutil.StringBuilder(`IFNULL(CAST(`, columnNameT, ` AS DECIMAL(65,`, strconv.Itoa(62-dataPrecisionV), `)),0)`), nil
		} else {
			inteNums := dataPrecisionV - dataPrecisionV
			return stringutil.StringBuilder(`TO_CHAR("`, columnNameS, `",'FM`, stringutil.PaddingString(inteNums, "9", "0")), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
		}
	case constant.BuildInOracleDatatypeInt, constant.BuildInOracleDatatypeInteger, constant.BuildInOracleDatatypeSmallint:
		return stringutil.StringBuilder(`NVL("`, columnNameS, `",0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil

	case constant.BuildInOracleDatatypeDoublePrecision, constant.BuildInOracleDatatypeBinaryDouble:
		return stringutil.StringBuilder(`RPAD(TO_CHAR("`, columnNameS, `",'FM99999999999999999999999999999999999990.999999999999990'),16,0)`), stringutil.StringBuilder(`RPAD(CAST(`, columnNameT, ` AS DECIMAL(65,15))),16,0)`), nil

	case constant.BuildInOracleDatatypeFloat, constant.BuildInOracleDatatypeReal, constant.BuildInOracleDatatypeBinaryFloat:
		return stringutil.StringBuilder(`RPAD(TO_CHAR("`, columnNameS, `",'FM99999999999999999999999999999999999990.9999990'),8,0)`), stringutil.StringBuilder(`RPAD(CAST(`, columnNameT, ` AS DECIMAL(65,7)),8,0)`), nil

	// character datatype
	case constant.BuildInOracleDatatypeCharacter, constant.BuildInOracleDatatypeVarchar, constant.BuildInOracleDatatypeChar, constant.BuildInOracleDatatypeVarchar2:
		// in order to save character length, cut more than varchar2. For character bytes smaller than 32, do not use HASH or destroy them in advance.
		if dataLengthS <= int64(constant.DataCompareMethodCheckMD5ValueLength) {
			return stringutil.StringBuilder(`NVL(CONVERT(TO_CLOB("`, columnNameS, `"),'`, dbCharsetSDest, `','`, dbCharsetSFrom, `'),0)`), stringutil.StringBuilder(`IFNULL(CONVERT(`, columnNameT, ` USING '`, dbCharsetTDest, `'),0)`), nil
		} else {
			return stringutil.StringBuilder(`NVL(UPPER(DBMS_CRYPTO.HASH(CONVERT(TO_CLOB("`, columnNameS, `"),'`, dbCharsetSDest, `','`, dbCharsetSFrom, `'),2)),0)`), stringutil.StringBuilder(`IFNULL(UPPER(MD5(CONVERT(`, columnNameT, ` USING '`, dbCharsetTDest, `'))),0)`), nil
		}
	case constant.BuildInOracleDatatypeNcharVarying, constant.BuildInOracleDatatypeNchar, constant.BuildInOracleDatatypeNvarchar2:
		// in order to save character length, cut more than varchar2. For character bytes smaller than 32, do not use HASH or destroy them in advance.
		if dataLengthS <= int64(constant.DataCompareMethodCheckMD5ValueLength) {
			return stringutil.StringBuilder(`NVL(CONVERT(TO_CLOB("`, columnNameS, `"),'`, dbCharsetSDest, `','`, dbCharsetSFrom, `'),0)`), stringutil.StringBuilder(`IFNULL(CONVERT(`, columnNameT, ` USING '`, dbCharsetTDest, `'),0)`), nil
		} else {
			return stringutil.StringBuilder(`NVL(UPPER(DBMS_CRYPTO.HASH(CONVERT(TO_CLOB("`, columnNameS, `"),'`, dbCharsetSDest, `','`, dbCharsetSFrom, `'),2)),0)`), stringutil.StringBuilder(`IFNULL(UPPER(MD5(CONVERT(`, columnNameT, ` USING '`, dbCharsetTDest, `'))),0)`), nil
		}
	case constant.BuildInOracleDatatypeClob:
		return stringutil.StringBuilder(`NVL(UPPER(DBMS_CRYPTO.HASH(CONVERT("`, columnNameS, `",'`, dbCharsetSDest, `','`, dbCharsetSFrom, `'),2)),0)`), stringutil.StringBuilder(`IFNULL(UPPER(MD5(CONVERT(`, columnNameT, ` USING '`, dbCharsetTDest, `'))),0)`), nil
	case constant.BuildInOracleDatatypeNclob:
		return stringutil.StringBuilder(`NVL(UPPER(DBMS_CRYPTO.HASH(CONVERT(TO_CLOB("`, columnNameS, `"),'`, dbCharsetSDest, `','`, dbCharsetSFrom, `'),2)),0)`), stringutil.StringBuilder(`IFNULL(UPPER(MD5(CONVERT(`, columnNameT, ` USING '`, dbCharsetTDest, `'))),0)`), nil
	case constant.BuildInOracleDatatypeRowid, constant.BuildInOracleDatatypeUrowid:
		return stringutil.StringBuilder(`NVL(ROWIDTOCHAR("`, columnNameS, `"), 0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
	// xmltype datatype
	case constant.BuildInOracleDatatypeXmltype:
		return stringutil.StringBuilder(`NVL(UPPER((DBMS_CRYPTO.HASH(CONVERT(XMLSERIALIZE(CONTENT "`, columnNameS, `" AS CLOB),'`, dbCharsetSDest, `','`, dbCharsetSFrom, `'),2))), 0)`), stringutil.StringBuilder(`IFNULL(UPPER(MD5(CONVERT(`, columnNameT, ` USING '`, dbCharsetTDest, `'))),0)`), nil
	// binary datatype
	case constant.BuildInOracleDatatypeBlob, constant.BuildInOracleDatatypeRaw:
		return stringutil.StringBuilder(`NVL(UPPER(DBMS_CRYPTO.HASH("`, columnNameS, `",2)), 0)`), stringutil.StringBuilder(`IFNULL(UPPER(MD5(`, columnNameT, `)),0)`), nil
	case constant.BuildInOracleDatatypeLong, constant.BuildInOracleDatatypeLongRAW, constant.BuildInOracleDatatypeBfile:
		// can't appear, if the column datatype appear long or long raw, it will be disabled checksum
		return stringutil.StringBuilder(`NVL("`, columnNameS, `",0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
	// oracle bfile datatype -> mysql compatible database varchar, so if the data compare meet bfile, it will be disabled checksum
	//case constant.BuildInOracleDatatypeBfile:
	//	return stringutil.StringBuilder(`NVL(UPPER(DBMS_CRYPTO.HASH(TO_CLOB("`, columnNameS, `"),2)), 0)`), stringutil.StringBuilder(`IFNULL(UPPER(MD5(CONVERT(`, columnNameT, ` USING '`, dbCharsetTDest, `'))),0)`), nil
	// time datatype
	case constant.BuildInOracleDatatypeDate:
		return stringutil.StringBuilder(`NVL(TO_CHAR("`, columnNameS, `",'YYYY-MM-DD HH24:MI:SS'), 0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
	// other datatype
	default:
		if strings.Contains(datatypeS, "INTERVAL") {
			return stringutil.StringBuilder(`NVL(TO_CHAR("`, columnNameS, `"),0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
		} else if strings.Contains(datatypeS, "TIMESTAMP") {
			if dataScaleV == 0 {
				return stringutil.StringBuilder(`NVL(TO_CHAR("`, columnNameS, `",'YYYY-MM-DD HH24:MI:SS'), 0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
			} else if dataScaleV > 0 && dataScaleV <= 6 {
				return stringutil.StringBuilder(`NVL(TO_CHAR("`, columnNameS, `",'YYYY-MM-DD HH24:MI:SS.FF`, strconv.Itoa(dataScaleV), `'),0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
			} else {
				return stringutil.StringBuilder(`NVL(TO_CHAR("`, columnNameS, `",'YYYY-MM-DD HH24:MI:SS.FF6'),0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
			}
		} else {
			return stringutil.StringBuilder(`NVL("`, columnNameS, `",0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
		}
	}
}
