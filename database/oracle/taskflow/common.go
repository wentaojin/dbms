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
	case "NUMBER":
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	case "DECIMAL", "DEC", "DOUBLE PRECISION", "FLOAT", "INTEGER", "INT", "REAL", "NUMERIC", "BINARY_FLOAT", "BINARY_DOUBLE", "SMALLINT":
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	// character datatype
	case "BFILE", "CHARACTER", "LONG", "NCHAR VARYING", "ROWID", "UROWID", "VARCHAR", "CHAR", "NCHAR", "VARCHAR2", "NVARCHAR2", "NCLOB", "CLOB":
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	// xmltype datatype
	case "XMLTYPE":
		return fmt.Sprintf(` XMLSERIALIZE(CONTENT "%s" AS CLOB) AS "%s"`, columnName, columnName), nil
	// binary datatype
	case "BLOB", "LONG RAW", "RAW":
		return stringutil.StringBuilder(`"`, columnName, `"`), nil
	// time datatype
	case "DATE":
		return stringutil.StringBuilder(`TO_CHAR("`, columnName, `",'yyyy-MM-dd HH24:mi:ss') AS "`, columnName, `"`), nil
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
				return stringutil.StringBuilder(`TO_CHAR("`, columnName, `",'yyyy-MM-dd HH24:mi:ss') AS "`, columnName, `"`), nil
			} else if dataScaleV < 0 && dataScaleV <= 6 {
				return stringutil.StringBuilder(`TO_CHAR("`, columnName,
					`",'yyyy-mm-dd hh24:mi:ss.ff`, dataScale, `') AS "`, columnName, `"`), nil
			} else {
				return stringutil.StringBuilder(`TO_CHAR("`, columnName, `",'yyyy-mm-dd hh24:mi:ss.ff6') AS "`, columnName, `"`), nil
			}
		} else {
			return stringutil.StringBuilder(`"`, columnName, `"`), nil
		}
	}
}

func optimizerDataCompareColumnST(columnNameS, datatypeS, dataScaleS, columnNameT string) (string, string, error) {
	switch strings.ToUpper(datatypeS) {
	// numeric type
	case "NUMBER":
		return stringutil.StringBuilder(`NVL("`, columnNameS, `",0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
	case "DECIMAL", "DEC", "DOUBLE PRECISION", "FLOAT", "INTEGER", "INT", "REAL", "NUMERIC", "BINARY_FLOAT", "BINARY_DOUBLE", "SMALLINT":
		return stringutil.StringBuilder(`NVL("`, columnNameS, `",0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
	// character datatype
	case "CHARACTER", "NCHAR VARYING", "VARCHAR", "CHAR", "NCHAR", "VARCHAR2", "NVARCHAR2":
		return stringutil.StringBuilder(`NVL("`, columnNameS, `",0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
	case "LONG":
		return stringutil.StringBuilder(`NVL(UPPER((DBMS_CRYPTO.HASH(TO_LOB("`, columnNameS, `"),2 /*DBMS_CRYPTO.HASH_MD5*/))), 0)`),
			stringutil.StringBuilder(`IFNULL(UPPER(MD5(`, columnNameT, `)),0)`), nil
	case "ROWID", "UROWID":
		return stringutil.StringBuilder(`NVL(ROWIDTOCHAR("`, columnNameS, `"), 0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
		// xmltype datatype
	case "XMLTYPE":
		return stringutil.StringBuilder(`NVL(UPPER((DBMS_CRYPTO.HASH(XMLSERIALIZE(CONTENT "`, columnNameS, `" AS CLOB),2 /*DBMS_CRYPTO.HASH_MD5*/))), 0)`), stringutil.StringBuilder(`IFNULL(UPPER(MD5(`, columnNameT, `)),0)`), nil
	// binary datatype
	case "BLOB", "LONG RAW", "RAW", "BFILE", "NCLOB", "CLOB":
		return stringutil.StringBuilder(`NVL(UPPER(DBMS_CRYPTO.HASH("`, columnNameS, `",2 /*DBMS_CRYPTO.HASH_MD5*/)), 0)`),
			stringutil.StringBuilder(`IFNULL(UPPER(MD5(`, columnNameT, `)),0)`), nil
	// time datatype
	case "DATE":
		return stringutil.StringBuilder(`NVL(TO_CHAR("`, columnNameS, `",'YYYY-MM-DD HH24:MI:SS'), 0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
	// other datatype
	default:
		if strings.Contains(datatypeS, "INTERVAL") {
			return stringutil.StringBuilder(`NVL(TO_CHAR("`, columnNameS, `"),0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
		} else if strings.Contains(datatypeS, "TIMESTAMP") {
			dataScaleV, err := strconv.Atoi(dataScaleS)
			if err != nil {
				return "", "", fmt.Errorf("aujust oracle timestamp datatype scale [%s] strconv.Atoi failed: %v", dataScaleS, err)
			}
			if dataScaleV == 0 {
				return stringutil.StringBuilder(`NVL(TO_CHAR("`, columnNameS, `",'YYYY-MM-DD HH24:MI:SS'), 0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
			} else if dataScaleV > 0 && dataScaleV <= 6 {
				return stringutil.StringBuilder(`NVL(TO_CHAR("`, columnNameS, `",'YYYY-MM-DD HH24:MI:SS.FF`, dataScaleS, `'),0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
			} else {
				return stringutil.StringBuilder(`NVL(TO_CHAR("`, columnNameS, `",'YYYY-MM-DD HH24:MI:SS.FF6'),0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
			}
		} else {
			return stringutil.StringBuilder(`NVL("`, columnNameS, `",0)`), stringutil.StringBuilder(`IFNULL(`, columnNameT, `,0)`), nil
		}
	}
}
