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

	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func OptimizerMYSQLCompatibleDataMigrateColumnS(columnName, datatype, datetimePrecision string) (string, error) {
	switch strings.ToUpper(datatype) {
	// numeric type
	case constant.BuildInMySQLDatatypeBigint,
		constant.BuildInMySQLDatatypeDecimal,
		constant.BuildInMySQLDatatypeInt,
		constant.BuildInMySQLDatatypeInteger,
		constant.BuildInMySQLDatatypeMediumint,
		constant.BuildInMySQLDatatypeNumeric,
		constant.BuildInMySQLDatatypeSmallint,
		constant.BuildInMySQLDatatypeTinyint,
		constant.BuildInMySQLDatatypeDouble,
		constant.BuildInMySQLDatatypeDoublePrecision,
		constant.BuildInMySQLDatatypeFloat,
		constant.BuildInMySQLDatatypeReal:
		return columnName, nil
	// character datatype
	case constant.BuildInMySQLDatatypeChar,
		constant.BuildInMySQLDatatypeLongText,
		constant.BuildInMySQLDatatypeMediumText,
		constant.BuildInMySQLDatatypeText,
		constant.BuildInMySQLDatatypeTinyText,
		constant.BuildInMySQLDatatypeVarchar:
		return columnName, nil
	// binary datatype
	case constant.BuildInMySQLDatatypeBinary,
		constant.BuildInMySQLDatatypeVarbinary,
		constant.BuildInMySQLDatatypeBlob,
		constant.BuildInMySQLDatatypeLongBlob,
		constant.BuildInMySQLDatatypeMediumBlob,
		constant.BuildInMySQLDatatypeTinyBlob:
		return columnName, nil
	// time datatype
	case constant.BuildInMySQLDatatypeDate,
		constant.BuildInMySQLDatatypeTime,
		constant.BuildInMySQLDatatypeYear:
		return columnName, nil
	case constant.BuildInMySQLDatatypeDatetime,
		constant.BuildInMySQLDatatypeTimestamp:
		datetimeP, err := strconv.Atoi(datetimePrecision)
		if err != nil {
			return "", fmt.Errorf("aujust mysql compatible timestamp datatype scale [%s] strconv.Atoi failed: %v", datetimePrecision, err)
		}
		if datetimeP == 0 {
			return fmt.Sprintf("IFNULL(DATE_FORMAT(%s, '%%Y-%%m-%%d %%H:%%i:%%s'),'0') AS %s", columnName, columnName), nil
		} else {
			return fmt.Sprintf("IFNULL(CONCAT(DATE_FORMAT(%s, '%%Y-%%m-%%d %%T.'),LPAD(SUBSTRING(TIME_FORMAT(%s, '%%f'), 1, %s), %s, '0')),'0') AS %s", columnName, columnName, datetimePrecision, datetimePrecision, columnName), nil
		}
	case constant.BuildInMySQLDatatypeBit:
		return columnName, nil
	// ORACLE ISN'T SUPPORT
	case constant.BuildInMySQLDatatypeSet,
		constant.BuildInMySQLDatatypeEnum:
		return columnName, nil
	// other datatype
	default:
		return columnName, nil
	}
}

func OptimizerMYSQLCompatibleMigrateOracleDataCompareColumnST(columnNameS, datatypeS string, datetimePrecisionS, dataLengthS int64, dataPrecisionS, dataScaleS, dbCharsetSDest, columnNameT, dbCharsetTFrom, dbCharsetTDest string) (string, string, error) {
	dataPrecisionV, err := strconv.Atoi(dataPrecisionS)
	if err != nil {
		return "", "", fmt.Errorf("aujust mysql compatible database table column [%s] datatype precision [%s] strconv.Atoi failed: %v", columnNameS, dataPrecisionS, err)
	}
	dataScaleV, err := strconv.Atoi(dataScaleS)
	if err != nil {
		return "", "", fmt.Errorf("aujust mysql compatible database table column [%s] scale [%s] strconv.Atoi failed: %v", columnNameS, dataScaleS, err)
	}
	nvlNullDecimalS := stringutil.StringBuilder("IFNULL(`", columnNameS, "`,0)")
	nvlNullDecimalT := stringutil.StringBuilder(`NVL(`, columnNameT, `,0)`)

	nvlNullStringS := stringutil.StringBuilder("IFNULL(`", columnNameS, "`,'0')")
	nvlNullStringT := stringutil.StringBuilder(`NVL(`, columnNameT, `,'0')`)

	switch strings.ToUpper(datatypeS) {
	// numeric type
	case constant.BuildInMySQLDatatypeDecimal, constant.BuildInMySQLDatatypeNumeric:
		if dataScaleV == 0 {
			return nvlNullDecimalS, nvlNullDecimalT, nil
		} else if dataScaleV > 0 {
			// max decimal(65,30)
			// decimal(65,30) -> number, oracle database to_char max 62 digits , translate to_char(35 position,27 position) -> decimal(65,30)
			toCharMax := 62
			if dataPrecisionV > toCharMax {
				return stringutil.StringBuilder(`CAST(`, nvlNullDecimalS, ` AS DECIMAL(62,27))`), stringutil.StringBuilder(`TO_CHAR(`, nvlNullDecimalT, `,'FM`, stringutil.PaddingString(35, "9", "0"), `.`, stringutil.PaddingString(27, "9", "0"), `')`), nil
			} else {
				toCharPaddingInteger := dataPrecisionV - dataScaleV
				return stringutil.StringBuilder(`CAST(`, nvlNullDecimalS, ` AS DECIMAL(`, strconv.Itoa(dataPrecisionV), `,`, strconv.Itoa(dataScaleV), `))`), stringutil.StringBuilder(`TO_CHAR(`, nvlNullDecimalT, `,'FM`, stringutil.PaddingString(toCharPaddingInteger, "9", "0"), `.`, stringutil.PaddingString(dataScaleV, "9", "0"), `')`), nil
			}
		} else {
			return "", "", fmt.Errorf("the mysql compatible database table column [%s] datatype [%s] data_scale value [%d] cannot less zero, please contact author or reselect", columnNameS, datatypeS, dataScaleV)
		}
	case constant.BuildInMySQLDatatypeBigint,
		constant.BuildInMySQLDatatypeInt,
		constant.BuildInMySQLDatatypeInteger,
		constant.BuildInMySQLDatatypeMediumint,
		constant.BuildInMySQLDatatypeSmallint,
		constant.BuildInMySQLDatatypeTinyint:
		return nvlNullDecimalS, nvlNullDecimalT, nil

	case constant.BuildInMySQLDatatypeDouble,
		constant.BuildInMySQLDatatypeDoublePrecision:
		return stringutil.StringBuilder(`RPAD(CAST(`, nvlNullDecimalS, ` AS DECIMAL(65,15)),16,0)`), stringutil.StringBuilder(`RPAD(TO_CHAR(`, nvlNullDecimalT, `,'FM99999999999999999999999999999999999990.999999999999990'),16,0)`), nil

	case constant.BuildInMySQLDatatypeFloat,
		constant.BuildInMySQLDatatypeReal:
		return stringutil.StringBuilder(`RPAD(CAST(`, nvlNullDecimalS, ` AS DECIMAL(65,7)),8,0)`), stringutil.StringBuilder(`RPAD(TO_CHAR(`, nvlNullDecimalT, `,'FM99999999999999999999999999999999999990.9999990'),8,0)`), nil

	// character datatype
	case constant.BuildInMySQLDatatypeChar, constant.BuildInMySQLDatatypeVarchar:
		// in order to save character length, cut more than varchar2. For character bytes smaller than 32, do not use HASH or destroy them in advance.
		//if dataLengthS <= int64(constant.DataCompareMethodCheckMD5ValueLength) {
		//return stringutil.StringBuilder(`CONVERT(`, nvlNullStringS, ` USING '`, dbCharsetSDest, `')`), stringutil.StringBuilder(`CONVERT(TO_CLOB(`, nvlNullStringT, `),'`, dbCharsetTDest, `','`, dbCharsetTFrom, `')`), nil
		//} else {
		//	return stringutil.StringBuilder(`UPPER(MD5(CONVERT(`, nvlNullStringS, ` USING '`, dbCharsetSDest, `')))`), stringutil.StringBuilder(`UPPER(DBMS_CRYPTO.HASH(CONVERT(TO_CLOB(`, nvlNullStringT, `),'`, dbCharsetTDest, `','`, dbCharsetTFrom, `'),2))`), nil
		//}
		return nvlNullStringS, nvlNullStringT, nil
	case constant.BuildInMySQLDatatypeLongText,
		constant.BuildInMySQLDatatypeMediumText,
		constant.BuildInMySQLDatatypeText,
		constant.BuildInMySQLDatatypeTinyText:
		return stringutil.StringBuilder(`UPPER(MD5(CONVERT(`, nvlNullStringS, ` USING '`, dbCharsetSDest, `')))`), stringutil.StringBuilder(`UPPER(DBMS_CRYPTO.HASH(CONVERT(TO_CLOB(`, nvlNullStringT, `),'`, dbCharsetTDest, `','`, dbCharsetTFrom, `'),2))`), nil
	// binary datatype
	case constant.BuildInMySQLDatatypeBinary,
		constant.BuildInMySQLDatatypeVarbinary,
		constant.BuildInMySQLDatatypeBlob,
		constant.BuildInMySQLDatatypeLongBlob,
		constant.BuildInMySQLDatatypeMediumBlob,
		constant.BuildInMySQLDatatypeTinyBlob:
		return stringutil.StringBuilder(`UPPER(MD5(`, nvlNullStringS, `))`), stringutil.StringBuilder(`UPPER(DBMS_CRYPTO.HASH(TO_BLOB(`, nvlNullStringT, `),2))`), nil

	// time datatype
	case constant.BuildInMySQLDatatypeDate:
		return nvlNullStringS, stringutil.StringBuilder(`NVL(TO_CHAR("`, columnNameT, `",'YYYY-MM-DD'),'0')`), nil
	case constant.BuildInMySQLDatatypeTime:
		return nvlNullStringS, stringutil.StringBuilder(`NVL(TO_CHAR("`, columnNameT, `",'HH24:MI:SS'),'0')`), nil
	case constant.BuildInMySQLDatatypeYear:
		return nvlNullStringS, stringutil.StringBuilder(`NVL(TO_CHAR("`, columnNameT, `",'YYYY'),'0')`), nil
	case constant.BuildInMySQLDatatypeDatetime,
		constant.BuildInMySQLDatatypeTimestamp:
		if datetimePrecisionS == 0 {
			return nvlNullStringS, columnNameT, nil
		} else {
			return nvlNullStringS, columnNameT, nil
		}
	case constant.BuildInMySQLDatatypeBit:
		return nvlNullStringS, nvlNullStringT, nil
	// ORACLE ISN'T SUPPORT
	case constant.BuildInMySQLDatatypeSet,
		constant.BuildInMySQLDatatypeEnum:
		return nvlNullStringS, nvlNullStringT, nil
	// other datatype
	default:
		return nvlNullStringS, nvlNullStringT, nil
	}
}

func OptimizerMYSQLCompatibleMigratePostgresDataCompareColumnST(columnNameS, datatypeS string, datatypeIsBooleanT bool, datetimePrecisionS int64, dataPrecisionS, dataScaleS, dbCharsetSDest, columnNameT, dbCharsetTDest string) (string, string, error) {
	dataPrecisionV, err := strconv.Atoi(dataPrecisionS)
	if err != nil {
		return "", "", fmt.Errorf("aujust mysql compatible database table column [%s] datatype precision [%s] strconv.Atoi failed: %v", columnNameS, dataPrecisionS, err)
	}
	dataScaleV, err := strconv.Atoi(dataScaleS)
	if err != nil {
		return "", "", fmt.Errorf("aujust mysql compatible database table column [%s] scale [%s] strconv.Atoi failed: %v", columnNameS, dataScaleS, err)
	}
	nvlNullDecimalS := stringutil.StringBuilder("IFNULL(`", columnNameS, "`,0)")
	nvlNullDecimalT := stringutil.StringBuilder(`COALESCE(`, columnNameT, `,0)`)

	nvlNullStringS := stringutil.StringBuilder("IFNULL(`", columnNameS, "`,'0')")
	nvlNullStringT := stringutil.StringBuilder(`COALESCE(`, columnNameT, `,'0')`)

	switch strings.ToUpper(datatypeS) {
	// numeric type
	case constant.BuildInMySQLDatatypeDecimal, constant.BuildInMySQLDatatypeNumeric:
		if dataScaleV == 0 {
			return nvlNullDecimalS, nvlNullDecimalT, nil
		} else if dataScaleV > 0 {
			// max decimal(65,30)
			// decimal(65,30) -> number, acorrding to oracle database to_char max 62 digits , translate to_char(35 position,27 position) -> decimal(65,30)
			toCharMax := 62
			if dataPrecisionV > toCharMax {
				return stringutil.StringBuilder(`CAST(`, nvlNullDecimalS, ` AS DECIMAL(62,27))`), stringutil.StringBuilder(`TO_CHAR(`, nvlNullDecimalT, `,'FM`, stringutil.PaddingString(35, "9", "0"), `.`, stringutil.PaddingString(27, "9", "0"), `')`), nil
			} else {
				toCharPaddingInteger := dataPrecisionV - dataScaleV
				return stringutil.StringBuilder(`CAST(`, nvlNullDecimalS, ` AS DECIMAL(`, strconv.Itoa(dataPrecisionV), `,`, strconv.Itoa(dataScaleV), `))`), stringutil.StringBuilder(`TO_CHAR(`, nvlNullDecimalT, `,'FM`, stringutil.PaddingString(toCharPaddingInteger, "9", "0"), `.`, stringutil.PaddingString(dataScaleV, "9", "0"), `')`), nil
			}
		} else {
			return "", "", fmt.Errorf("the mysql compatible database table column [%s] datatype [%s] data_scale value [%d] cannot less zero, please contact author or reselect", columnNameS, datatypeS, dataScaleV)
		}
	case constant.BuildInMySQLDatatypeBigint,
		constant.BuildInMySQLDatatypeInt,
		constant.BuildInMySQLDatatypeInteger,
		constant.BuildInMySQLDatatypeMediumint,
		constant.BuildInMySQLDatatypeSmallint:
		return nvlNullDecimalS, nvlNullDecimalT, nil
	case constant.BuildInMySQLDatatypeTinyint:
		if datatypeIsBooleanT {
			return stringutil.StringBuilder("CASE IFNULL(`", columnNameS, "`,0) WHEN 1 THEN 1 WHEN 0 THEN 0 ELSE `", columnNameS, "` END AS `", columnNameS, "`"),
				stringutil.StringBuilder(`CASE COALESCE(`, columnNameT, `,false) WHEN true THEN 1 WHEN false THEN 0 ELSE -100001 END AS `, columnNameT), nil
		} else {
			return nvlNullDecimalS, nvlNullDecimalT, nil
		}
	case constant.BuildInMySQLDatatypeDouble,
		constant.BuildInMySQLDatatypeDoublePrecision:
		return stringutil.StringBuilder(`RPAD(CAST(`, nvlNullDecimalS, ` AS DECIMAL(65,15)),16,0)`), stringutil.StringBuilder(`RPAD(TO_CHAR(`, nvlNullDecimalT, `,'FM99999999999999999999999999999999999990.999999999999990'),16,0)`), nil

	case constant.BuildInMySQLDatatypeFloat,
		constant.BuildInMySQLDatatypeReal:
		return stringutil.StringBuilder(`RPAD(CAST(`, nvlNullDecimalS, ` AS DECIMAL(65,7)),8,0)`), stringutil.StringBuilder(`RPAD(TO_CHAR(`, nvlNullDecimalT, `,'FM99999999999999999999999999999999999990.9999990'),8,0)`), nil

	// character datatype
	case constant.BuildInMySQLDatatypeChar, constant.BuildInMySQLDatatypeVarchar:
		// in order to save character length, cut more than varchar2. For character bytes smaller than 32, do not use HASH or destroy them in advance.
		//if dataLengthS <= int64(constant.DataCompareMethodCheckMD5ValueLength) {
		//return stringutil.StringBuilder(`CONVERT(`, nvlNullStringS, ` USING '`, dbCharsetSDest, `')`), stringutil.StringBuilder(`CONVERT(TO_CLOB(`, nvlNullStringT, `),'`, dbCharsetTDest, `','`, dbCharsetTFrom, `')`), nil
		//} else {
		//	return stringutil.StringBuilder(`UPPER(MD5(CONVERT(`, nvlNullStringS, ` USING '`, dbCharsetSDest, `')))`), stringutil.StringBuilder(`UPPER(DBMS_CRYPTO.HASH(CONVERT(TO_CLOB(`, nvlNullStringT, `),'`, dbCharsetTDest, `','`, dbCharsetTFrom, `'),2))`), nil
		//}
		return nvlNullStringS, nvlNullStringT, nil
	case constant.BuildInMySQLDatatypeLongText,
		constant.BuildInMySQLDatatypeMediumText,
		constant.BuildInMySQLDatatypeText,
		constant.BuildInMySQLDatatypeTinyText:
		return stringutil.StringBuilder(`MD5(CONVERT(`, nvlNullStringS, ` USING '`, dbCharsetSDest, `'))`), stringutil.StringBuilder(`MD5(CONVERT_TO(`, nvlNullStringT, `,'`, dbCharsetTDest, `'))`), nil
	// binary datatype
	case constant.BuildInMySQLDatatypeBinary,
		constant.BuildInMySQLDatatypeVarbinary,
		constant.BuildInMySQLDatatypeBlob,
		constant.BuildInMySQLDatatypeLongBlob,
		constant.BuildInMySQLDatatypeMediumBlob,
		constant.BuildInMySQLDatatypeTinyBlob:
		return stringutil.StringBuilder(`MD5(`, nvlNullStringS, `)`), stringutil.StringBuilder(`MD5(`, nvlNullStringT, `)`), nil

	// time datatype
	case constant.BuildInMySQLDatatypeDate:
		return nvlNullStringS, stringutil.StringBuilder(`COALESCE(TO_CHAR(`, columnNameT, `,'YYYY-MM-DD'),'0')`), nil
	case constant.BuildInMySQLDatatypeTime:
		return nvlNullStringS, stringutil.StringBuilder(`COALESCE(TO_CHAR(`, columnNameT, `,'HH24:MI:SS'),'0')`), nil
	case constant.BuildInMySQLDatatypeYear:
		return nvlNullStringS, stringutil.StringBuilder(`COALESCE(TO_CHAR(`, columnNameT, `,'YYYY'),'0')`), nil
	case constant.BuildInMySQLDatatypeDatetime,
		constant.BuildInMySQLDatatypeTimestamp:
		if datetimePrecisionS == 0 {
			return nvlNullStringS, columnNameT, nil
		} else {
			return stringutil.StringBuilder("DATE_FORMAT(`", columnNameS, "`,'%%Y-%%m-%%d %%H:%%i:%%s.%%f') AS `", columnNameS, "`"), stringutil.StringBuilder(`TO_CHAR(`, columnNameT, `,'yyyy-mm-dd hh24:mi:ss.us') AS `, columnNameT), nil
		}
	case constant.BuildInMySQLDatatypeBit:
		return nvlNullStringS, nvlNullStringT, nil
	// ORACLE ISN'T SUPPORT
	case constant.BuildInMySQLDatatypeSet,
		constant.BuildInMySQLDatatypeEnum:
		return nvlNullStringS, nvlNullStringT, nil
	// other datatype
	default:
		return nvlNullStringS, nvlNullStringT, nil
	}
}
