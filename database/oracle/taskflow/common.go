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
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/utils/structure"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
)

func inspectMigrateTask(taskName, taskFlow, taskMode string, databaseS database.IDatabase, connectDBCharsetS, connectDBCharsetT string) (string, string, bool, error) {
	if strings.EqualFold(taskMode, constant.TaskModeStructMigrate) {
		dbCharsetMapping := constant.MigrateTableStructureDatabaseCharsetMap[taskFlow][stringutil.StringUpper(connectDBCharsetS)]
		if !strings.EqualFold(connectDBCharsetT, dbCharsetMapping) {
			return "", "", false, fmt.Errorf("oracle current task [%s] taskflow [%s] charset [%s] mapping [%v] isn't equal with database connect charset [%s], please adjust database connect charset", taskName, taskFlow, connectDBCharsetS, dbCharsetMapping, connectDBCharsetT)
		}
	}

	version, err := databaseS.GetDatabaseVersion()
	if err != nil {
		return version, "", false, err
	}

	var nlsComp string
	oracleCollation := false
	if stringutil.VersionOrdinal(version) >= stringutil.VersionOrdinal(constant.OracleDatabaseTableAndColumnSupportVersion) {
		oracleCollation = true
	} else {
		nlsComp, err = databaseS.GetDatabaseCollation()
		if err != nil {
			return version, "", false, err
		}
	}

	// whether the oracle version can specify table and field collation，if the oracle database version is 12.2 and the above version, it's specify table and field collation, otherwise can't specify
	// oracle database nls_sort/nls_comp value need to be equal, USING_NLS_COMP value is nls_comp
	dbCharsetS, err := databaseS.GetDatabaseCharset()
	if err != nil {
		return version, "", oracleCollation, err
	}
	if !strings.EqualFold(connectDBCharsetS, dbCharsetS) {
		zap.L().Warn("oracle charset and oracle config charset",
			zap.String("oracle database charset", dbCharsetS),
			zap.String("oracle config charset", connectDBCharsetS))
		return version, "", oracleCollation, fmt.Errorf("oracle database charset [%v] and oracle config charset [%v] aren't equal, please adjust oracle config charset", dbCharsetS, connectDBCharsetS)
	}
	if _, ok := constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(connectDBCharsetS)]; !ok {
		return version, "", oracleCollation, fmt.Errorf("oracle database charset [%v] isn't support, only support charset [%v]", dbCharsetS, stringutil.StringPairKey(constant.MigrateOracleCharsetStringConvertMapping))
	}
	if !stringutil.IsContainedString(constant.MigrateDataSupportCharset, stringutil.StringUpper(connectDBCharsetT)) {
		return version, "", oracleCollation, fmt.Errorf("mysql compatible database current config charset [%v] isn't support, support charset [%v]", connectDBCharsetT, stringutil.StringJoin(constant.MigrateDataSupportCharset, ","))
	}

	if strings.EqualFold(taskMode, constant.TaskModeStructMigrate) {
		if _, ok := constant.MigrateTableStructureDatabaseCollationMap[taskFlow][stringutil.StringUpper(nlsComp)]; !ok {
			return version, "", oracleCollation, fmt.Errorf("oracle database collation nlssort [%v] isn't support", nlsComp)
		}

		if _, ok := constant.MigrateTableStructureDatabaseCollationMap[taskFlow][stringutil.StringUpper(nlsComp)]; !ok {
			return version, "", oracleCollation, fmt.Errorf("oracle database collation nlscomp [%v] isn't support", nlsComp)
		}

		return version, nlsComp, oracleCollation, nil
	}

	return version, "", oracleCollation, nil
}

func getDatabaseTableColumnBucket(ctx context.Context,
	databaseS, databaseT database.IDatabase, taskName, taskFlow, schemaNameS, schemaNameT, tableNameS, tableNameT string, collationS bool, columnNameSlis []string, connCharsetS, connCharsetT string) ([]*structure.Range, error) {
	var (
		bucketRanges   []*structure.Range
		randomValueSli [][]string
		newColumnNameS []string
	)

	columnAttriS := make(map[string]map[string]string)
	columnAttriT := make(map[string]map[string]string)
	columnRouteS := make(map[string]string)
	columnRouteNewS := make(map[string]string)
	columnCollationS := make(map[string]string)

	routeRules, err := model.GetIMigrateColumnRouteRW().FindColumnRouteRule(ctx, &rule.ColumnRouteRule{
		TaskName:    taskName,
		SchemaNameS: schemaNameS,
		TableNameS:  tableNameS,
	})
	if err != nil {
		return bucketRanges, err
	}
	for _, r := range routeRules {
		columnRouteS[r.ColumnNameS] = r.ColumnNameT
	}

	for i, column := range columnNameSlis {
		var columnT string
		convertUtf8Raws, err := stringutil.CharsetConvert([]byte(column), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(connCharsetS)], constant.CharsetUTF8MB4)
		if err != nil {
			return bucketRanges, err
		}
		columnUtf8S := stringutil.BytesToString(convertUtf8Raws)
		if val, ok := columnRouteS[columnUtf8S]; ok {
			columnT = val
		} else {
			columnT = columnUtf8S
		}
		attriS, err := databaseS.GetDatabaseTableColumnAttribute(schemaNameS, tableNameS, column, collationS)
		if err != nil {
			return bucketRanges, err
		}

		var attriT []map[string]string
		if databaseT != nil {
			attriT, err = databaseT.GetDatabaseTableColumnAttribute(schemaNameT, tableNameT, columnT, collationS)
			if err != nil {
				return bucketRanges, err
			}
		}

		columnBucketS, err := databaseS.GetDatabaseTableColumnBucket(schemaNameS, tableNameS, column, attriS[0]["DATA_TYPE"])
		if err != nil {
			return bucketRanges, err
		}
		// first elems
		if i == 0 && len(columnBucketS) == 0 {
			break
		} else if i > 0 && len(columnBucketS) == 0 {
			continue
		} else {
			columnNameUtf8Raw, err := stringutil.CharsetConvert([]byte(column), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(connCharsetS)], constant.CharsetUTF8MB4)
			if err != nil {
				return bucketRanges, fmt.Errorf("[InitDataCompareTask] oracle schema [%s] table [%s] column [%s] charset convert [UTFMB4] failed, error: %v", schemaNameS, tableNameS, column, err)
			}

			columnNameS := stringutil.BytesToString(columnNameUtf8Raw)
			randomValueSli = append(randomValueSli, columnBucketS)
			newColumnNameS = append(newColumnNameS, columnNameS)
			columnAttriS[columnNameS] = attriS[0]
			if len(attriT) > 0 && databaseT != nil {
				columnAttriT[columnNameS] = map[string]string{
					columnT: attriT[0]["DATA_TYPE"],
				}
			} else {
				columnAttriT[columnNameS] = attriS[0]
			}
			columnRouteNewS[columnNameS] = columnT
			columnCollationS[columnNameS] = attriS[0]["COLLATION"]
		}
	}

	if len(randomValueSli) > 0 {
		randomValues, randomValuesLen := stringutil.StringSliceAlignLen(randomValueSli)
		for i := 0; i <= randomValuesLen; i++ {
			newChunk := structure.NewRange()

			for j, columnS := range newColumnNameS {
				if i == 0 {
					if len(randomValues[j]) == 0 {
						break
					}
					err = newChunk.Update(taskFlow,
						stringutil.StringUpper(connCharsetS),
						stringutil.StringUpper(connCharsetT),
						columnS,
						columnRouteNewS[columnS],
						columnCollationS[columnS],
						columnAttriS[columnS],
						columnAttriT[columnS],
						"", randomValues[j][i], false, true)
					if err != nil {
						return bucketRanges, err
					}
				} else if i == len(randomValues[0]) {
					err = newChunk.Update(taskFlow,
						stringutil.StringUpper(connCharsetS),
						stringutil.StringUpper(connCharsetT),
						columnS,
						columnRouteNewS[columnS],
						columnCollationS[columnS],
						columnAttriS[columnS],
						columnAttriT[columnS], randomValues[j][i-1], "", true, false)
					if err != nil {
						return bucketRanges, err
					}
				} else {
					err = newChunk.Update(taskFlow,
						stringutil.StringUpper(connCharsetS),
						stringutil.StringUpper(connCharsetT),
						columnS,
						columnRouteNewS[columnS],
						columnCollationS[columnS],
						columnAttriS[columnS],
						columnAttriT[columnS], randomValues[j][i-1], randomValues[j][i], true, true)
					if err != nil {
						return bucketRanges, err
					}
				}
			}
			bucketRanges = append(bucketRanges, newChunk)
		}
	}
	return bucketRanges, nil
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
