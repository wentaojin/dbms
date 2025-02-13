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
	"context"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"
	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"golang.org/x/sync/errgroup"
)

// DataCompareSeek is a struct that encapsulates the logic for computing and comparing checksums.
type DataCompareSeek struct {
	taskName          string
	taskFlow          string
	dbTypeS           string
	dbTypeT           string
	dbCharsetS        string
	dbCharsetT        string
	databaseS         database.IDatabase
	databaseT         database.IDatabase
	columnDetailS     []string
	columnDetailT     []string
	disableCompareMd5 bool
	results           chan [2]string
	errC              chan error
}

// NewDataCompareSeek creates a new DataCompareSeek.
func NewDataCompareSeek(
	taskName string,
	taskFlow string,
	dbTypeS string,
	dbTypeT string,
	dbCharsetS string,
	dbCharsetT string,
	databaseS database.IDatabase, databaseT database.IDatabase,
	columnDetailS []string,
	columnDetailT []string,
	disableCompareMd5 bool) *DataCompareSeek {
	return &DataCompareSeek{
		taskName:          taskName,
		taskFlow:          taskFlow,
		dbTypeS:           dbTypeS,
		dbTypeT:           dbTypeT,
		dbCharsetS:        dbCharsetS,
		dbCharsetT:        dbCharsetT,
		databaseS:         databaseS,
		databaseT:         databaseT,
		columnDetailS:     columnDetailS,
		columnDetailT:     columnDetailT,
		results:           make(chan [2]string, len(columnDetailS)),
		errC:              make(chan error, 1),
		disableCompareMd5: disableCompareMd5,
	}
}

func (d *DataCompareSeek) upstreamChecksum(columnDetailS string, chunkDetailS string, queryCondArgsS []interface{}, dmt *task.DataCompareTask) (decimal.Decimal, error) {
	var execQueryS string
	switch stringutil.StringUpper(d.dbTypeS) {
	case constant.DatabaseTypeOracle:
		if !strings.EqualFold(dmt.SnapshotPointS, "") && strings.EqualFold(dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, dmt.SchemaNameS, `"."`, dmt.TableNameS, `" AS OF SCN `, dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
		} else if !strings.EqualFold(dmt.SnapshotPointS, "") && !strings.EqualFold(dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, dmt.SchemaNameS, `"."`, dmt.TableNameS, `" AS OF SCN `, dmt.SnapshotPointS, ` WHERE `, chunkDetailS)
		} else if strings.EqualFold(dmt.SnapshotPointS, "") && !strings.EqualFold(dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, dmt.SchemaNameS, `"."`, dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, dmt.SchemaNameS, `"."`, dmt.TableNameS, `" WHERE `, chunkDetailS)
		}
		if strings.EqualFold(dmt.CompareMethod, constant.DataCompareMethodDatabaseCheckMD5) {
			execQueryS = fmt.Sprintf(`SELECT
		TO_CHAR(NVL(TO_NUMBER(SUM(TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 1, 8),'xxxxxxxx')+
		TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 9, 8),'xxxxxxxx')+
		TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 17, 8),'xxxxxxxx')+ 
		TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 25, 8),'xxxxxxxx'))),0)) AS ROWSCHECKSUM
	FROM
		(%v) subq`, execQueryS)
		}
	case constant.DatabaseTypeTiDB:
		if !strings.EqualFold(dmt.SnapshotPointS, "") && strings.EqualFold(dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", columnDetailS, " FROM `", dmt.SchemaNameS, "`.`", dmt.TableNameS, "` AS OF TIMESTAMP '", dmt.SnapshotPointS, "' WHERE ", chunkDetailS)
		} else if !strings.EqualFold(dmt.SnapshotPointS, "") && !strings.EqualFold(dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", dmt.SqlHintS, " ", columnDetailS, " FROM `", dmt.SchemaNameS, "`.`", dmt.TableNameS, "` AS OF TIMESTAMP '", dmt.SnapshotPointS, "' WHERE ", chunkDetailS)
		} else if strings.EqualFold(dmt.SnapshotPointS, "") && !strings.EqualFold(dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", dmt.SqlHintS, " ", columnDetailS, " FROM `", dmt.SchemaNameS, "`.`", dmt.TableNameS, "` WHERE ", chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder("SELECT ", columnDetailS, " FROM `", dmt.SchemaNameS, "`.`", dmt.TableNameS, "` WHERE ", chunkDetailS)
		}
		if strings.EqualFold(dmt.CompareMethod, constant.DataCompareMethodDatabaseCheckMD5) {
			execQueryS = fmt.Sprintf(`
	SELECT
		 CAST(IFNULL(SUM(CONV(SUBSTRING(subq.ROWSCHECKSUM, 1, 8),16,10)+ 
		 CONV(SUBSTRING(subq.ROWSCHECKSUM, 9, 8),16,10)+
		 CONV(SUBSTRING(subq.ROWSCHECKSUM, 17, 8),16,10)+
		 CONV(SUBSTRING(subq.ROWSCHECKSUM, 25, 8),16,10)),0) AS CHAR) AS ROWSCHECKSUM
	FROM
		(%v) subq`, execQueryS)
		}
	case constant.DatabaseTypeMySQL:
		if !strings.EqualFold(dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder("SELECT ", dmt.SqlHintS, " ", columnDetailS, " FROM `", dmt.SchemaNameS, "`.`", dmt.TableNameS, "` WHERE ", chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder("SELECT ", columnDetailS, " FROM `", dmt.SchemaNameS, "`.`", dmt.TableNameS, "` WHERE ", chunkDetailS)
		}
		if strings.EqualFold(dmt.CompareMethod, constant.DataCompareMethodDatabaseCheckMD5) {
			execQueryS = fmt.Sprintf(`
	SELECT
		 CAST(IFNULL(SUM(CONV(SUBSTRING(subq.ROWSCHECKSUM, 1, 8),16,10)+ 
		 CONV(SUBSTRING(subq.ROWSCHECKSUM, 9, 8),16,10)+
		 CONV(SUBSTRING(subq.ROWSCHECKSUM, 17, 8),16,10)+
		 CONV(SUBSTRING(subq.ROWSCHECKSUM, 25, 8),16,10)),0) AS CHAR) AS ROWSCHECKSUM
	FROM
		(%v) subq`, execQueryS)
		}
	case constant.DatabaseTypePostgresql:
		if !strings.EqualFold(dmt.SnapshotPointS, "") && strings.EqualFold(dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, dmt.SnapshotPointS, `'`, constant.StringSeparatorSemicolon, `SELECT `, columnDetailS, ` FROM "`, dmt.SchemaNameS, `"."`, dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else if !strings.EqualFold(dmt.SnapshotPointS, "") && !strings.EqualFold(dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, dmt.SnapshotPointS, `'`, constant.StringSeparatorSemicolon, `SELECT `, dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, dmt.SchemaNameS, `"."`, dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else if strings.EqualFold(dmt.SnapshotPointS, "") && !strings.EqualFold(dmt.SqlHintS, "") {
			execQueryS = stringutil.StringBuilder(`SELECT `, dmt.SqlHintS, ` `, columnDetailS, ` FROM "`, dmt.SchemaNameS, `"."`, dmt.TableNameS, `" WHERE `, chunkDetailS)
		} else {
			execQueryS = stringutil.StringBuilder(`SELECT `, columnDetailS, ` FROM "`, dmt.SchemaNameS, `"."`, dmt.TableNameS, `" WHERE `, chunkDetailS)
		}
		if strings.EqualFold(dmt.CompareMethod, constant.DataCompareMethodDatabaseCheckMD5) {
			execQueryS = fmt.Sprintf(`SELECT
		COALESCE(SUM(('x'||SUBSTR(subq."ROWSCHECKSUM", 1, 8))::BIT(32)::BIGINT +
		('x'||SUBSTR(subq."ROWSCHECKSUM", 9, 8))::BIT(32)::BIGINT +
		('x'||SUBSTR(subq."ROWSCHECKSUM", 17, 8))::BIT(32)::BIGINT + 
		('x'||SUBSTR(subq."ROWSCHECKSUM", 25, 8))::BIT(32)::BIGINT),0)::TEXT AS "ROWSCHECKSUM"
	FROM
		(%v) subq`, execQueryS)
		}
	default:
		return decimal.Decimal{}, fmt.Errorf("the task [%s] db_type_s [%s] is not supported, please contact author or reselect", dmt.TaskName, d.dbTypeS)
	}

	_, resultStrS, err := d.databaseS.GetDatabaseTableCompareRow(execQueryS, queryCondArgsS...)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("the database source query sql [%v] args [%v] running failed: [%v]", execQueryS, queryCondArgsS, err)
	}
	resultS, err := decimal.NewFromString(resultStrS[0]["ROWSCHECKSUM"])
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("parse the database source value [%s] failed: %v", resultStrS[0]["ROWSCHECKSUM"], err)
	}
	return resultS, nil
}

func (d *DataCompareSeek) downstreamChecksum(columnDetailT string, chunkDetailT string, queryCondArgsT []interface{}, dmt *task.DataCompareTask) (decimal.Decimal, error) {
	var execQueryT string
	switch stringutil.StringUpper(d.dbTypeT) {
	case constant.DatabaseTypeOracle:
		if !strings.EqualFold(dmt.SqlHintT, "") && strings.EqualFold(dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, dmt.SchemaNameT, `"."`, dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else if !strings.EqualFold(dmt.SqlHintT, "") && !strings.EqualFold(dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, dmt.SchemaNameT, `"."`, dmt.TableNameT, `" AS OF SCN `, dmt.SnapshotPointT, ` WHERE `, chunkDetailT)
		} else if strings.EqualFold(dmt.SqlHintT, "") && !strings.EqualFold(dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM "`, dmt.SchemaNameT, `"."`, dmt.TableNameT, `" AS OF SCN `, dmt.SnapshotPointT, ` WHERE `, chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM "`, dmt.SchemaNameT, `"."`, dmt.TableNameT, `" WHERE `, chunkDetailT)
		}
		if strings.EqualFold(dmt.CompareMethod, constant.DataCompareMethodDatabaseCheckMD5) {
			execQueryT = fmt.Sprintf(`SELECT
TO_CHAR(NVL(TO_NUMBER(SUM(TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 1, 8),'xxxxxxxx')+
TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 9, 8),'xxxxxxxx')+
TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 17, 8),'xxxxxxxx')+ 
TO_NUMBER(SUBSTR(subq.ROWSCHECKSUM, 25, 8),'xxxxxxxx'))),0)) AS ROWSCHECKSUM
FROM
(%v) subq`, execQueryT)
		}
	case constant.DatabaseTypeTiDB:
		if !strings.EqualFold(dmt.SqlHintT, "") && strings.EqualFold(dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", dmt.SqlHintT, " ", columnDetailT, " FROM `", dmt.SchemaNameT, "`.`", dmt.TableNameT, "` WHERE ", chunkDetailT)
		} else if !strings.EqualFold(dmt.SqlHintT, "") && !strings.EqualFold(dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", dmt.SqlHintT, " ", columnDetailT, " FROM `", dmt.SchemaNameT, "`.`", dmt.TableNameT, "` AS OF TIMESTAMP '", dmt.SnapshotPointT, "' WHERE ", chunkDetailT)
		} else if strings.EqualFold(dmt.SqlHintT, "") && !strings.EqualFold(dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", columnDetailT, " FROM `", dmt.SchemaNameT, "`.`", dmt.TableNameT, "` AS OF TIMESTAMP '", dmt.SnapshotPointT, "' WHERE ", chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder("SELECT ", columnDetailT, " FROM `", dmt.SchemaNameT, "`.`", dmt.TableNameT, "` WHERE ", chunkDetailT)
		}
		if strings.EqualFold(dmt.CompareMethod, constant.DataCompareMethodDatabaseCheckMD5) {
			execQueryT = fmt.Sprintf(`
SELECT
 CAST(IFNULL(SUM(CONV(SUBSTRING(subq.ROWSCHECKSUM, 1, 8),16,10)+ 
 CONV(SUBSTRING(subq.ROWSCHECKSUM, 9, 8),16,10)+
 CONV(SUBSTRING(subq.ROWSCHECKSUM, 17, 8),16,10)+
 CONV(SUBSTRING(subq.ROWSCHECKSUM, 25, 8),16,10)),0) AS CHAR) AS ROWSCHECKSUM
FROM
(%v) subq`, execQueryT)
		}
	case constant.DatabaseTypeMySQL:
		if !strings.EqualFold(dmt.SqlHintT, "") {
			execQueryT = stringutil.StringBuilder("SELECT ", dmt.SqlHintT, " ", columnDetailT, " FROM `", dmt.SchemaNameT, "`.`", dmt.TableNameT, "` WHERE ", chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder("SELECT ", columnDetailT, " FROM ", dmt.SchemaNameT, "`.`", dmt.TableNameT, "` WHERE ", chunkDetailT)
		}
		if strings.EqualFold(dmt.CompareMethod, constant.DataCompareMethodDatabaseCheckMD5) {
			execQueryT = fmt.Sprintf(`
SELECT
 CAST(IFNULL(SUM(CONV(SUBSTRING(subq.ROWSCHECKSUM, 1, 8),16,10)+ 
 CONV(SUBSTRING(subq.ROWSCHECKSUM, 9, 8),16,10)+
 CONV(SUBSTRING(subq.ROWSCHECKSUM, 17, 8),16,10)+
 CONV(SUBSTRING(subq.ROWSCHECKSUM, 25, 8),16,10)),0) AS CHAR) AS ROWSCHECKSUM
FROM
(%v) subq`, execQueryT)
		}
	case constant.DatabaseTypePostgresql:
		if !strings.EqualFold(dmt.SqlHintT, "") && strings.EqualFold(dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SELECT `, dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, dmt.SchemaNameT, `"."`, dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else if !strings.EqualFold(dmt.SqlHintT, "") && !strings.EqualFold(dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, dmt.SnapshotPointT, `'`, constant.StringSeparatorSemicolon, `SELECT `, dmt.SqlHintT, ` `, columnDetailT, ` FROM "`, dmt.SchemaNameT, `"."`, dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else if strings.EqualFold(dmt.SqlHintT, "") && !strings.EqualFold(dmt.SnapshotPointT, "") {
			execQueryT = stringutil.StringBuilder(`SET TRANSACTION SNAPSHOT '`, dmt.SnapshotPointT, `'`, constant.StringSeparatorSemicolon, `SELECT `, columnDetailT, ` FROM "`, dmt.SchemaNameT, `"."`, dmt.TableNameT, `" WHERE `, chunkDetailT)
		} else {
			execQueryT = stringutil.StringBuilder(`SELECT `, columnDetailT, ` FROM "`, dmt.SchemaNameT, `"."`, dmt.TableNameT, `" WHERE `, chunkDetailT)
		}
		if strings.EqualFold(dmt.CompareMethod, constant.DataCompareMethodDatabaseCheckMD5) {
			execQueryT = fmt.Sprintf(`SELECT
COALESCE(SUM(('x'||SUBSTR(subq."ROWSCHECKSUM", 1, 8))::BIT(32)::BIGINT +
('x'||SUBSTR(subq."ROWSCHECKSUM", 9, 8))::BIT(32)::BIGINT +
('x'||SUBSTR(subq."ROWSCHECKSUM", 17, 8))::BIT(32)::BIGINT + 
('x'||SUBSTR(subq."ROWSCHECKSUM", 25, 8))::BIT(32)::BIGINT),0)::TEXT AS "ROWSCHECKSUM"
FROM
(%v) subq`, execQueryT)
		}
	default:
		return decimal.Decimal{}, fmt.Errorf("the task [%s] db_type_t [%s] is not supported, please contact author or reselect", dmt.TaskName, d.dbTypeT)
	}

	_, resultStrT, err := d.databaseT.GetDatabaseTableCompareRow(execQueryT, queryCondArgsT...)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("the database target query sql [%v] args [%v] running failed: [%v]", execQueryT, queryCondArgsT, err)
	}
	resultT, err := decimal.NewFromString(resultStrT[0]["ROWSCHECKSUM"])
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("parse the database target value [%s] failed: %v", resultStrT[0]["ROWSCHECKSUM"], err)
	}
	return resultT, nil
}

func (d *DataCompareSeek) genColumnDetail(columnNameSilSC, columnNameSliTC []string) (string, string, error) {
	if !d.disableCompareMd5 {
		switch d.taskFlow {
		case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB:
			return fmt.Sprintf(`UPPER(DBMS_CRYPTO.HASH(CONVERT(TO_CLOB(%s),'%s','%s'), 2)) AS ROWSCHECKSUM`,
					stringutil.StringJoin(columnNameSilSC, constant.StringSplicingSymbol), constant.ORACLECharsetAL32UTF8, stringutil.StringUpper(d.dbCharsetS)),
				fmt.Sprintf(`UPPER(MD5(CONVERT(CONCAT(%s) USING '%s'))) AS ROWSCHECKSUM`,
					stringutil.StringJoin(columnNameSliTC, constant.StringSeparatorComma), constant.MYSQLCharsetUTF8MB4), nil
		case constant.TaskFlowTiDBToOracle, constant.TaskFlowMySQLToOracle:
			return fmt.Sprintf(`UPPER(MD5(CONVERT(CONCAT(%s) USING '%s'))) AS ROWSCHECKSUM`,
					stringutil.StringJoin(columnNameSilSC, constant.StringSeparatorComma), constant.MYSQLCharsetUTF8MB4),
				fmt.Sprintf(`UPPER(DBMS_CRYPTO.HASH(CONVERT(TO_CLOB(%s),'%s','%s'), 2)) AS ROWSCHECKSUM`,
					stringutil.StringJoin(columnNameSliTC, constant.StringSplicingSymbol), constant.ORACLECharsetAL32UTF8, stringutil.StringUpper(d.dbCharsetT)), nil
		case constant.TaskFlowPostgresToMySQL, constant.TaskFlowPostgresToTiDB:
			return fmt.Sprintf(`MD5(CONVERT_TO(%s,'%s')) AS "ROWSCHECKSUM"`,
					stringutil.StringJoin(columnNameSilSC, constant.StringSplicingSymbol), constant.PostgreSQLCharsetUTF8),
				fmt.Sprintf(`MD5(CONVERT(CONCAT(%s) USING '%s')) AS ROWSCHECKSUM`,
					stringutil.StringJoin(columnNameSliTC, constant.StringSeparatorComma), constant.MYSQLCharsetUTF8MB4), nil
		case constant.TaskFlowTiDBToPostgres, constant.TaskFlowMySQLToPostgres:
			return fmt.Sprintf(`MD5(CONVERT(CONCAT(%s) USING '%s')) AS ROWSCHECKSUM`,
					stringutil.StringJoin(columnNameSilSC, constant.StringSeparatorComma), constant.MYSQLCharsetUTF8MB4),
				fmt.Sprintf(`MD5(CONVERT_TO(%s,'%s')) AS "ROWSCHECKSUM"`,
					stringutil.StringJoin(columnNameSliTC, constant.StringSplicingSymbol), constant.PostgreSQLCharsetUTF8), nil
		default:
			return "", "", fmt.Errorf("the task_name [%s] taskflow [%s] return isn't support, please contact author", d.taskName, d.taskFlow)
		}
	}
	switch d.taskFlow {
	case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB:
		return fmt.Sprintf(`TO_CHAR(NVL(SUM(CRC32(CONVERT(%s,'%s','%s'))),0)) AS ROWSCHECKSUM`,
				stringutil.StringJoin(columnNameSilSC, constant.StringSplicingSymbol), constant.ORACLECharsetAL32UTF8, stringutil.StringUpper(d.dbCharsetS)),
			fmt.Sprintf(`CAST(IFNULL(SUM(CRC32(CONVERT(CONCAT(%s) USING '%s'))),0) AS CHAR) AS ROWSCHECKSUM`,
				stringutil.StringJoin(columnNameSliTC, constant.StringSeparatorComma), constant.MYSQLCharsetUTF8MB4), nil
	case constant.TaskFlowTiDBToOracle, constant.TaskFlowMySQLToOracle:
		return fmt.Sprintf(`CAST(IFNULL(SUM(CRC32(CONVERT(CONCAT(%s) USING '%s'))),0) AS CHAR) AS ROWSCHECKSUM`,
				stringutil.StringJoin(columnNameSilSC, constant.StringSeparatorComma), constant.MYSQLCharsetUTF8MB4),
			fmt.Sprintf(`TO_CHAR(NVL(SUM(CRC32(CONVERT(%s,'%s','%s'))),0)) AS ROWSCHECKSUM`,
				stringutil.StringJoin(columnNameSliTC, constant.StringSplicingSymbol), constant.ORACLECharsetAL32UTF8, stringutil.StringUpper(d.dbCharsetT)), nil
	case constant.TaskFlowPostgresToMySQL, constant.TaskFlowPostgresToTiDB:
		return fmt.Sprintf(`COALESCE(SUM(CRC32(CONVERT_TO(%s,'%s'))),0)::TEXT AS "ROWSCHECKSUM"`,
				stringutil.StringJoin(columnNameSilSC, constant.StringSplicingSymbol), constant.PostgreSQLCharsetUTF8),
			fmt.Sprintf(`CAST(IFNULL(SUM(CRC32(CONVERT(CONCAT(%s) USING '%s'))),0) AS CHAR) AS ROWSCHECKSUM`,
				stringutil.StringJoin(columnNameSliTC, constant.StringSeparatorComma), constant.MYSQLCharsetUTF8MB4), nil
	case constant.TaskFlowTiDBToPostgres, constant.TaskFlowMySQLToPostgres:
		return fmt.Sprintf(`CAST(IFNULL(SUM(CRC32(CONVERT(CONCAT(%s) USING '%s'))),0) AS CHAR) AS ROWSCHECKSUM`,
				stringutil.StringJoin(columnNameSilSC, constant.StringSeparatorComma), constant.MYSQLCharsetUTF8MB4),
			fmt.Sprintf(`COALESCE(SUM(CRC32(CONVERT_TO(%s,'%s'))),0)::TEXT AS "ROWSCHECKSUM"`,
				stringutil.StringJoin(columnNameSliTC, constant.StringSplicingSymbol), constant.PostgreSQLCharsetUTF8), nil
	default:
		return "", "", fmt.Errorf("the task_name [%s] taskflow [%s] return isn't support, please contact author", d.taskName, d.taskFlow)
	}
}

// findDifferences recursively finds differences in checksums between two string slices.
func (d *DataCompareSeek) findDifferences(ctx context.Context, upstream, downstream []string,
	chunkDetailS, chunkDetailT string,
	queryCondArgsS, queryCondArgsT []interface{}, dmt *task.DataCompareTask, startIndex int) error {
	if len(upstream) == 0 && len(downstream) == 0 {
		return nil
	} else if len(upstream) != len(downstream) {
		return fmt.Errorf("length mismatch between upstream [%d] and downstream [%d] at index %d", len(upstream), len(downstream), startIndex)
	}

	columnDetailS, columnDetailT, err := d.genColumnDetail(upstream, downstream)
	if err != nil {
		return err
	}

	upstreamCRC, err := d.upstreamChecksum(columnDetailS, chunkDetailS, queryCondArgsS, dmt)
	if err != nil {
		return err
	}
	downstreamCRC, err := d.downstreamChecksum(columnDetailT, chunkDetailT, queryCondArgsT, dmt)
	if err != nil {
		return err
	}

	if !upstreamCRC.Equal(downstreamCRC) {
		if len(upstream) == 1 && len(downstream) == 1 {
			d.results <- [2]string{upstream[0], downstream[0]}
			return nil
		}

		mid := len(upstream) / 2

		errG, errCtx := errgroup.WithContext(ctx)
		errG.SetLimit(2)
		errG.Go(func() error {
			if err := d.findDifferences(errCtx, upstream[:mid], downstream[:mid], chunkDetailS, chunkDetailT, queryCondArgsS, queryCondArgsT, dmt, startIndex); err != nil {
				return err
			}
			return nil
		})

		errG.Go(func() error {
			if err := d.findDifferences(errCtx, upstream[mid:], downstream[mid:], chunkDetailS, chunkDetailT, queryCondArgsS, queryCondArgsT, dmt, startIndex+mid); err != nil {
				return err
			}
			return nil
		})

		return errG.Wait()
	}
	return nil
}

// collectDifferences collects the differences between the upstream and downstream slices.
func (d *DataCompareSeek) collectDifferences(ctx context.Context, chunkDetailS, chunkDetailT string,
	queryCondArgsS, queryCondArgsT []interface{}, dmt *task.DataCompareTask) ([][2]string, error) {
	var results [][2]string

	errG, errCtx := errgroup.WithContext(ctx)
	errG.Go(func() error {
		defer close(d.results) // Ensure the results channel is closed when the function returns
		if err := d.findDifferences(errCtx, d.columnDetailS, d.columnDetailT, chunkDetailS, chunkDetailT, queryCondArgsS, queryCondArgsT, dmt, 0); err != nil {
			return err
		}
		return nil
	})

	errG.Go(func() error {
		for diff := range d.results {
			results = append(results, diff)
		}
		return nil
	})

	if err := errG.Wait(); err != nil {
		return results, err
	}

	return results, nil
}
