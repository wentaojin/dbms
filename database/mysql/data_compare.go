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
package mysql

import (
	"context"
	"fmt"
	"github.com/wentaojin/dbms/utils/structure"
	"hash/crc32"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/shopspring/decimal"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func (d *Database) GetDatabaseTableConstraintIndexColumn(schemaNameS, tableNameS string) (map[string]string, error) {
	ci := make(map[string]string)
	pkRes, err := d.GetDatabaseTablePrimaryKey(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	if len(pkRes) > 1 {
		return nil, fmt.Errorf("the database schema [%s] table [%s] has more than one primary key", schemaNameS, tableNameS)
	}
	if len(pkRes) == 1 {
		ci[pkRes[0]["CONSTRAINT_NAME"]] = pkRes[0]["COLUMN_LIST"]
	}
	ukRes, err := d.GetDatabaseTableUniqueKey(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	for _, uk := range ukRes {
		ci[uk["CONSTRAINT_NAME"]] = uk["COLUMN_LIST"]
	}
	normalRes, err := d.GetDatabaseTableNormalIndex(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	for _, nr := range normalRes {
		ci[nr["INDEX_NAME"]] = nr["COLUMN_LIST"]
	}
	uniqueRes, err := d.GetDatabaseTableUniqueIndex(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	for _, ur := range uniqueRes {
		ci[ur["INDEX_NAME"]] = ur["COLUMN_LIST"]
	}
	return ci, nil
}

func (d *Database) GetDatabaseTableStatisticsBucket(schemaNameS, tableNameS string) (map[string][]structure.Bucket, error) {
	constraintColumns, err2 := d.GetDatabaseTableConstraintIndexColumn(schemaNameS, tableNameS)
	if err2 != nil {
		return nil, err2
	}
	_, res, err := d.GeneralQuery(`SELECT VERSION() AS VERSION`)
	if err != nil {
		return nil, err
	}

	buckets := make(map[string][]structure.Bucket)
	if strings.Contains(res[0]["VERSION"], constant.DatabaseTypeTiDB) {
		/*
			example in tidb:
			mysql> SHOW STATS_BUCKETS WHERE db_name= "test" AND table_name="testa";
			+---------+------------+----------------+-------------+----------+-----------+-------+---------+---------------------+---------------------+
			| Db_name | Table_name | Partition_name | Column_name | Is_index | Bucket_id | Count | Repeats | Lower_Bound         | Upper_Bound         |
			+---------+------------+----------------+-------------+----------+-----------+-------+---------+---------------------+---------------------+
			| test    | testa      |                | PRIMARY     |        1 |         0 |    64 |       1 | 1846693550524203008 | 1846838686059069440 |
			| test    | testa      |                | PRIMARY     |        1 |         1 |   128 |       1 | 1846840885082324992 | 1847056389361369088 |
			+---------+------------+----------------+-------------+----------+-----------+-------+---------+---------------------+---------------------+
		*/
		_, res, err = d.GeneralQuery(fmt.Sprintf(`SHOW STATS_BUCKETS WHERE db_name= '%s' AND table_name= '%s'`, schemaNameS, tableNameS))
		if err != nil {
			return nil, err
		}

		var pkIntegerColumnName string
		if pkValus, ok := constraintColumns["PRIMARY"]; ok {
			pkSlis := stringutil.StringSplit(pkValus, constant.StringSeparatorComma)
			if len(pkSlis) == 1 {
				columnInfo, err := d.GetDatabaseTableColumnInfo(schemaNameS, tableNameS, false)
				if err != nil {
					return nil, err
				}
				for _, column := range columnInfo {
					if strings.EqualFold(pkSlis[0], column["COLUMN_NAME"]) && stringutil.IsContainedString(constant.TiDBDatabaseIntegerColumnDatatypePrimaryKey, stringutil.StringUpper(column["DATA_TYPE"])) {
						pkIntegerColumnName = pkSlis[0]
						break
					}
				}
			}
		}

		for _, r := range res {
			if _, ok := buckets[r["COLUMN_NAME"]]; !ok {
				buckets[r["COLUMN_NAME"]] = []structure.Bucket{}
			}
			count, err := strconv.ParseInt(r["Count"], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("the database [%s] table [%s] statistics bucket error parsing integer: %v", schemaNameS, tableNameS, err)
			}
			// filter index column
			if strings.EqualFold(r["IS_INDEX"], "1") {
				// transform index name to index column
				columnNames, ok := constraintColumns[r["COLUMN_NAME"]]
				if !ok {
					return nil, fmt.Errorf("the database [%s] table [%s] statistics bucket index name [%s] is existed, but the index system view query record not found", schemaNameS, tableNameS, r["COLUMN_NAME"])
				}
				buckets[columnNames] = append(buckets[columnNames], structure.Bucket{
					Count:      count,
					LowerBound: r["LOWER_BOUND"],
					UpperBound: r["UPPER_BOUND"],
				})
			}
			// when primary key is int type, the columnName will be column's name, not `PRIMARY`, check and transform here.
			if !strings.EqualFold(pkIntegerColumnName, "") && strings.EqualFold(r["COLUMN_NAME"], pkIntegerColumnName) {
				buckets[r["COLUMN_NAME"]] = append(buckets[r["COLUMN_NAME"]], structure.Bucket{
					Count:      count,
					LowerBound: r["LOWER_BOUND"],
					UpperBound: r["UPPER_BOUND"],
				})
			}
		}
		return buckets, nil
	}
	return nil, fmt.Errorf("the database [%s] table [%s] statistics bucket doesn't supported, only support tidb database, version: [%v]", schemaNameS, tableNameS, res[0]["VERSION"])
}

func (d *Database) GetDatabaseTableStatisticsHistogram(schemaNameS, tableNameS string) (map[string][]structure.Histogram, error) {
	_, res, err := d.GeneralQuery(`SELECT VERSION() AS VERSION`)
	if err != nil {
		return nil, err
	}

	hists := make(map[string][]structure.Histogram)
	if strings.Contains(res[0]["VERSION"], constant.DatabaseTypeTiDB) {
		_, res, err = d.GeneralQuery(fmt.Sprintf(`SHOW STATS_HISTOGRAMS WHERE db_name= '%s' AND table_name= '%s'`, schemaNameS, tableNameS))
		if err != nil {
			return nil, err
		}
		for _, r := range res {
			if _, ok := hists[r["COLUMN_NAME"]]; !ok {
				hists[r["COLUMN_NAME"]] = make([]structure.Histogram, 0, 100)
			}
			disCount, err := strconv.ParseInt(r["DISTINCT_COUNT"], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing integer: %v", err)
			}
			nullCount, err := strconv.ParseInt(r["NULL_COUNT"], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing integer: %v", err)
			}
			hists[r["COLUMN_NAME"]] = append(hists[r["COLUMN_NAME"]], structure.Histogram{
				DistinctCount: disCount,
				NullCount:     nullCount,
			})
		}
		return hists, nil
	}
	return nil, fmt.Errorf("the database table statistics histograms doesn't supported, only support tidb database, version: [%v]", res[0]["VERSION"])
}

func (d *Database) GetDatabaseTableColumnProperties(schemaNameS, tableNameS, columnNameS string, collationS bool) ([]map[string]string, error) {
	sqlStr := fmt.Sprintf(`SELECT
		TABLE_SCHEMA AS OWNER,
		TABLE_NAME,
		COLUMN_NAME,
		DATA_TYPE,
		NUMERIC_SCALE,
		DATETIME_PRECISION,
		COLLATION_NAME AS COLLATION
	FROM
		INFORMATION_SCHEMA.COLUMNS
	WHERE
		TABLE_SCHEMA = '%s' 
		AND TABLE_NAME = '%s'
		AND COLUMN_NAME = '%s'`, schemaNameS, tableNameS, columnNameS)
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableCompareData(querySQL string, callTimeout int, dbCharsetS, dbCharsetT string) ([]string, uint32, map[string]int64, error) {
	var (
		rowData        []string
		columnNames    []string
		databaaseTypes []string
		scanTypes      []string
		err            error
		crc32Sum       uint32
	)

	var crc32Val uint32 = 0

	// record repeat counts
	batchRowsM := make(map[string]int64)

	deadline := time.Now().Add(time.Duration(callTimeout) * time.Second)

	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()

	rows, err := d.QueryContext(ctx, querySQL)
	if err != nil {
		return nil, crc32Sum, nil, err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, crc32Sum, nil, err
	}

	for _, ct := range colTypes {
		convertUtf8Raw, err := stringutil.CharsetConvert([]byte(ct.Name()), dbCharsetS, constant.CharsetUTF8MB4)
		if err != nil {
			return nil, crc32Sum, nil, fmt.Errorf("column [%s] charset convert failed, %v", ct.Name(), err)
		}
		columnNames = append(columnNames, stringutil.BytesToString(convertUtf8Raw))
		databaaseTypes = append(databaaseTypes, ct.DatabaseTypeName())
		scanTypes = append(scanTypes, ct.ScanType().String())
	}

	columnNums := len(columnNames)

	// data scan
	rawResult := make([][]byte, columnNums)
	valuePtrs := make([]interface{}, columnNums)
	for i, _ := range columnNames {
		valuePtrs[i] = &rawResult[i]
	}

	for rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return nil, crc32Sum, nil, err
		}

		for i, colName := range columnNames {
			val := rawResult[i]
			// ORACLE database NULL and "" are the same, but mysql database NULL and "" are the different
			if val == nil {
				rowData = append(rowData, `NULL`)
			} else if stringutil.BytesToString(val) == "" {
				rowData = append(rowData, "")
			} else {
				switch scanTypes[i] {
				case "sql.NullInt16":
					rowData = append(rowData, stringutil.BytesToString(val))
				case "sql.NullInt32":
					rowData = append(rowData, stringutil.BytesToString(val))
				case "sql.NullInt64":
					rowData = append(rowData, stringutil.BytesToString(val))
				case "sql.NullFloat64":
					rowData = append(rowData, stringutil.BytesToString(val))
				default:
					if strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeDecimal) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeBigint) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeDouble) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeDoublePrecision) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeFloat) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeInt) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeInteger) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeMediumint) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeNumeric) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeReal) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeSmallint) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeTinyint) {
						rfs, err := decimal.NewFromString(stringutil.BytesToString(val))
						if err != nil {
							return nil, crc32Sum, nil, fmt.Errorf("column [%s] datatype [%s] value [%v] NewFromString strconv failed, %v", colName, databaaseTypes[i], val, err)
						}
						rowData = append(rowData, rfs.String())
					} else {
						convertUtf8Raw, err := stringutil.CharsetConvert(val, dbCharsetS, constant.CharsetUTF8MB4)
						if err != nil {
							return nil, crc32Sum, nil, fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
						}
						convertTargetRaw, err := stringutil.CharsetConvert(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT)
						if err != nil {
							return nil, crc32Sum, nil, fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
						}
						rowData = append(rowData, fmt.Sprintf("'%v'", stringutil.BytesToString(convertTargetRaw)))
					}
				}
			}
		}

		// append
		batchKey := stringutil.StringJoin(rowData, constant.StringSeparatorComma)

		crc32Sum = atomic.AddUint32(&crc32Val, crc32.ChecksumIEEE([]byte(batchKey)))

		if val, ok := batchRowsM[batchKey]; ok {
			batchRowsM[batchKey] = val + 1
		} else {
			batchRowsM[batchKey] = 1
		}
		// clear
		rowData = rowData[0:0]
	}

	if err = rows.Err(); err != nil {
		return nil, crc32Sum, nil, err
	}

	return columnNames, crc32Sum, batchRowsM, nil
}

func (d *Database) FindDatabaseTableBestColumn(schemaNameS, tableNameS, columnNameS string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableColumnBucket(schemaNameS, tableNameS string, columnNameS, datatypeS string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}
