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
	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"github.com/wentaojin/dbms/utils/structure"
	"go.uber.org/zap"
	"strings"
)

func ProcessUpstreamDatabaseTableColumnStatisticsBucket(divideDbType, divideDbCharset string, caseFieldRule string,
	database database.IDatabase, schemaName, tableName string, consIndexColumns *structure.HighestBucket, chunkSize int64) (*structure.HighestBucket, []*structure.Range, error) {
	if consIndexColumns == nil {
		return nil, nil, nil
	}

	var chunkRanges []*structure.Range

	// column name charset trasnform
	var newColumns []string
	for _, col := range consIndexColumns.IndexColumn {
		var columnName string
		switch stringutil.StringUpper(divideDbType) {
		case constant.DatabaseTypeOracle:
			convertUtf8Raws, err := stringutil.CharsetConvert([]byte(col), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(divideDbCharset)], constant.CharsetUTF8MB4)
			if err != nil {
				return nil, nil, err
			}
			columnName = stringutil.BytesToString(convertUtf8Raws)

			if strings.EqualFold(caseFieldRule, constant.ParamValueDataCompareCaseFieldRuleLower) {
				columnName = strings.ToLower(stringutil.BytesToString(convertUtf8Raws))
			}
			if strings.EqualFold(caseFieldRule, constant.ParamValueDataCompareCaseFieldRuleUpper) {
				columnName = strings.ToUpper(stringutil.BytesToString(convertUtf8Raws))
			}

		case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
			convertUtf8Raws, err := stringutil.CharsetConvert([]byte(col), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(divideDbCharset)], constant.CharsetUTF8MB4)
			if err != nil {
				return nil, nil, err
			}
			columnName = stringutil.BytesToString(convertUtf8Raws)

			if strings.EqualFold(caseFieldRule, constant.ParamValueDataCompareCaseFieldRuleLower) {
				columnName = strings.ToLower(stringutil.BytesToString(convertUtf8Raws))
			}
			if strings.EqualFold(caseFieldRule, constant.ParamValueDataCompareCaseFieldRuleUpper) {
				columnName = strings.ToUpper(stringutil.BytesToString(convertUtf8Raws))
			}
		default:
			return nil, nil, fmt.Errorf("the database type [%s] is not supported, please contact author or reselect", divideDbType)
		}

		newColumns = append(newColumns, columnName)
	}

	consIndexColumns.IndexColumn = newColumns

	for _, b := range consIndexColumns.Buckets {
		switch stringutil.StringUpper(divideDbType) {
		case constant.DatabaseTypeOracle:
			convertUtf8Raws, err := stringutil.CharsetConvert([]byte(b.LowerBound), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(divideDbCharset)], constant.CharsetUTF8MB4)
			if err != nil {
				return nil, nil, err
			}
			b.LowerBound = stringutil.BytesToString(convertUtf8Raws)

			convertUtf8Raws, err = stringutil.CharsetConvert([]byte(b.UpperBound), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(divideDbCharset)], constant.CharsetUTF8MB4)
			if err != nil {
				return nil, nil, err
			}
			b.UpperBound = stringutil.BytesToString(convertUtf8Raws)
		case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
			convertUtf8Raws, err := stringutil.CharsetConvert([]byte(b.LowerBound), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(divideDbCharset)], constant.CharsetUTF8MB4)
			if err != nil {
				return nil, nil, err
			}
			b.LowerBound = stringutil.BytesToString(convertUtf8Raws)

			convertUtf8Raws, err = stringutil.CharsetConvert([]byte(b.UpperBound), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(divideDbCharset)], constant.CharsetUTF8MB4)
			if err != nil {
				return nil, nil, err
			}
			b.UpperBound = stringutil.BytesToString(convertUtf8Raws)
		default:
			return nil, nil, fmt.Errorf("the database type [%s] is not supported, please contact author or reselect", divideDbType)
		}
	}

	// divide buckets
	var (
		lowerValues, upperValues []string
		latestCount              int64
		err                      error
	)
	firstBucket := 0

	halfChunkSize := chunkSize >> 1
	// `firstBucket` is the first bucket of one chunk.
	for i := firstBucket; i < len(consIndexColumns.Buckets); i++ {
		count := consIndexColumns.Buckets[i].Count - latestCount
		if count < chunkSize {
			// merge more buckets into one chunk
			continue
		}

		upperValues, err = ExtractDatabaseTableStatisticsValuesFromBuckets(divideDbType, consIndexColumns.Buckets[i].UpperBound, consIndexColumns.IndexColumn)
		if err != nil {
			return nil, nil, err
		}

		chunkRange := structure.NewChunkRange()
		for j, columnName := range consIndexColumns.IndexColumn {
			var lowerValue, upperValue string
			if len(lowerValues) > 0 {
				lowerValue = lowerValues[j]
			}
			if len(upperValues) > 0 {
				upperValue = upperValues[j]
			}
			err = chunkRange.Update(divideDbType, divideDbCharset, columnName, consIndexColumns.ColumnCollation[j], consIndexColumns.ColumnDatatype[j], consIndexColumns.DatetimePrecision[j], lowerValue, upperValue, len(lowerValues) > 0, len(upperValues) > 0)
			if err != nil {
				return nil, nil, err
			}
		}

		// `splitRangeByRandom` will skip when chunkCnt <= 1
		// assume the number of the selected buckets is `x`
		// if x >= 2                              ->  chunkCnt = 1
		// if x = 1                               ->  chunkCnt = (count + halfChunkSize) / chunkSize
		// count >= chunkSize
		if i == firstBucket {
			chunkCnt := int((count + halfChunkSize) / chunkSize)
			buckets, err := DivideDatabaseTableColumnStatisticsBucket(divideDbType, divideDbCharset, database, schemaName, tableName, &structure.HighestBucket{
				IndexName:         consIndexColumns.IndexName,
				IndexColumn:       consIndexColumns.IndexColumn,
				ColumnDatatype:    consIndexColumns.ColumnDatatype,
				ColumnCollation:   consIndexColumns.ColumnCollation,
				DatetimePrecision: consIndexColumns.DatetimePrecision,
				Buckets:           consIndexColumns.Buckets,
			}, chunkRange, chunkCnt)
			if err != nil {
				return nil, nil, err
			}
			chunkRanges = append(chunkRanges, buckets...)
		} else {
			// use multi-buckets so chunkCnt = 1
			buckets, err := DivideDatabaseTableColumnStatisticsBucket(divideDbType, divideDbCharset, database, schemaName, tableName, &structure.HighestBucket{
				IndexName:         consIndexColumns.IndexName,
				IndexColumn:       consIndexColumns.IndexColumn,
				ColumnDatatype:    consIndexColumns.ColumnDatatype,
				ColumnCollation:   consIndexColumns.ColumnCollation,
				DatetimePrecision: consIndexColumns.DatetimePrecision,
				Buckets:           consIndexColumns.Buckets,
			}, chunkRange, 1)
			if err != nil {
				return nil, nil, err
			}
			chunkRanges = append(chunkRanges, buckets...)
		}

		latestCount = consIndexColumns.Buckets[i].Count
		lowerValues = upperValues
		firstBucket = i + 1
	}

	// merge the rest keys into one chunk
	chunkRange := structure.NewChunkRange()
	if len(lowerValues) > 0 {
		for j, columnName := range consIndexColumns.IndexColumn {
			err = chunkRange.Update(divideDbType, divideDbCharset, columnName, consIndexColumns.ColumnCollation[j], consIndexColumns.ColumnDatatype[j], consIndexColumns.DatetimePrecision[j], lowerValues[j], "", true, false)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	// When the table is much less than chunkSize,
	// it will return a chunk include the whole table.
	buckets, err := DivideDatabaseTableColumnStatisticsBucket(divideDbType, divideDbCharset, database, schemaName, tableName, &structure.HighestBucket{
		IndexName:         consIndexColumns.IndexName,
		IndexColumn:       consIndexColumns.IndexColumn,
		ColumnDatatype:    consIndexColumns.ColumnDatatype,
		ColumnCollation:   consIndexColumns.ColumnCollation,
		DatetimePrecision: consIndexColumns.DatetimePrecision,
		Buckets:           consIndexColumns.Buckets,
	}, chunkRange, 1)
	if err != nil {
		return nil, nil, err
	}
	chunkRanges = append(chunkRanges, buckets...)

	return consIndexColumns, chunkRanges, nil
}

func ReverseUpstreamHighestBucketDownstreamRule(taskFlow, dbTypeT, dbCharsetS string, columnDatatypeT []string, cons *structure.HighestBucket, columnRouteRule map[string]string) (*structure.Rule, error) {
	downstreamConsIndexColumns := &structure.HighestBucket{
		IndexName:         cons.IndexName,
		ColumnDatatype:    columnDatatypeT,
		ColumnCollation:   cons.ColumnCollation,
		DatetimePrecision: cons.DatetimePrecision,
	}
	var columnCollationDownStreams []string

	switch stringutil.StringUpper(dbTypeT) {
	case constant.DatabaseTypeOracle:
		for _, c := range cons.ColumnCollation {
			if !strings.EqualFold(c, "") {
				columnCollationDownStreams = append(columnCollationDownStreams, constant.MigrateTableStructureDatabaseCollationMap[taskFlow][stringutil.StringUpper(c)][constant.MigrateTableStructureDatabaseCharsetMap[taskFlow][dbCharsetS]])
			} else {
				columnCollationDownStreams = append(columnCollationDownStreams, c)
			}
		}
	case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
		for _, c := range cons.ColumnCollation {
			if !strings.EqualFold(c, "") {
				columnCollationDownStreams = append(columnCollationDownStreams, constant.MigrateTableStructureDatabaseCollationMap[taskFlow][stringutil.StringUpper(c)][constant.MigrateTableStructureDatabaseCharsetMap[taskFlow][dbCharsetS]])
			} else {
				columnCollationDownStreams = append(columnCollationDownStreams, c)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported the downstream database type: %s", dbTypeT)
	}

	if len(columnCollationDownStreams) > 0 {
		downstreamConsIndexColumns.ColumnCollation = columnCollationDownStreams
	}

	columnDatatypeM := make(map[string]string)
	columnCollationM := make(map[string]string)
	columnDatePrecisionM := make(map[string]string)

	for i, c := range downstreamConsIndexColumns.IndexColumn {
		columnDatatypeM[c] = downstreamConsIndexColumns.ColumnDatatype[i]
		columnCollationM[c] = downstreamConsIndexColumns.ColumnCollation[i]
		columnDatePrecisionM[c] = downstreamConsIndexColumns.DatetimePrecision[i]
	}

	return &structure.Rule{
		IndexColumnRule:       columnRouteRule,
		ColumnDatatypeRule:    columnDatatypeM,
		ColumnCollationRule:   columnCollationM,
		DatetimePrecisionRule: columnDatePrecisionM,
	}, nil
}

func ProcessDownstreamDatabaseTableColumnStatisticsBucket(dbTypeT string, bs []*structure.Range, r *structure.Rule) ([]*structure.Range, error) {
	var ranges []*structure.Range
	for _, b := range bs {
		for _, s := range b.Bounds {
			if val, ok := r.IndexColumnRule[s.ColumnName]; ok {
				s.ColumnName = val
			}
			if val, ok := r.ColumnCollationRule[s.ColumnName]; ok {
				s.Collation = val
			}
		}

		for k, c := range b.BoundOffset {
			if val, ok := r.IndexColumnRule[k]; ok {
				delete(b.BoundOffset, k)
				b.BoundOffset[val] = c
			}
		}

		ranges = append(ranges, &structure.Range{
			DBType:      dbTypeT,
			Bounds:      b.Bounds,
			BoundOffset: b.BoundOffset,
		})
	}
	return ranges, nil
}

func GetDownstreamDatabaseTableColumnDatatype(schemaNameT, tableNameT string, databaseT database.IDatabase, originColumnNameSli []string, columnNameRouteRule map[string]string) ([]string, error) {
	var (
		columnNameSliT []string
		columnTypeSliT []string
	)
	// keep column order
	for _, c := range originColumnNameSli {
		if val, ok := columnNameRouteRule[c]; ok {
			columnNameSliT = append(columnNameSliT, val)
		} else {
			jsonStr, err := stringutil.MarshalIndentJSON(columnNameRouteRule)
			if err != nil {
				return nil, fmt.Errorf("the database table column route rule json marshal failed: %v", err)
			}
			return nil, fmt.Errorf("the database table column [%s] route rule mapping [%v] failed, please check or retry again", c, jsonStr)
		}
	}

	props, err := databaseT.GetDatabaseTableColumnProperties(schemaNameT, tableNameT, columnNameSliT)
	if err != nil {
		return columnTypeSliT, err
	}
	// keep order
	for _, c := range columnNameSliT {
		for _, p := range props {
			if strings.EqualFold(c, p["COLUMN_NAME"]) {
				columnTypeSliT = append(columnTypeSliT, p["DATA_TYPE"])
			}
		}
	}
	return columnTypeSliT, nil
}

// ExtractDatabaseTableStatisticsValuesFromBuckets analyze upperBound or lowerBound to string for each column.
// upperBound and lowerBound are looks like '(123, abc)' for multiple fields, or '123' for one field.
func ExtractDatabaseTableStatisticsValuesFromBuckets(divideDbType, valueString string, columnNames []string) ([]string, error) {
	switch stringutil.StringUpper(divideDbType) {
	case constant.DatabaseTypeTiDB:
		// FIXME: maybe some values contains '(', ')' or ', '
		vStr := strings.Trim(valueString, "()")
		values := strings.Split(vStr, ", ")
		if len(values) != len(columnNames) {
			return nil, fmt.Errorf("extract database type [%s] value %s failed", divideDbType, valueString)
		}
		return values, nil
	case constant.DatabaseTypeOracle:
		values := strings.Split(valueString, constant.StringSeparatorComma)
		if len(values) != len(columnNames) {
			return nil, fmt.Errorf("extract database type [%s] value %s failed", divideDbType, valueString)
		}
		return values, nil
	default:
		return nil, fmt.Errorf("extract database type [%s] value %s is not supported, please contact author or reselect", divideDbType, valueString)
	}
}

// DivideDatabaseTableColumnStatisticsBucket splits a chunk to multiple chunks by random
// Notice: If the `count <= 1`, it will skip splitting and return `chunk` as a slice directly.
func DivideDatabaseTableColumnStatisticsBucket(divideDbType, divideDbCharset string, database database.IDatabase, schemaName, tableName string, consIndexColumns *structure.HighestBucket, chunkRange *structure.Range, divideCountCnt int) ([]*structure.Range, error) {
	var chunkRanges []*structure.Range

	if divideCountCnt <= 1 {
		chunkRanges = append(chunkRanges, chunkRange)
		return chunkRanges, nil
	}

	chunkConds := chunkRange.ToString()

	randomValueSli, err := database.GetDatabaseTableRandomValues(schemaName, tableName, consIndexColumns.IndexColumn, chunkConds, divideCountCnt-1, consIndexColumns.ColumnCollation)
	if err != nil {
		return nil, err
	}

	logger.Debug("divide database bucket value by random", zap.Stringer("chunk", chunkRange), zap.Int("random values num", len(randomValueSli)))

	if len(randomValueSli) > 0 {
		randomValues, randomValuesLen := stringutil.StringSliceAlignLen(randomValueSli)
		for i := 0; i <= randomValuesLen; i++ {
			newChunk := chunkRange.Copy()

			for j, columnName := range consIndexColumns.IndexColumn {
				if i == 0 {
					if len(randomValues[j]) == 0 {
						break
					}
					err = newChunk.Update(divideDbType,
						divideDbCharset,
						columnName,
						consIndexColumns.ColumnCollation[j],
						consIndexColumns.ColumnDatatype[j],
						consIndexColumns.DatetimePrecision[j],
						"", randomValues[j][i], false, true)
					if err != nil {
						return chunkRanges, err
					}
				} else if i == len(randomValues[0]) {
					err = newChunk.Update(divideDbType,
						divideDbCharset,
						columnName,
						consIndexColumns.ColumnCollation[j],
						consIndexColumns.ColumnDatatype[j],
						consIndexColumns.DatetimePrecision[j],
						randomValues[j][i-1], "", true, false)
					if err != nil {
						return chunkRanges, err
					}
				} else {
					err = newChunk.Update(divideDbType,
						divideDbCharset,
						columnName,
						consIndexColumns.ColumnCollation[j],
						consIndexColumns.ColumnDatatype[j],
						consIndexColumns.DatetimePrecision[j], randomValues[j][i-1], randomValues[j][i], true, true)
					if err != nil {
						return chunkRanges, err
					}
				}
			}
			chunkRanges = append(chunkRanges, newChunk)
		}
	}

	logger.Debug("divide database bucket value by random", zap.Stringer("origin chunk range", chunkRange), zap.Int("divide chunk range num", len(chunkRanges)))

	return chunkRanges, nil
}
