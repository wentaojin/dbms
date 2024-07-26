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

func ProcessUpstreamDatabaseTableColumnStatisticsBucket(dbTypeS, dbCharsetS string, caseFieldRule string,
	database database.IDatabase, schemaName, tableName string, cons *structure.HighestBucket, chunkSize int64, enableCollation bool) (*structure.HighestBucket, []*structure.Range, error) {
	if cons == nil {
		return nil, nil, nil
	}

	var chunkRanges []*structure.Range

	// column name charset transform
	var newColumns []string
	for _, col := range cons.IndexColumn {
		var columnName string
		switch stringutil.StringUpper(dbTypeS) {
		case constant.DatabaseTypeOracle:
			convertUtf8Raws, err := stringutil.CharsetConvert([]byte(col), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(dbCharsetS)], constant.CharsetUTF8MB4)
			if err != nil {
				return nil, nil, nil
			}
			columnName = stringutil.BytesToString(convertUtf8Raws)

			if strings.EqualFold(caseFieldRule, constant.ParamValueDataCompareCaseFieldRuleLower) {
				columnName = strings.ToLower(stringutil.BytesToString(convertUtf8Raws))
			}
			if strings.EqualFold(caseFieldRule, constant.ParamValueDataCompareCaseFieldRuleUpper) {
				columnName = strings.ToUpper(stringutil.BytesToString(convertUtf8Raws))
			}

		case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
			convertUtf8Raws, err := stringutil.CharsetConvert([]byte(col), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(dbCharsetS)], constant.CharsetUTF8MB4)
			if err != nil {
				return nil, nil, nil
			}
			columnName = stringutil.BytesToString(convertUtf8Raws)

			if strings.EqualFold(caseFieldRule, constant.ParamValueDataCompareCaseFieldRuleLower) {
				columnName = strings.ToLower(stringutil.BytesToString(convertUtf8Raws))
			}
			if strings.EqualFold(caseFieldRule, constant.ParamValueDataCompareCaseFieldRuleUpper) {
				columnName = strings.ToUpper(stringutil.BytesToString(convertUtf8Raws))
			}
		default:
			return nil, nil, fmt.Errorf("the database type [%s] is not supported, please contact author or reselect", dbTypeS)
		}

		newColumns = append(newColumns, columnName)
	}

	cons.IndexColumn = newColumns

	for _, b := range cons.Buckets {
		switch stringutil.StringUpper(dbTypeS) {
		case constant.DatabaseTypeOracle:
			convertUtf8Raws, err := stringutil.CharsetConvert([]byte(b.LowerBound), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(dbCharsetS)], constant.CharsetUTF8MB4)
			if err != nil {
				return nil, nil, err
			}
			b.LowerBound = stringutil.BytesToString(convertUtf8Raws)

			convertUtf8Raws, err = stringutil.CharsetConvert([]byte(b.UpperBound), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(dbCharsetS)], constant.CharsetUTF8MB4)
			if err != nil {
				return nil, nil, err
			}
			b.UpperBound = stringutil.BytesToString(convertUtf8Raws)
		case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
			convertUtf8Raws, err := stringutil.CharsetConvert([]byte(b.LowerBound), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(dbCharsetS)], constant.CharsetUTF8MB4)
			if err != nil {
				return nil, nil, err
			}
			b.LowerBound = stringutil.BytesToString(convertUtf8Raws)

			convertUtf8Raws, err = stringutil.CharsetConvert([]byte(b.UpperBound), constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(dbCharsetS)], constant.CharsetUTF8MB4)
			if err != nil {
				return nil, nil, err
			}
			b.UpperBound = stringutil.BytesToString(convertUtf8Raws)
		default:
			return nil, nil, fmt.Errorf("the database type [%s] is not supported, please contact author or reselect", dbTypeS)
		}
	}

	// collation enable setting
	for i, _ := range cons.ColumnCollation {
		if !enableCollation {
			// ignore collation setting, fill ""
			cons.ColumnCollation[i] = constant.DataCompareDisabledCollationSettingFillEmptyString
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
	for i := firstBucket; i < len(cons.Buckets); i++ {
		count := cons.Buckets[i].Count - latestCount
		if count < chunkSize {
			// merge more buckets into one chunk
			continue
		}

		upperValues, err = ExtractDatabaseTableStatisticsValuesFromBuckets(dbTypeS, cons.Buckets[i].UpperBound, cons.IndexColumn)
		if err != nil {
			return nil, nil, err
		}

		chunkRange := structure.NewChunkRange(dbTypeS)
		for j, columnName := range cons.IndexColumn {
			var lowerValue, upperValue string
			if len(lowerValues) > 0 {
				lowerValue = lowerValues[j]
			}
			if len(upperValues) > 0 {
				upperValue = upperValues[j]
			}
			err = chunkRange.Update(columnName, cons.ColumnCollation[j], cons.ColumnDatatype[j], cons.DatetimePrecision[j], lowerValue, upperValue, len(lowerValues) > 0, len(upperValues) > 0)
			if err != nil {
				return nil, nil, err
			}
		}

		// count >= chunkSize
		if i == firstBucket {
			chunkCnt := int((count + halfChunkSize) / chunkSize)
			buckets, err := DivideDatabaseTableColumnStatisticsBucket(database, schemaName, tableName, cons, chunkRange, chunkCnt)
			if err != nil {
				return nil, nil, err
			}
			chunkRanges = append(chunkRanges, buckets...)

			logger.Debug("divide database bucket value",
				zap.String("schema_name_s", schemaName),
				zap.String("table_name_s", tableName),
				zap.Int("bucket_id", i),
				zap.Int("bucket_first_value", firstBucket),
				zap.Int64("bucket_counts", count),
				zap.Int64("chunk_size", chunkSize),
				zap.Any("lower_values", lowerValues),
				zap.Any("upper_values", upperValues),
				zap.Any("chunk_ranges", chunkRange))
		} else {
			// use multi-buckets so chunkCnt = 1
			buckets, err := DivideDatabaseTableColumnStatisticsBucket(database, schemaName, tableName, cons, chunkRange, 1)
			if err != nil {
				return nil, nil, err
			}
			chunkRanges = append(chunkRanges, buckets...)
			logger.Debug("divide database bucket value",
				zap.String("schema_name_s", schemaName),
				zap.String("table_name_s", tableName),
				zap.Int("bucket_id", i),
				zap.Int("bucket_first_value", firstBucket),
				zap.Int64("bucket_counts", count),
				zap.Int64("chunk_size", chunkSize),
				zap.Any("lower_values", lowerValues),
				zap.Any("upper_values", upperValues),
				zap.Any("chunk_ranges", chunkRange))
		}

		latestCount = cons.Buckets[i].Count
		lowerValues = upperValues
		firstBucket = i + 1
	}

	// merge the rest keys into one chunk
	chunkRange := structure.NewChunkRange(dbTypeS)
	if len(lowerValues) > 0 {
		for j, columnName := range cons.IndexColumn {
			err = chunkRange.Update(columnName, cons.ColumnCollation[j], cons.ColumnDatatype[j], cons.DatetimePrecision[j], lowerValues[j], "", true, false)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	// When the table is much less than chunkSize,
	// it will return a chunk include the whole table.
	buckets, err := DivideDatabaseTableColumnStatisticsBucket(database, schemaName, tableName, cons, chunkRange, 1)
	if err != nil {
		return nil, nil, err
	}
	chunkRanges = append(chunkRanges, buckets...)

	logger.Debug("divide database bucket value",
		zap.String("schema_name_s", schemaName),
		zap.String("table_name_s", tableName),
		zap.String("bucket_id", "merge"),
		zap.Int64("chunk_size", chunkSize),
		zap.Any("lower_values", lowerValues),
		zap.Any("upper_values", upperValues),
		zap.Any("chunk_ranges", chunkRange))
	return cons, chunkRanges, nil
}

func ReverseUpstreamHighestBucketDownstreamRule(taskFlow, dbTypeT, dbCharsetS string, columnDatatypeT []string, cons *structure.HighestBucket, columnRouteRule map[string]string) (*structure.Rule, error) {
	cons.ColumnDatatype = columnDatatypeT

	var columnCollationDownStreams []string

	switch stringutil.StringUpper(dbTypeT) {
	case constant.DatabaseTypeOracle:
		for _, c := range cons.ColumnCollation {
			if !strings.EqualFold(c, constant.DataCompareDisabledCollationSettingFillEmptyString) {
				collationTStr := constant.MigrateTableStructureDatabaseCollationMap[taskFlow][stringutil.StringUpper(c)][constant.MigrateTableStructureDatabaseCharsetMap[taskFlow][dbCharsetS]]
				collationTSli := stringutil.StringSplit(collationTStr, constant.StringSeparatorSlash)
				// get first collation
				columnCollationDownStreams = append(columnCollationDownStreams, collationTSli[0])
			} else {
				columnCollationDownStreams = append(columnCollationDownStreams, c)
			}
		}
	case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
		for _, c := range cons.ColumnCollation {
			if !strings.EqualFold(c, constant.DataCompareDisabledCollationSettingFillEmptyString) {
				collationTStr := constant.MigrateTableStructureDatabaseCollationMap[taskFlow][stringutil.StringUpper(c)][constant.MigrateTableStructureDatabaseCharsetMap[taskFlow][dbCharsetS]]
				collationTSli := stringutil.StringSplit(collationTStr, constant.StringSeparatorSlash)
				// get first collation
				columnCollationDownStreams = append(columnCollationDownStreams, collationTSli[0])
			} else {
				columnCollationDownStreams = append(columnCollationDownStreams, c)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported the downstream database type: %s", dbTypeT)
	}

	if len(columnCollationDownStreams) > 0 {
		cons.ColumnCollation = columnCollationDownStreams
	}

	columnDatatypeM := make(map[string]string)
	columnCollationM := make(map[string]string)
	columnDatePrecisionM := make(map[string]string)

	for i, c := range cons.IndexColumn {
		columnDatatypeM[c] = cons.ColumnDatatype[i]
		columnCollationM[c] = cons.ColumnCollation[i]
		columnDatePrecisionM[c] = cons.DatetimePrecision[i]
	}

	logger.Info("reverse data compare task init table chunk",
		zap.Any("upstreamConsIndexColumns", &structure.Rule{
			IndexColumnRule:       columnRouteRule,
			ColumnDatatypeRule:    columnDatatypeM,
			ColumnCollationRule:   columnCollationM,
			DatetimePrecisionRule: columnDatePrecisionM,
		}))

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
		var bounds []*structure.Bound

		newBoundOffset := make(map[string]int)

		for _, s := range b.Bounds {
			bd := &structure.Bound{}
			bd.Lower = s.Lower
			bd.Upper = s.Upper
			bd.HasLower = s.HasLower
			bd.HasUpper = s.HasUpper

			if val, ok := r.IndexColumnRule[s.ColumnName]; ok {
				bd.ColumnName = val
			} else {
				return nil, fmt.Errorf("the database type [%s] upstream range column [%s] not found map index column rule [%v]", dbTypeT, s.ColumnName, r.IndexColumnRule)
			}
			if val, ok := r.ColumnCollationRule[s.ColumnName]; ok {
				switch dbTypeT {
				case constant.DatabaseTypeOracle:
					if !strings.EqualFold(val, constant.DataCompareDisabledCollationSettingFillEmptyString) {
						bd.Collation = fmt.Sprintf("'NLS_SORT = %s'", val)
					} else {
						bd.Collation = val
					}
				case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
					if !strings.EqualFold(val, constant.DataCompareDisabledCollationSettingFillEmptyString) {
						bd.Collation = fmt.Sprintf("COLLATE '%s'", val)
					} else {
						bd.Collation = val
					}
				default:
					return nil, fmt.Errorf("the downstream column collation database type [%s] isn't support, please contact author or reselect", dbTypeT)
				}
			} else {
				return nil, fmt.Errorf("the database type [%s] upstream range column [%s] collation not found map index column collation rule [%v]", dbTypeT, s.ColumnName, r.ColumnCollationRule)
			}
			if val, ok := r.ColumnDatatypeRule[s.ColumnName]; ok {
				bd.Datatype = val
			} else {
				return nil, fmt.Errorf("the database type [%s] upstream range column [%s] not found map column datatype rule [%v]", dbTypeT, s.ColumnName, r.ColumnDatatypeRule)
			}
			if val, ok := r.DatetimePrecisionRule[s.ColumnName]; ok {
				bd.DatetimePrecision = val
			} else {
				return nil, fmt.Errorf("the database type [%s] upstream range column [%s] not found map column datetime precision rule [%v]", dbTypeT, s.ColumnName, r.DatetimePrecisionRule)
			}
			bounds = append(bounds, bd)
		}

		for k, c := range b.BoundOffset {
			if val, ok := r.IndexColumnRule[k]; ok {
				newBoundOffset[val] = c
			} else {
				return nil, fmt.Errorf("the database type [%s] upstream range column [%s] bound offset not found map bound offset [%v]", dbTypeT, k, b.BoundOffset)
			}
		}

		ranges = append(ranges, &structure.Range{
			DBType:      dbTypeT,
			Bounds:      bounds,
			BoundOffset: newBoundOffset,
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
	if len(props) == 0 {
		return columnTypeSliT, fmt.Errorf("the dowstream database schema [%s] table [%s] columns [%s] query has no records", schemaNameT, tableNameT, stringutil.StringJoin(originColumnNameSli, constant.StringSeparatorComma))
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
			return nil, fmt.Errorf("extract database type [%s] value %s failed, values %v not match columnNames %v", divideDbType, valueString, values, columnNames)
		}
		return values, nil
	case constant.DatabaseTypeOracle:
		values := strings.Split(valueString, constant.StringSeparatorComma)
		if len(values) != len(columnNames) {
			return nil, fmt.Errorf("extract database type [%s] value %s failed, values %v not match columnNames %v", divideDbType, valueString, values, columnNames)
		}
		return values, nil
	default:
		return nil, fmt.Errorf("extract database type [%s] value %s is not supported, please contact author or reselect", divideDbType, valueString)
	}
}

// DivideDatabaseTableColumnStatisticsBucket splits a chunk to multiple chunks by random
// Notice: If the `count <= 1`, it will skip splitting and return `chunk` as a slice directly.
func DivideDatabaseTableColumnStatisticsBucket(database database.IDatabase, schemaName, tableName string, cons *structure.HighestBucket, chunkRange *structure.Range, divideCountCnt int) ([]*structure.Range, error) {
	var chunkRanges []*structure.Range

	if divideCountCnt <= 1 {
		chunkRanges = append(chunkRanges, chunkRange)
		return chunkRanges, nil
	}

	chunkConds, chunkArgs := chunkRange.ToString()

	randomValueSli, err := database.GetDatabaseTableRandomValues(schemaName, tableName, cons.IndexColumn, chunkConds, chunkArgs, divideCountCnt-1, cons.ColumnCollation)
	if err != nil {
		return nil, err
	}

	logger.Debug("divide database bucket value by random", zap.Stringer("chunk", chunkRange), zap.Int("random values num", len(randomValueSli)))

	for i := 0; i <= len(randomValueSli); i++ {
		newChunk := chunkRange.Copy()

		for j, columnName := range cons.IndexColumn {
			if i == 0 {
				if len(randomValueSli) == 0 {
					// randomValues is empty, so chunks will append chunk itself
					break
				}
				err = newChunk.Update(
					columnName,
					cons.ColumnCollation[j],
					cons.ColumnDatatype[j],
					cons.DatetimePrecision[j],
					"", randomValueSli[i][j], false, true)
				if err != nil {
					return chunkRanges, err
				}
			} else if i == len(randomValueSli) {
				err = newChunk.Update(
					columnName,
					cons.ColumnCollation[j],
					cons.ColumnDatatype[j],
					cons.DatetimePrecision[j],
					randomValueSli[i-1][j], "", true, false)
				if err != nil {
					return chunkRanges, err
				}
			} else {
				err = newChunk.Update(
					columnName,
					cons.ColumnCollation[j],
					cons.ColumnDatatype[j],
					cons.DatetimePrecision[j],
					randomValueSli[i-1][j],
					randomValueSli[i][j], true, true)
				if err != nil {
					return chunkRanges, err
				}
			}
		}
		chunkRanges = append(chunkRanges, newChunk)
	}

	logger.Debug("divide database bucket value by random", zap.Stringer("origin chunk range", chunkRange), zap.Int("divide chunk range num", len(chunkRanges)))

	return chunkRanges, nil
}
