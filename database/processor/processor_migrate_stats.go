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
func DivideDatabaseTableColumnStatisticsBucket(database database.IDatabase, schemaName, tableName string, cons *structure.Selectivity, chunkRange *structure.Range, divideCountCnt int) ([]*structure.Range, error) {
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

	logger.Debug("divide database bucket value by random", zap.Stringer("chunk", chunkRange), zap.Int("random values num", len(randomValueSli)), zap.Any("random values", randomValueSli))

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
				// bucket upper newChunk.Bounds[j].Upper
				err = newChunk.Update(
					columnName,
					cons.ColumnCollation[j],
					cons.ColumnDatatype[j],
					cons.DatetimePrecision[j],
					randomValueSli[i-1][j], newChunk.Bounds[j].Upper, true, true)
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

	logger.Debug("divide database bucket value by random",
		zap.Int("divide chunk range num", len(chunkRanges)),
		zap.Stringer("origin chunk range", chunkRange),
		zap.Any("new chunk range", chunkRanges))

	return chunkRanges, nil
}
