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
	"strings"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"github.com/wentaojin/dbms/utils/structure"
	"go.uber.org/zap"
)

type Divide struct {
	DBTypeS     string
	DBCharsetS  string
	SchemaNameS string
	TableNameS  string
	ChunkSize   int64
	DatabaseS   database.IDatabase
	Cons        *structure.Selectivity
	RangeC      chan []*structure.Range
}

func (d *Divide) ProcessUpstreamStatisticsBucket() error {
	// divide buckets
	var (
		lowerValues, upperValues []string
		latestCount              int64
		err                      error
	)

	// `bucketID` is the bucket id of one chunk.
	bucketID := 0

	halfChunkSize := d.ChunkSize >> 1
	// `firstBucket` is the first bucket of one chunk.
	for i := bucketID; i < len(d.Cons.Buckets); i++ {
		count := d.Cons.Buckets[i].Count - latestCount
		if count < d.ChunkSize {
			// merge more buckets into one chunk
			logger.Info("divide database bucket value processing",
				zap.String("schema_name_s", d.SchemaNameS),
				zap.String("table_name_s", d.TableNameS),
				zap.Int("bucket_id", bucketID),
				zap.Int64("bucket_counts", count),
				zap.Int64("chunk_size", d.ChunkSize),
				zap.String("origin_lower_value", d.Cons.Buckets[i].LowerBound),
				zap.String("origin_upper_value", d.Cons.Buckets[i].UpperBound),
				zap.Any("new_lower_values", lowerValues),
				zap.Any("new_upper_values", upperValues),
				zap.String("chunk_process_action", "bucket_counts not match chunk_size, skip divide"))
			continue
		}

		upperValues, err = ExtractDatabaseTableStatisticsValuesFromBuckets(d.DBTypeS, d.Cons.Buckets[i].UpperBound, d.Cons.IndexColumn)
		if err != nil {
			return err
		}

		logger.Debug("divide database bucket value processing",
			zap.String("schema_name_s", d.SchemaNameS),
			zap.String("table_name_s", d.TableNameS),
			zap.Int("bucket_id", bucketID),
			zap.Int64("bucket_counts", count),
			zap.Int64("chunk_size", d.ChunkSize),
			zap.String("origin_lower_value", d.Cons.Buckets[i].LowerBound),
			zap.String("origin_upper_value", d.Cons.Buckets[i].UpperBound),
			zap.String("chunk_process_action", "divide"))

		var chunkRange *structure.Range
		switch stringutil.StringUpper(d.DBTypeS) {
		case constant.DatabaseTypeOracle:
			chunkRange = structure.NewChunkRange(d.DBTypeS, d.DBCharsetS, constant.ORACLECharsetAL32UTF8)
		case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
			chunkRange = structure.NewChunkRange(d.DBTypeS, d.DBCharsetS, constant.MYSQLCharsetUTF8MB4)
		case constant.DatabaseTypePostgresql:
			chunkRange = structure.NewChunkRange(d.DBTypeS, d.DBCharsetS, constant.PostgreSQLCharsetUTF8)
		default:
			return fmt.Errorf("the database type [%s] is not supported, please contact author or reselect", d.DBTypeS)
		}
		for j, columnName := range d.Cons.IndexColumn {
			var lowerValue, upperValue string
			if len(lowerValues) > 0 {
				lowerValue = lowerValues[j]
			}
			if len(upperValues) > 0 {
				upperValue = upperValues[j]
			}
			err = chunkRange.Update(columnName, d.Cons.ColumnCollation[j], d.Cons.ColumnDatatype[j], d.Cons.DatetimePrecision[j], lowerValue, upperValue, len(lowerValues) > 0, len(upperValues) > 0)
			if err != nil {
				return err
			}
		}

		// count >= chunkSize
		if i == bucketID {
			chunkCnt := int((count + halfChunkSize) / d.ChunkSize)
			ranges, err := DivideDatabaseTableColumnStatisticsBucket(d.DatabaseS, d.SchemaNameS, d.TableNameS, d.Cons, chunkRange, chunkCnt)
			if err != nil {
				return err
			}
			d.RangeC <- ranges

			logger.Debug("divide database bucket value processing",
				zap.String("schema_name_s", d.SchemaNameS),
				zap.String("table_name_s", d.TableNameS),
				zap.Int("current_bucket_id", bucketID),
				zap.Int64("bucket_counts", count),
				zap.Int64("chunk_size", d.ChunkSize),
				zap.String("origin_lower_value", d.Cons.Buckets[i].LowerBound),
				zap.String("origin_upper_value", d.Cons.Buckets[i].UpperBound),
				zap.Any("new_lower_values", lowerValues),
				zap.Any("new_upper_values", upperValues),
				zap.Any("chunk_divide_range", chunkRange),
				zap.String("chunk_process_action", "random divide"))
		} else {
			// merge bucket
			// use multi-buckets so chunkCnt = 1
			ranges, err := DivideDatabaseTableColumnStatisticsBucket(d.DatabaseS, d.SchemaNameS, d.TableNameS, d.Cons, chunkRange, 1)
			if err != nil {
				return err
			}
			d.RangeC <- ranges

			logger.Debug("divide database bucket value processing",
				zap.String("schema_name_s", d.SchemaNameS),
				zap.String("table_name_s", d.TableNameS),
				zap.Int("current_bucket_id", bucketID),
				zap.Int64("bucket_counts", count),
				zap.Int64("chunk_size", d.ChunkSize),
				zap.String("origin_lower_value", d.Cons.Buckets[i].LowerBound),
				zap.String("origin_upper_value", d.Cons.Buckets[i].UpperBound),
				zap.Any("new_lower_values", lowerValues),
				zap.Any("new_upper_values", upperValues),
				zap.Any("chunk_divide_range", chunkRange),
				zap.String("chunk_process_action", "random divide"))
		}

		latestCount = d.Cons.Buckets[i].Count
		lowerValues = upperValues
		bucketID = i + 1
	}

	// merge the rest keys into one chunk
	var chunkRange *structure.Range
	switch stringutil.StringUpper(d.DBTypeS) {
	case constant.DatabaseTypeOracle:
		chunkRange = structure.NewChunkRange(d.DBTypeS, d.DBCharsetS, constant.ORACLECharsetAL32UTF8)
	case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
		chunkRange = structure.NewChunkRange(d.DBTypeS, d.DBCharsetS, constant.MYSQLCharsetUTF8MB4)
	case constant.DatabaseTypePostgresql:
		chunkRange = structure.NewChunkRange(d.DBTypeS, d.DBCharsetS, constant.PostgreSQLCharsetUTF8)
	default:
		return fmt.Errorf("the database type [%s] is not supported, please contact author or reselect", d.DBTypeS)
	}
	if len(lowerValues) > 0 {
		for j, columnName := range d.Cons.IndexColumn {
			err = chunkRange.Update(columnName, d.Cons.ColumnCollation[j], d.Cons.ColumnDatatype[j], d.Cons.DatetimePrecision[j], lowerValues[j], "", true, false)
			if err != nil {
				return err
			}
		}
	}

	logger.Debug("divide database bucket value processing",
		zap.String("schema_name_s", d.SchemaNameS),
		zap.String("table_name_s", d.TableNameS),
		zap.Int("bucket_id", bucketID),
		zap.Int64("chunk_size", d.ChunkSize),
		zap.Any("new_lower_values", lowerValues),
		zap.Any("new_upper_values", upperValues),
		zap.Any("chunk_divide_range", chunkRange),
		zap.String("chunk_process_action", "merge divide"))

	// When the table is much less than chunkSize,
	// it will return a chunk include the whole table.
	ranges, err := DivideDatabaseTableColumnStatisticsBucket(d.DatabaseS, d.SchemaNameS, d.TableNameS, d.Cons, chunkRange, 1)
	if err != nil {
		return err
	}
	d.RangeC <- ranges

	return nil
}

func ProcessDownstreamStatisticsBucket(dbTypeT, dbCharsetT string, bs []*structure.Range, r *structure.Rule) ([]*structure.Range, error) {
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
				bd.Collation = val
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

		// the database charset
		switch stringutil.StringUpper(dbTypeT) {
		case constant.DatabaseTypeOracle:
			ranges = append(ranges, &structure.Range{
				DBType:        dbTypeT,
				DBCharsetFrom: dbCharsetT,
				DBCharsetDest: constant.ORACLECharsetAL32UTF8,
				Bounds:        bounds,
				BoundOffset:   newBoundOffset,
			})
		case constant.DatabaseTypeMySQL, constant.DatabaseTypeTiDB:
			ranges = append(ranges, &structure.Range{
				DBType:        dbTypeT,
				DBCharsetFrom: dbCharsetT,
				DBCharsetDest: constant.MYSQLCharsetUTF8MB4,
				Bounds:        bounds,
				BoundOffset:   newBoundOffset,
			})
		case constant.DatabaseTypePostgresql:
			ranges = append(ranges, &structure.Range{
				DBType:        dbTypeT,
				DBCharsetFrom: dbCharsetT,
				DBCharsetDest: constant.PostgreSQLCharsetUTF8,
				Bounds:        bounds,
				BoundOffset:   newBoundOffset,
			})
		default:
			return nil, fmt.Errorf("the database type [%s] is not supported, please contact author or reselect", dbTypeT)
		}

	}
	return ranges, nil
}

func GetDownstreamTableColumnDatatype(schemaNameT, tableNameT string, databaseT database.IDatabase, originColumnNameSli []string, columnNameRouteRule map[string]string) ([]string, error) {
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
