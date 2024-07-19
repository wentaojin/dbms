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
package structure

import (
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"sort"
	"strings"
)

// HighestBucket store the highest selectivity constraint or index bucket
type HighestBucket struct {
	IndexName         string
	IndexColumn       []string
	ColumnDatatype    []string
	ColumnCollation   []string
	DatetimePrecision []string
	Buckets           []Bucket
}

// Bucket store the index statistics bucket
type Bucket struct {
	Count      int64
	LowerBound string
	UpperBound string
}

// Histogram store the index statistics histogram
type Histogram struct {
	DistinctCount int64
	NullCount     int64
}

// Rule store the highest selectivity bucket upstream and downstream mapping rule, key is the database table column name
type Rule struct {
	IndexColumnRule       map[string]string
	ColumnDatatypeRule    map[string]string
	ColumnCollationRule   map[string]string
	DatetimePrecisionRule map[string]string
}

// StringSliceCreateBuckets creates buckets from a sorted slice of strings.
// Each bucket's lower bound is the value of one element, and the upper bound is the next element's value.
// The last bucket will have the same upper bound as the previous bucket if there are no more elements.
func StringSliceCreateBuckets(newColumnsBs []string, estimateCounts int64) []Bucket {
	// Sort the string slice
	sort.Strings(newColumnsBs)

	// Create buckets
	buckets := make([]Bucket, len(newColumnsBs))
	for i := 0; i < len(newColumnsBs); i++ {
		if i == len(newColumnsBs)-1 {
			// For the last element, use the previous element as the upper bound
			buckets[i] = Bucket{Count: estimateCounts, LowerBound: newColumnsBs[i], UpperBound: newColumnsBs[i-1]}
		} else {
			buckets[i] = Bucket{Count: estimateCounts, LowerBound: newColumnsBs[i], UpperBound: newColumnsBs[i+1]}
		}
	}

	// Remove the last bucket which was created with incorrect upper bound
	if len(buckets) > 0 {
		buckets = buckets[:len(buckets)-1]
	}

	return buckets
}

func FindMaxDistinctCountHistogram(histogramMap map[string]Histogram, consColumns map[string]string) map[string]Histogram {
	if len(histogramMap) == 0 {
		return nil
	}
	maxHist := make(map[string]Histogram)
	bestHist := make(map[string]Histogram)
	maxDistinctCount := int64(0)

	for k, h := range histogramMap {
		// record distinct count equal
		if h.DistinctCount >= maxDistinctCount {
			maxDistinctCount = h.DistinctCount
			maxHist[k] = h
		}
	}

	var bestKey string
	minColumnLen := int64(0)
	for k, _ := range maxHist {
		columns := stringutil.StringSplit(consColumns[k], constant.StringSeparatorComma)
		if int64(len(columns)) <= minColumnLen {
			minColumnLen = int64(len(columns))
			// if the columns length equal, return the last
			bestKey = k
		}
	}

	bestHist[bestKey] = maxHist[bestKey]
	return bestHist
}

func FindColumnMatchConstraintIndexNames(consColumns map[string]string, compareField string, ignoreFields []string) []string {
	var conSlis []string
	for cons, column := range consColumns {
		columns := stringutil.StringSplit(column, constant.StringSeparatorComma)
		// match prefix index
		// column a, index a,b,c and b,c,a ,only match a,b,c
		if !strings.EqualFold(compareField, "") {
			if strings.EqualFold(columns[0], compareField) {
				conSlis = append(conSlis, cons)
			}
		} else {
			// ignore fields
			if !stringutil.IsContainedString(ignoreFields, columns[0]) {
				conSlis = append(conSlis, cons)
			}
		}
	}
	return conSlis
}

func ExtractColumnMatchHistogram(matchIndexes []string, histogramMap map[string]Histogram) map[string]Histogram {
	matchHists := make(map[string]Histogram)
	for _, matchIndex := range matchIndexes {
		if val, ok := histogramMap[matchIndex]; ok {
			matchHists[matchIndex] = val
		}
	}
	return matchHists
}

func FindMaxDistinctCountBucket(maxHists map[string]Histogram, bucketMap map[string][]Bucket, consColumns map[string]string) (string, []Bucket, string) {
	var (
		highestConKey     string
		highestConColumns string
	)

	maxBuckets := make(map[string][]Bucket)
	for k, _ := range maxHists {
		if val, ok := bucketMap[k]; ok {
			maxBuckets[k] = val
			highestConKey = k
			break
		}
	}

	for k, _ := range maxBuckets {
		if val, ok := consColumns[k]; ok {
			highestConColumns = val
			break
		}
	}
	return highestConKey, maxBuckets[highestConKey], highestConColumns
}
