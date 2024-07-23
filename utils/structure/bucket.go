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
	"fmt"
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

func (h *HighestBucket) String() string {
	jsonStr, _ := stringutil.MarshalJSON(h)
	return jsonStr
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
func StringSliceCreateBuckets(newColumnsBs []string, numRows int64) []Bucket {
	// Sort the string slice
	sort.Strings(newColumnsBs)

	// Create buckets
	buckets := make([]Bucket, len(newColumnsBs))
	for i := 0; i < len(newColumnsBs); i++ {
		if i == len(newColumnsBs)-1 {
			// For the last element, use the previous element as the upper bound
			buckets[i] = Bucket{LowerBound: newColumnsBs[i], UpperBound: newColumnsBs[i-1]}
		} else {
			buckets[i] = Bucket{LowerBound: newColumnsBs[i], UpperBound: newColumnsBs[i+1]}
		}
	}

	// Remove the last bucket which was created with incorrect upper bound
	if len(buckets) > 0 {
		buckets = buckets[:len(buckets)-1]

		// divide buckets equally by number of the database table data rows, and the number of rows in the current bucket is obtained by subtracting the buckets.
		counts := numRows / int64(len(buckets))

		for i, _ := range buckets {
			if i == 0 {
				buckets[i].Count = counts
				continue
			}
			buckets[i].Count = buckets[i-1].Count + counts
		}
	}

	return buckets
}

func FindColumnMatchConstraintIndexNames(consColumns map[string]string, compareField string, ignoreFields []string) []string {
	var conSlis []string
	for cons, column := range consColumns {
		columns := stringutil.StringSplit(column, constant.StringSeparatorComplexSymbol)
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

func SortDistinctCountHistogram(histogramMap map[string]Histogram, consColumns map[string]string) SortHistograms {
	if len(histogramMap) == 0 {
		return nil
	}
	var hists SortHistograms
	for k, v := range histogramMap {
		hists = append(hists, SortHistogram{
			Key:      k,
			Value:    v,
			OrderKey: consColumns[k],
		})
	}

	sort.Sort(hists)
	return hists
}

func FindMatchDistinctCountBucket(sortHists SortHistograms, bucketMap map[string][]Bucket, consColumns map[string]string) (*HighestBucket, error) {
	var (
		sortBuckets []*HighestBucket
	)
	for _, hist := range sortHists {
		if val, ok := bucketMap[hist.Key]; ok {
			sortBuckets = append(sortBuckets, &HighestBucket{
				IndexName:         hist.Key,
				IndexColumn:       stringutil.StringSplit(consColumns[hist.Key], constant.StringSeparatorComplexSymbol),
				ColumnDatatype:    nil,
				ColumnCollation:   nil,
				DatetimePrecision: nil,
				Buckets:           val,
			})
		}
	}

	// the found more key, and return first key
	if len(sortBuckets) >= 1 {
		return sortBuckets[0], nil
	}
	return &HighestBucket{}, fmt.Errorf("the database table index name is empty, please contact author or analyze table statistics and historgam rerunning")
}
