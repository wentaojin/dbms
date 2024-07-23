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
)

type SortHistogram struct {
	Key      string // IndexName
	Value    Histogram
	OrderKey string // IndexColumn
}

type SortHistograms []SortHistogram

func (s SortHistograms) Len() int {
	return len(s)
}

func (s SortHistograms) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortHistograms) Less(i, j int) bool {
	if s[i].Value.DistinctCount != s[j].Value.DistinctCount {
		// Note that this is sorting in descending order
		return s[i].Value.DistinctCount > s[j].Value.DistinctCount
	}
	// If the DistinctCount values is the same, sort according to the length of the string key
	return len(stringutil.StringSplit(s[i].OrderKey, constant.StringSeparatorComplexSymbol)) < len(stringutil.StringSplit(s[j].OrderKey, constant.StringSeparatorComplexSymbol))
}
