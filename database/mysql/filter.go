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
	"fmt"
	"time"

	"github.com/wentaojin/dbms/utils/filter"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
)

func (d *Database) FilterDatabaseTable(sourceSchema string, includeTableS, excludeTableS []string) ([]string, error) {
	startTime := time.Now()
	var (
		exporterTableSlice []string
		excludeTableSlice  []string
		err                error
	)

	allTables, err := d.GetDatabaseTable(sourceSchema)
	if err != nil {
		return exporterTableSlice, err
	}

	switch {
	case len(includeTableS) != 0 && len(excludeTableS) == 0:
		f, err := filter.Parse(includeTableS)
		if err != nil {
			return nil, fmt.Errorf("oracle schema filter include tables failed, error: [%v]", err)
		}

		for _, t := range allTables {
			if f.MatchTable(t) {
				exporterTableSlice = append(exporterTableSlice, t)
			}
		}
	case len(includeTableS) == 0 && len(excludeTableS) != 0:
		f, err := filter.Parse(excludeTableS)
		if err != nil {
			return nil, fmt.Errorf("oracle schema filter exclude tables failed, error: [%v]", err)
		}

		for _, t := range allTables {
			if f.MatchTable(t) {
				excludeTableSlice = append(excludeTableSlice, t)
			}
		}
		exporterTableSlice = stringutil.StringItemsFilterDifference(allTables, excludeTableSlice)

	case len(includeTableS) == 0 && len(excludeTableS) == 0:
		exporterTableSlice = allTables

	default:
		return exporterTableSlice, fmt.Errorf("source config params include-table-s/exclude-table-s cannot exist at the same time")
	}

	if len(exporterTableSlice) == 0 {
		return exporterTableSlice, fmt.Errorf("exporter tables aren't exist, please check config params include-table-s/exclude-table-s")
	}

	endTime := time.Now()
	zap.L().Info("filter mysql compatible database table",
		zap.String("schema", sourceSchema),
		zap.Strings("exporter tables list", exporterTableSlice),
		zap.Int("include table counts", len(exporterTableSlice)),
		zap.Int("exclude table counts", len(excludeTableSlice)),
		zap.Int("all table counts", len(allTables)),
		zap.String("cost", endTime.Sub(startTime).String()))

	return exporterTableSlice, nil
}

func (d *Database) FilterDatabaseIncompatibleTable(sourceSchema string, exporters []string) ([]string, []string, []string, []string, []string, error) {
	//TODO implement me
	panic("implement me")
}
