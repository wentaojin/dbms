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
package oracle

import (
	"fmt"
	"time"

	"github.com/wentaojin/dbms/utils/filter"

	"github.com/wentaojin/dbms/utils/stringutil"

	"go.uber.org/zap"
)

func (d *Database) FilterDatabaseSchema(sourceSchemas []string) error {
	allOraSchemas, err := d.GetDatabaseSchema()
	if err != nil {
		return err
	}

	for _, s := range sourceSchemas {
		if !stringutil.IsContainedString(allOraSchemas, stringutil.StringUpper(s)) {
			return fmt.Errorf("oracle schema isn't contained in the database, failed schemas: [%v]", s)
		}
	}
	return nil
}

func (d *Database) FilterDatabaseTable(sourceSchema string, sourceIncludeTables, sourceExcludeTables []string) ([]string, error) {
	startTime := time.Now()
	var (
		exporterTableSlice []string
		excludeTables      []string
		err                error
	)

	allTables, err := d.GetDatabaseTable(stringutil.StringUpper(sourceSchema))
	if err != nil {
		return exporterTableSlice, err
	}

	switch {
	case len(sourceIncludeTables) != 0 && len(sourceExcludeTables) == 0:
		f, err := filter.Parse(sourceIncludeTables)
		if err != nil {
			return nil, fmt.Errorf("oracle schema filter include tables failed, error: [%v]", err)
		}

		for _, t := range allTables {
			if f.MatchTable(t) {
				exporterTableSlice = append(exporterTableSlice, t)
			}
		}
	case len(sourceIncludeTables) == 0 && len(sourceExcludeTables) != 0:
		f, err := filter.Parse(sourceExcludeTables)
		if err != nil {
			return nil, fmt.Errorf("oracle schema filter exclude tables failed, error: [%v]", err)
		}

		for _, t := range allTables {
			if f.MatchTable(t) {
				excludeTables = append(excludeTables, t)
			}
		}
		exporterTableSlice = stringutil.StringItemsFilterDifference(allTables, excludeTables)

	case len(sourceIncludeTables) == 0 && len(sourceExcludeTables) == 0:
		exporterTableSlice = allTables

	default:
		return exporterTableSlice, fmt.Errorf("source config params source-include-table/source-exclude-table cannot exist at the same time")
	}

	if len(exporterTableSlice) == 0 {
		return exporterTableSlice, fmt.Errorf("exporter tables aren't exist, please check config params source-include-table/source-exclude-table")
	}

	endTime := time.Now()
	zap.L().Info("get oracle tables",
		zap.String("schema", sourceSchema),
		zap.Strings("exporter tables list", exporterTableSlice),
		zap.Int("include table counts", len(exporterTableSlice)),
		zap.Int("exclude table counts", len(excludeTables)),
		zap.Int("all table counts", len(allTables)),
		zap.String("cost", endTime.Sub(startTime).String()))

	return exporterTableSlice, nil
}

func (d *Database) FilterDatabaseIncompatibleTable(sourceSchema string, exporters []string) ([]string, []string, []string, []string, []string, error) {
	partitionTables, err := d.filterOraclePartitionTable(sourceSchema, exporters)
	if err != nil {
		return []string{}, []string{}, []string{}, []string{}, []string{}, fmt.Errorf("error on filter r.Oracle partition table: %v", err)
	}
	temporaryTables, err := d.filterOracleTemporaryTable(sourceSchema, exporters)
	if err != nil {
		return []string{}, []string{}, []string{}, []string{}, []string{}, fmt.Errorf("error on filter r.Oracle temporary table: %v", err)

	}
	clusteredTables, err := d.filterOracleClusteredTable(sourceSchema, exporters)
	if err != nil {
		return []string{}, []string{}, []string{}, []string{}, []string{}, fmt.Errorf("error on filter r.Oracle clustered table: %v", err)

	}
	materializedView, err := d.filterOracleMaterializedView(sourceSchema, exporters)
	if err != nil {
		return []string{}, []string{}, []string{}, []string{}, []string{}, fmt.Errorf("error on filter r.Oracle materialized view: %v", err)

	}

	if len(partitionTables) != 0 {
		zap.L().Warn("partition tables",
			zap.String("schema", sourceSchema),
			zap.String("partition table list", fmt.Sprintf("%v", partitionTables)),
			zap.String("suggest", "if necessary, please manually convert and process the tables in the above list"))
	}
	if len(temporaryTables) != 0 {
		zap.L().Warn("temporary tables",
			zap.String("schema", sourceSchema),
			zap.String("temporary table list", fmt.Sprintf("%v", temporaryTables)),
			zap.String("suggest", "if necessary, please manually process the tables in the above list"))
	}
	if len(clusteredTables) != 0 {
		zap.L().Warn("clustered tables",
			zap.String("schema", sourceSchema),
			zap.String("clustered table list", fmt.Sprintf("%v", clusteredTables)),
			zap.String("suggest", "if necessary, please manually process the tables in the above list"))
	}

	var exporterTables []string
	if len(materializedView) != 0 {
		zap.L().Warn("materialized views",
			zap.String("schema", sourceSchema),
			zap.String("materialized view list", fmt.Sprintf("%v", materializedView)),
			zap.String("suggest", "if necessary, please manually process the tables in the above list"))

		// exclude materialized view
		exporterTables = stringutil.StringItemsFilterDifference(exporters, materializedView)
	} else {
		exporterTables = exporters
	}
	return partitionTables, temporaryTables, clusteredTables, materializedView, exporterTables, nil
}

func (d *Database) filterOraclePartitionTable(sourceSchema string, exporters []string) ([]string, error) {
	tables, err := d.GetDatabasePartitionTable(stringutil.StringUpper(sourceSchema))
	if err != nil {
		return nil, err
	}
	return stringutil.StringItemsFilterIntersection(exporters, tables), nil
}

func (d *Database) filterOracleTemporaryTable(sourceSchema string, exporters []string) ([]string, error) {
	tables, err := d.GetDatabaseTemporaryTable(stringutil.StringUpper(sourceSchema))
	if err != nil {
		return nil, err
	}
	return stringutil.StringItemsFilterIntersection(exporters, tables), nil
}

func (d *Database) filterOracleClusteredTable(sourceSchema string, exporters []string) ([]string, error) {
	tables, err := d.GetDatabaseClusteredTable(stringutil.StringUpper(sourceSchema))
	if err != nil {
		return nil, err
	}
	return stringutil.StringItemsFilterIntersection(exporters, tables), nil
}

func (d *Database) filterOracleMaterializedView(sourceSchema string, exporters []string) ([]string, error) {
	tables, err := d.GetDatabaseMaterializedView(stringutil.StringUpper(sourceSchema))
	if err != nil {
		return nil, err
	}
	return stringutil.StringItemsFilterIntersection(exporters, tables), nil
}
