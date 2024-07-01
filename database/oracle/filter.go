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
	"github.com/wentaojin/dbms/utils/structure"
	"time"

	"github.com/wentaojin/dbms/utils/filter"

	"github.com/wentaojin/dbms/utils/stringutil"

	"go.uber.org/zap"
)

func (d *Database) FilterDatabaseTable(sourceSchema string, includeTableS, excludeTableS []string) (*structure.TableObjects, error) {
	startTime := time.Now()
	var (
		exporterTableSlice []string
		excludeTableSlice  []string
		err                error
	)

	allTables, err := d.GetDatabaseTable(sourceSchema)
	if err != nil {
		return nil, err
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
		return nil, fmt.Errorf("source config params include-table-s/exclude-table-s cannot exist at the same time")
	}

	if len(exporterTableSlice) == 0 {
		return nil, fmt.Errorf("exporter tables aren't exist, please check config params include-table-s/exclude-table-s")
	}

	endTime := time.Now()
	zap.L().Info("filter oracle database table",
		zap.String("schema", sourceSchema),
		zap.Strings("exporter tables list", exporterTableSlice),
		zap.Int("include table counts", len(exporterTableSlice)),
		zap.Int("exclude table counts", len(excludeTableSlice)),
		zap.Int("all table counts", len(allTables)),
		zap.String("cost", endTime.Sub(startTime).String()))

	return d.FilterDatabaseIncompatibleTable(sourceSchema, exporterTableSlice)
}

func (d *Database) FilterDatabaseIncompatibleTable(sourceSchema string, exporters []string) (*structure.TableObjects, error) {
	partitionTables, err := d.filterOraclePartitionTable(sourceSchema, exporters)
	if err != nil {
		return nil, fmt.Errorf("error on filter oracle compatible database partition table: %v", err)
	}
	temporaryTables, err := d.filterOracleTemporaryTable(sourceSchema, exporters)
	if err != nil {
		return nil, fmt.Errorf("error on filter oracle compatible database temporary table: %v", err)

	}
	clusteredTables, err := d.filterOracleClusteredTable(sourceSchema, exporters)
	if err != nil {
		return nil, fmt.Errorf("error on filter oracle compatible database clustered table: %v", err)

	}
	materializedView, err := d.filterOracleMaterializedView(sourceSchema, exporters)
	if err != nil {
		return nil, fmt.Errorf("error on filter oracle compatible database materialized view: %v", err)
	}
	externalTables, err := d.filterOracleExternalTable(sourceSchema, exporters)
	if err != nil {
		return nil, fmt.Errorf("error on filter oracle compatible database external table: %v", err)
	}
	normalViews, err := d.filterOracleNormalView(sourceSchema, exporters)
	if err != nil {
		return nil, fmt.Errorf("error on filter oracle compatible database normal view: %v", err)
	}
	compositeTables, err := d.filterOracleCompositeTypeTable(sourceSchema, exporters)
	if err != nil {
		return nil, fmt.Errorf("error on filter oracle compatible database composity table: %v", err)
	}

	if len(partitionTables) != 0 {
		zap.L().Warn("partition tables",
			zap.String("schema", sourceSchema),
			zap.Strings("partition table list", partitionTables),
			zap.String("suggest", "if necessary, please manually convert and process the tables in the above list"))
	}
	if len(temporaryTables) != 0 {
		zap.L().Warn("temporary tables",
			zap.String("schema", sourceSchema),
			zap.Strings("temporary table list", temporaryTables),
			zap.String("suggest", "if necessary, please manually process the tables in the above list"))
	}
	if len(clusteredTables) != 0 {
		zap.L().Warn("clustered tables",
			zap.String("schema", sourceSchema),
			zap.Strings("clustered table list", clusteredTables),
			zap.String("suggest", "if necessary, please manually process the tables in the above list"))
	}
	if len(externalTables) != 0 {
		zap.L().Warn("external tables",
			zap.String("schema", sourceSchema),
			zap.Strings("external table list", externalTables),
			zap.String("suggest", "if necessary, please manually process the tables in the above list"))
	}
	if len(normalViews) != 0 {
		// DBA_TABLES Not Records
		// VIEWS Records DBA_VIEWS
		zap.L().Warn("table views",
			zap.String("schema", sourceSchema),
			zap.Strings("table view list", normalViews),
			zap.String("suggest", "if necessary, please manually process the views in the above list"))
	}
	if len(compositeTables) != 0 {
		// DBA_TABLES Records
		zap.L().Warn("composite tables",
			zap.String("schema", sourceSchema),
			zap.Strings("composite table list", compositeTables),
			zap.String("suggest", "if necessary, please manually process the composite tables in the above list"))
	}

	var exporterTables []string
	if len(materializedView) != 0 {
		zap.L().Warn("materialized views",
			zap.String("schema", sourceSchema),
			zap.Strings("materialized view list", materializedView),
			zap.String("suggest", "if necessary, please manually process the tables in the above list"))
		// DBA_TABLES
		// exclude materialized view
		exporterTables = stringutil.StringItemsFilterDifference(exporters, materializedView)
	} else {
		exporterTables = exporters
	}

	return &structure.TableObjects{
		PartitionTables:   partitionTables,
		TemporaryTables:   temporaryTables,
		ClusteredTables:   clusteredTables,
		MaterializedViews: materializedView,
		ExternalTables:    externalTables,
		NormalViews:       normalViews,
		CompositeTables:   compositeTables,
		TaskTables:        exporterTables,
	}, nil
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

func (d *Database) filterOracleNormalView(sourceSchema string, exporters []string) ([]string, error) {
	tables, err := d.GetDatabaseNormalView(stringutil.StringUpper(sourceSchema))
	if err != nil {
		return nil, err
	}
	return stringutil.StringItemsFilterIntersection(exporters, tables), nil
}

func (d *Database) filterOracleExternalTable(sourceSchema string, exporters []string) ([]string, error) {
	tables, err := d.GetDatabaseExternalTable(stringutil.StringUpper(sourceSchema))
	if err != nil {
		return nil, err
	}
	return stringutil.StringItemsFilterIntersection(exporters, tables), nil
}

func (d *Database) filterOracleCompositeTypeTable(sourceSchema string, exporters []string) ([]string, error) {
	tables, err := d.GetDatabaseCompositeTypeTable(stringutil.StringUpper(sourceSchema))
	if err != nil {
		return nil, err
	}
	return stringutil.StringItemsFilterIntersection(exporters, tables), nil
}
