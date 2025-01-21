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
package taskflow

import (
	"fmt"
	"strings"

	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/model/buildin"
)

func AssessDatabaseOverview(databaseS database.IDatabase, objAssessCompsMap map[string]buildin.BuildinCompatibleRule, reportName, reportUser string) (*ReportOverview, ReportSummary, error) {

	dbName, platformID, platformName, err := databaseS.GetDatabasePlatformName()
	if err != nil {
		return nil, ReportSummary{}, err
	}

	globalName, err := databaseS.GetDatabaseGlobalName()
	if err != nil {
		return nil, ReportSummary{}, err
	}
	dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, err := databaseS.GetDatabaseParameters()
	if err != nil {
		return nil, ReportSummary{}, err
	}

	instanceRes, err := databaseS.GetDatabaseInstance()
	if err != nil {
		return nil, ReportSummary{}, err
	}

	dataSize, err := databaseS.GetDatabaseSize()
	if err != nil {
		return nil, ReportSummary{}, err
	}

	hostCPU, err := databaseS.GetDatabaseServerNumCPU()
	if err != nil {
		return nil, ReportSummary{}, err
	}
	memorySize, err := databaseS.GetDatabaseServerMemorySize()
	if err != nil {
		return nil, ReportSummary{}, err
	}

	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	if val, ok := objAssessCompsMap[stringutil.StringUpper(characterSet)]; ok {
		if strings.EqualFold(val.IsCompatible, constant.AssessYesCompatible) {
			assessComp += 1
		}
		if strings.EqualFold(val.IsCompatible, constant.AssessNoCompatible) {
			assessInComp += 1
		}
		if strings.EqualFold(val.IsConvertible, constant.AssessYesConvertible) {
			assessConvert += 1
		}
		if strings.EqualFold(val.IsConvertible, constant.AssessNoCompatible) {
			assessInConvert += 1
		}
	} else {
		assessInComp += 1
		assessConvert += 1
	}

	return &ReportOverview{
			ReportName:        reportName,
			ReportUser:        reportUser,
			HostName:          instanceRes[0]["HOST_NAME"],
			PlatformName:      fmt.Sprintf("%s/%s", platformName, platformID),
			DBName:            dbName,
			GlobalDBName:      globalName,
			ClusterDB:         clusterDatabase,
			ClusterDBInstance: CLusterDatabaseInstance,
			InstanceName:      instanceRes[0]["INSTANCE_NAME"],
			InstanceNumber:    instanceRes[0]["INSTANCE_NUMBER"],
			ThreadNumber:      instanceRes[0]["THREAD_NUMBER"],
			BlockSize:         dbBlockSize,
			TotalUsedSize:     dataSize,
			HostCPUS:          hostCPU,
			HostMem:           memorySize,
			CharacterSet:      characterSet},
		ReportSummary{
			AssessType:    constant.AssessTypeDatabaseOverview,
			AssessName:    constant.AssessNameDatabaseOverview,
			AssessTotal:   1,
			Compatible:    assessComp,
			Incompatible:  assessInComp,
			Convertible:   assessConvert,
			InConvertible: assessInConvert,
		}, nil
}

func AssessDatabaseSchemaTableTypeCompatible(databaseS database.IDatabase, schemaNames []string, objAssessCompsMap map[string]buildin.BuildinCompatibleRule) ([]SchemaTableTypeCompatibles, ReportSummary, error) {
	tableTypeCounts, err := databaseS.GetDatabaseSchemaTableTypeCounts(schemaNames)
	if err != nil {
		return nil, ReportSummary{}, err
	}
	if len(tableTypeCounts) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaTableTypeCompatibles

	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range tableTypeCounts {
		if val, ok := objAssessCompsMap[stringutil.StringUpper(ow["TABLE_TYPE"])]; ok {
			listData = append(listData, SchemaTableTypeCompatibles{
				Schema:        ow["SCHEMA_NAME"],
				TableType:     ow["TABLE_TYPE"],
				ObjectCounts:  ow["COUNTS"],
				ObjectSize:    ow["OBJECT_SIZE"],
				IsCompatible:  val.IsCompatible,
				IsConvertible: val.IsConvertible,
			})
			if strings.EqualFold(val.IsCompatible, constant.AssessYesCompatible) {
				assessComp += 1
			}
			if strings.EqualFold(val.IsCompatible, constant.AssessNoCompatible) {
				assessInComp += 1
			}
			if strings.EqualFold(val.IsConvertible, constant.AssessYesConvertible) {
				assessConvert += 1
			}
			if strings.EqualFold(val.IsConvertible, constant.AssessNoConvertible) {
				assessInConvert += 1
			}
		} else {
			listData = append(listData, SchemaTableTypeCompatibles{
				Schema:        ow["SCHEMA_NAME"],
				TableType:     ow["TABLE_TYPE"],
				ObjectCounts:  ow["COUNTS"],
				ObjectSize:    ow["OBJECT_SIZE"],
				IsCompatible:  constant.AssessNoCompatible,
				IsConvertible: constant.AssessNoConvertible,
			})

			assessInComp += 1
			assessInConvert += 1
		}
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCompatible,
		AssessName:    constant.AssessNameTableTypeCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseSchemaColumnTypeCompatible(databaseS database.IDatabase, schemaNames []string, buildinDatatypeMap map[string]buildin.BuildinDatatypeRule) ([]SchemaColumnTypeCompatibles, ReportSummary, error) {
	columnInfo, err := databaseS.GetDatabaseSchemaColumnTypeCounts(schemaNames)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(columnInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaColumnTypeCompatibles
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range columnInfo {
		if val, ok := buildinDatatypeMap[stringutil.StringUpper(ow["DATA_TYPE"])]; ok {
			listData = append(listData, SchemaColumnTypeCompatibles{
				Schema:        ow["OWNER"],
				ColumnType:    ow["DATA_TYPE"],
				ObjectCounts:  ow["COUNT"],
				MaxDataLength: ow["MAX_DATA_LENGTH"],
				ColumnTypeMap: val.DatatypeNameT,
				IsEquivalent:  constant.AssessYesEquivalent,
			})

			assessComp += 1
			assessConvert += 1
		} else {
			listData = append(listData, SchemaColumnTypeCompatibles{
				Schema:        ow["OWNER"],
				ColumnType:    ow["DATA_TYPE"],
				ObjectCounts:  ow["COUNT"],
				MaxDataLength: ow["MAX_DATA_LENGTH"],
				ColumnTypeMap: val.DatatypeNameT,
				IsEquivalent:  constant.AssessNoEquivalent,
			})

			assessInComp += 1
			assessInConvert += 1
		}
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCompatible,
		AssessName:    constant.AssessNameColumnTypeCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseSchemaConstraintTypeCompatible(databaseS database.IDatabase, schemaNames []string, objAssessCompsMap map[string]buildin.BuildinCompatibleRule) ([]SchemaConstraintTypeCompatibles, ReportSummary, error) {
	columnInfo, err := databaseS.GetDatabaseSchemaConstraintTypeCounts(schemaNames)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(columnInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaConstraintTypeCompatibles
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range columnInfo {
		if val, ok := objAssessCompsMap[stringutil.StringUpper(ow["CONSTRAINT_TYPE"])]; ok {
			listData = append(listData, SchemaConstraintTypeCompatibles{
				Schema:         ow["OWNER"],
				ConstraintType: ow["CONSTRAINT_TYPE"],
				ObjectCounts:   ow["COUNT"],
				IsCompatible:   val.IsCompatible,
				IsConvertible:  val.IsConvertible,
			})
			if strings.EqualFold(val.IsCompatible, constant.AssessYesCompatible) {
				assessComp += 1
			}
			if strings.EqualFold(val.IsCompatible, constant.AssessNoCompatible) {
				assessInComp += 1
			}
			if strings.EqualFold(val.IsConvertible, constant.AssessYesConvertible) {
				assessConvert += 1
			}
			if strings.EqualFold(val.IsConvertible, constant.AssessNoConvertible) {
				assessInConvert += 1
			}
		} else {
			listData = append(listData, SchemaConstraintTypeCompatibles{
				Schema:         ow["OWNER"],
				ConstraintType: ow["CONSTRAINT_TYPE"],
				ObjectCounts:   ow["COUNT"],
				IsCompatible:   constant.AssessNoCompatible,
				IsConvertible:  constant.AssessNoConvertible,
			})

			assessInComp += 1
			assessInConvert += 1
		}
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCompatible,
		AssessName:    constant.AssessNameConstraintTypeCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseSchemaIndexTypeCompatible(databaseS database.IDatabase, schemaNames []string, objAssessCompsMap map[string]buildin.BuildinCompatibleRule) ([]SchemaIndexTypeCompatibles, ReportSummary, error) {
	columnInfo, err := databaseS.GetDatabaseSchemaIndexTypeCounts(schemaNames)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(columnInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaIndexTypeCompatibles
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range columnInfo {
		if val, ok := objAssessCompsMap[stringutil.StringUpper(ow["INDEX_TYPE"])]; ok {
			listData = append(listData, SchemaIndexTypeCompatibles{
				Schema:        ow["TABLE_OWNER"],
				IndexType:     ow["INDEX_TYPE"],
				ObjectCounts:  ow["COUNT"],
				IsCompatible:  val.IsCompatible,
				IsConvertible: val.IsConvertible,
			})
			if strings.EqualFold(val.IsCompatible, constant.AssessYesCompatible) {
				assessComp += 1
			}
			if strings.EqualFold(val.IsCompatible, constant.AssessNoCompatible) {
				assessInComp += 1
			}
			if strings.EqualFold(val.IsConvertible, constant.AssessYesConvertible) {
				assessConvert += 1
			}
			if strings.EqualFold(val.IsConvertible, constant.AssessNoConvertible) {
				assessInConvert += 1
			}
		} else {
			listData = append(listData, SchemaIndexTypeCompatibles{
				Schema:        ow["TABLE_OWNER"],
				IndexType:     ow["INDEX_TYPE"],
				ObjectCounts:  ow["COUNT"],
				IsCompatible:  constant.AssessNoCompatible,
				IsConvertible: constant.AssessNoConvertible,
			})
			assessInComp += 1
			assessInConvert += 1
		}
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCompatible,
		AssessName:    constant.AssessNameIndexTypeCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseSchemaDefaultValue(databaseS database.IDatabase, schemaNames []string, defaultValueMap map[string]buildin.BuildinDefaultvalRule) ([]SchemaDefaultValueCompatibles, ReportSummary, error) {
	dataDefaults, err := databaseS.GetDatabaseSchemaColumnDataDefaultCounts(schemaNames)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(dataDefaults) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaDefaultValueCompatibles
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range dataDefaults {
		if val, ok := defaultValueMap[stringutil.StringUpper(ow["DATA_DEFAULT"])]; ok {
			listData = append(listData, SchemaDefaultValueCompatibles{
				Schema:             ow["OWNER"],
				ColumnDefaultValue: ow["DATA_DEFAULT"],
				ObjectCounts:       ow["COUNTS"],
				DefaultValueMap:    val.DefaultValueT,
				IsCompatible:       constant.AssessYesCompatible,
				IsConvertible:      constant.AssessYesConvertible,
			})

			assessComp += 1
			assessConvert += 1

		} else {
			listData = append(listData, SchemaDefaultValueCompatibles{
				Schema:             ow["OWNER"],
				ColumnDefaultValue: ow["DATA_DEFAULT"],
				ObjectCounts:       ow["COUNTS"],
				DefaultValueMap:    val.DefaultValueT,
				IsCompatible:       constant.AssessNoCompatible,
				IsConvertible:      constant.AssessNoConvertible,
			})

			assessInComp += 1
			assessInConvert += 1
		}
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCompatible,
		AssessName:    constant.AssessNameDefaultValueCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseSchemaViewTypeCompatible(databaseS database.IDatabase, schemaNames []string, objAssessCompsMap map[string]buildin.BuildinCompatibleRule) ([]SchemaViewTypeCompatibles, ReportSummary, error) {
	viewTypes, err := databaseS.GetDatabaseSchemaViewTypeCounts(schemaNames)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(viewTypes) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaViewTypeCompatibles
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range viewTypes {
		if val, ok := objAssessCompsMap[stringutil.StringUpper(ow["VIEW_TYPE"])]; ok {
			listData = append(listData, SchemaViewTypeCompatibles{
				Schema:        ow["OWNER"],
				ViewType:      ow["VIEW_TYPE"],
				ViewTypeOwner: ow["VIEW_TYPE_OWNER"],
				ObjectCounts:  ow["COUNTS"],
				IsCompatible:  val.IsCompatible,
				IsConvertible: val.IsConvertible,
			})
			if strings.EqualFold(val.IsCompatible, constant.AssessYesCompatible) {
				assessComp += 1
			}
			if strings.EqualFold(val.IsCompatible, constant.AssessNoCompatible) {
				assessInComp += 1
			}
			if strings.EqualFold(val.IsConvertible, constant.AssessYesConvertible) {
				assessConvert += 1
			}
			if strings.EqualFold(val.IsConvertible, constant.AssessNoConvertible) {
				assessInConvert += 1
			}
		} else {
			listData = append(listData, SchemaViewTypeCompatibles{
				Schema:        ow["OWNER"],
				ViewType:      ow["VIEW_TYPE"],
				ObjectCounts:  ow["COUNTS"],
				IsCompatible:  constant.AssessNoCompatible,
				IsConvertible: constant.AssessNoConvertible,
			})
			assessInComp += 1
			assessInConvert += 1
		}
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCompatible,
		AssessName:    constant.AssessNameViewTypeCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseSchemaObjectTypeCompatible(databaseS database.IDatabase, schemaNames []string, objAssessCompsMap map[string]buildin.BuildinCompatibleRule) ([]SchemaObjectTypeCompatibles, ReportSummary, error) {
	codeInfo, err := databaseS.GetDatabaseSchemaObjectTypeCounts(schemaNames)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(codeInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaObjectTypeCompatibles
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range codeInfo {
		if val, ok := objAssessCompsMap[stringutil.StringUpper(ow["OBJECT_TYPE"])]; ok {
			listData = append(listData, SchemaObjectTypeCompatibles{
				Schema:        ow["OWNER"],
				ObjectType:    ow["OBJECT_TYPE"],
				ObjectCounts:  ow["COUNTS"],
				IsCompatible:  val.IsCompatible,
				IsConvertible: val.IsConvertible,
			})
			if strings.EqualFold(val.IsCompatible, constant.AssessYesCompatible) {
				assessComp += 1
			}
			if strings.EqualFold(val.IsCompatible, constant.AssessNoCompatible) {
				assessInComp += 1
			}
			if strings.EqualFold(val.IsConvertible, constant.AssessYesConvertible) {
				assessConvert += 1
			}
			if strings.EqualFold(val.IsConvertible, constant.AssessNoConvertible) {
				assessInConvert += 1
			}
		} else {
			listData = append(listData, SchemaObjectTypeCompatibles{
				Schema:        ow["OWNER"],
				ObjectType:    ow["OBJECT_TYPE"],
				ObjectCounts:  ow["COUNTS"],
				IsCompatible:  constant.AssessNoCompatible,
				IsConvertible: constant.AssessYesConvertible,
			})
			assessInComp += 1
			assessConvert += 1
		}
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCompatible,
		AssessName:    constant.AssessNameObjectTypeCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseSchemaPartitionTypeCompatible(databaseS database.IDatabase, schemaNames []string, objAssessCompsMap map[string]buildin.BuildinCompatibleRule) ([]SchemaPartitionTypeCompatibles, ReportSummary, error) {
	partitionInfo, err := databaseS.GetDatabaseSchemaPartitionTypeCounts(schemaNames)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(partitionInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaPartitionTypeCompatibles
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range partitionInfo {
		if val, ok := objAssessCompsMap[stringutil.StringUpper(ow["PARTITIONING_TYPE"])]; ok {
			listData = append(listData, SchemaPartitionTypeCompatibles{
				Schema:        ow["OWNER"],
				PartitionType: ow["PARTITIONING_TYPE"],
				ObjectCounts:  ow["COUNTS"],
				IsCompatible:  val.IsCompatible,
				IsConvertible: val.IsConvertible,
			})
			if strings.EqualFold(val.IsCompatible, constant.AssessYesCompatible) {
				assessComp += 1
			}
			if strings.EqualFold(val.IsCompatible, constant.AssessNoCompatible) {
				assessInComp += 1
			}
			if strings.EqualFold(val.IsConvertible, constant.AssessYesConvertible) {
				assessConvert += 1
			}
			if strings.EqualFold(val.IsConvertible, constant.AssessNoConvertible) {
				assessInConvert += 1
			}
		} else {
			listData = append(listData, SchemaPartitionTypeCompatibles{
				Schema:        ow["OWNER"],
				PartitionType: ow["PARTITIONING_TYPE"],
				ObjectCounts:  ow["COUNTS"],
				IsCompatible:  constant.AssessNoCompatible,
				IsConvertible: constant.AssessYesConvertible,
			})
			assessInComp += 1
			assessConvert += 1
		}
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCompatible,
		AssessName:    constant.AssessNamePartitionTypeCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseSchemaSubPartitionTypeCompatible(databaseS database.IDatabase, schemaNames []string, objAssessCompsMap map[string]buildin.BuildinCompatibleRule) ([]SchemaSubPartitionTypeCompatibles, ReportSummary, error) {
	partitionInfo, err := databaseS.GetDatabaseSchemaSubPartitionTypeCounts(schemaNames)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(partitionInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaSubPartitionTypeCompatibles
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range partitionInfo {
		if val, ok := objAssessCompsMap[stringutil.StringUpper(ow["SUBPARTITIONING_TYPE"])]; ok {
			listData = append(listData, SchemaSubPartitionTypeCompatibles{
				Schema:           ow["OWNER"],
				SubPartitionType: ow["SUBPARTITIONING_TYPE"],
				ObjectCounts:     ow["COUNTS"],
				IsCompatible:     val.IsCompatible,
				IsConvertible:    val.IsConvertible,
			})
			if strings.EqualFold(val.IsCompatible, constant.AssessYesCompatible) {
				assessComp += 1
			}
			if strings.EqualFold(val.IsCompatible, constant.AssessNoCompatible) {
				assessInComp += 1
			}
			if strings.EqualFold(val.IsConvertible, constant.AssessYesConvertible) {
				assessConvert += 1
			}
			if strings.EqualFold(val.IsConvertible, constant.AssessNoConvertible) {
				assessInConvert += 1
			}
		} else {
			listData = append(listData, SchemaSubPartitionTypeCompatibles{
				Schema:           ow["OWNER"],
				SubPartitionType: ow["SUBPARTITIONING_TYPE"],
				ObjectCounts:     ow["COUNTS"],
				IsCompatible:     constant.AssessNoCompatible,
				IsConvertible:    constant.AssessYesConvertible,
			})
			assessInComp += 1
			assessConvert += 1
		}
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCompatible,
		AssessName:    constant.AssessNameSubPartitionTypeCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseSchemaTemporaryTableCompatible(databaseS database.IDatabase, schemaNames []string, objAssessCompsMap map[string]buildin.BuildinCompatibleRule) ([]SchemaTemporaryTableTypeCompatibles, ReportSummary, error) {

	synonymInfo, err := databaseS.GetDatabaseSchemaTemporaryTableTypeCounts(schemaNames)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaTemporaryTableTypeCompatibles
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		if val, ok := objAssessCompsMap[stringutil.StringUpper(ow["TEMP_TYPE"])]; ok {
			listData = append(listData, SchemaTemporaryTableTypeCompatibles{
				Schema:             ow["OWNER"],
				TemporaryTableType: ow["TEMP_TYPE"],
				ObjectCounts:       ow["COUNTS"],
				IsCompatible:       val.IsCompatible,
				IsConvertible:      val.IsConvertible,
			})
			if strings.EqualFold(val.IsCompatible, constant.AssessYesCompatible) {
				assessComp += 1
			}
			if strings.EqualFold(val.IsCompatible, constant.AssessNoCompatible) {
				assessInComp += 1
			}
			if strings.EqualFold(val.IsConvertible, constant.AssessYesConvertible) {
				assessConvert += 1
			}
			if strings.EqualFold(val.IsConvertible, constant.AssessNoConvertible) {
				assessInConvert += 1
			}
		} else {
			listData = append(listData, SchemaTemporaryTableTypeCompatibles{
				Schema:             ow["OWNER"],
				TemporaryTableType: ow["TEMP_TYPE"],
				ObjectCounts:       ow["COUNTS"],
				IsCompatible:       constant.AssessNoCompatible,
				IsConvertible:      constant.AssessYesConvertible,
			})
			assessInComp += 1
			assessConvert += 1
		}
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCompatible,
		AssessName:    constant.AssessNameTemporaryTableTypeCompatible,
		AssessTotal:   len(listData),
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

/*
Oracle Database Check
*/
func AssessDatabasePartitionTableCountsCheck(databaseS database.IDatabase, schemaNames []string, limit int) ([]SchemaPartitionTableCountsCheck, ReportSummary, error) {
	synonymInfo, err := databaseS.GetDatabaseSchemaPartitionTableCountsOverLimit(schemaNames, limit)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaPartitionTableCountsCheck
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, SchemaPartitionTableCountsCheck{
			Schema:          ow["OWNER"],
			TableName:       ow["TABLE_NAME"],
			PartitionCounts: ow["PARTITION_COUNT"],
		})
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCheck,
		AssessName:    constant.AssessNamePartitionTableCountsCheck,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseTableRowLengthMBCheck(databaseS database.IDatabase, schemaNames []string, limitMB int) ([]SchemaTableRowLengthCheck, ReportSummary, error) {
	synonymInfo, err := databaseS.GetDatabaseSchemaTableAvgRowLengthOverLimitMB(schemaNames, limitMB)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaTableRowLengthCheck
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, SchemaTableRowLengthCheck{
			Schema:       ow["OWNER"],
			TableName:    ow["TABLE_NAME"],
			AvgRowLength: ow["AVG_ROW_LEN"],
		})
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCheck,
		AssessName:    constant.AssessNameTableRowLengthCheck,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseTableIndexRowLengthCheck(databaseS database.IDatabase, schemaNames []string, limit int) ([]SchemaTableIndexRowLengthCheck, ReportSummary, error) {
	synonymInfo, err := databaseS.GetDatabaseSchemaTableIndexLengthOverLimit(schemaNames, limit)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaTableIndexRowLengthCheck
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, SchemaTableIndexRowLengthCheck{
			Schema:       ow["INDEX_OWNER"],
			TableName:    ow["TABLE_NAME"],
			IndexName:    ow["INDEX_NAME"],
			ColumnLength: ow["LENGTH_OVER"],
		})
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCheck,
		AssessName:    constant.AssessNameIndexRowLengthCheck,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseTableColumnCountsCheck(databaseS database.IDatabase, schemaNames []string, limit int) ([]SchemaTableColumnCountsCheck, ReportSummary, error) {
	synonymInfo, err := databaseS.GetDatabaseSchemaTableColumnCountsOverLimit(schemaNames, limit)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaTableColumnCountsCheck
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, SchemaTableColumnCountsCheck{
			Schema:       ow["OWNER"],
			TableName:    ow["TABLE_NAME"],
			ColumnCounts: ow["COUNT_OVER"],
		})
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCheck,
		AssessName:    constant.AssessNameTableColumnCountsCheck,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseTableIndexCountsCheck(databaseS database.IDatabase, schemaNames []string, limit int) ([]SchemaTableIndexCountsCheck, ReportSummary, error) {
	synonymInfo, err := databaseS.GetDatabaseSchemaTableIndexCountsOverLimit(schemaNames, limit)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaTableIndexCountsCheck
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, SchemaTableIndexCountsCheck{
			Schema:      ow["TABLE_OWNER"],
			TableName:   ow["TABLE_NAME"],
			IndexCounts: ow["COUNT_OVER"],
		})
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCheck,
		AssessName:    constant.AssessNameTableIndexCountsCheck,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseUsernameLengthCheck(databaseS database.IDatabase, schemaNames []string, limit int) ([]UsernameLengthCheck, ReportSummary, error) {
	synonymInfo, err := databaseS.GetDatabaseUsernameLengthOverLimit(schemaNames, limit)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []UsernameLengthCheck
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, UsernameLengthCheck{
			Schema:        ow["USERNAME"],
			AccountStatus: ow["ACCOUNT_STATUS"],
			Created:       ow["CREATED"],
			Length:        ow["LENGTH_OVER"],
		})
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCheck,
		AssessName:    constant.AssessNameUsernameLengthCheck,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseTableNameLengthCheck(databaseS database.IDatabase, schemaNames []string, limit int) ([]SchemaTableNameLengthCheck, ReportSummary, error) {
	synonymInfo, err := databaseS.GetDatabaseSchemaTableNameLengthOverLimit(schemaNames, limit)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaTableNameLengthCheck
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, SchemaTableNameLengthCheck{
			Schema:    ow["OWNER"],
			TableName: ow["TABLE_NAME"],
			Length:    ow["LENGTH_OVER"],
		})
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCheck,
		AssessName:    constant.AssessNameTableNameLengthCheck,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseColumnNameLengthCheck(databaseS database.IDatabase, schemaNames []string, limit int) ([]SchemaTableColumnNameLengthCheck, ReportSummary, error) {
	synonymInfo, err := databaseS.GetDatabaseSchemaTableColumnNameLengthOverLimit(schemaNames, limit)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaTableColumnNameLengthCheck
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, SchemaTableColumnNameLengthCheck{
			Schema:     ow["OWNER"],
			TableName:  ow["TABLE_NAME"],
			ColumnName: ow["COLUMN_NAME"],
			Length:     ow["LENGTH_OVER"],
		})
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCheck,
		AssessName:    constant.AssessNameColumnNameLengthCheck,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseIndexNameLengthCheck(databaseS database.IDatabase, schemaNames []string, limit int) ([]SchemaTableIndexNameLengthCheck, ReportSummary, error) {
	synonymInfo, err := databaseS.GetDatabaseSchemaTableIndexNameLengthOverLimit(schemaNames, limit)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaTableIndexNameLengthCheck
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0
	for _, ow := range synonymInfo {
		listData = append(listData, SchemaTableIndexNameLengthCheck{
			Schema:    ow["INDEX_OWNER"],
			TableName: ow["TABLE_NAME"],
			IndexName: ow["INDEX_NAME"],
			Length:    ow["LENGTH_OVER"],
		})
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCheck,
		AssessName:    constant.AssessNameIndexNameLengthCheck,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseViewNameLengthCheck(databaseS database.IDatabase, schemaNames []string, limit int) ([]SchemaViewNameLengthCheck, ReportSummary, error) {
	synonymInfo, err := databaseS.GetDatabaseSchemaTableViewNameLengthOverLimit(schemaNames, limit)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaViewNameLengthCheck
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, SchemaViewNameLengthCheck{
			Schema:   ow["OWNER"],
			ViewName: ow["VIEW_NAME"],
			ReadOnly: ow["READ_ONLY"],
			Length:   ow["LENGTH_OVER"],
		})
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCheck,
		AssessName:    constant.AssessNameViewNameLengthCheck,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseSequenceNameLengthCheck(databaseS database.IDatabase, schemaNames []string, limit int) ([]SchemaSequenceNameLengthCheck, ReportSummary, error) {

	synonymInfo, err := databaseS.GetDatabaseSchemaTableSequenceNameLengthOverLimit(schemaNames, limit)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaSequenceNameLengthCheck
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, SchemaSequenceNameLengthCheck{
			Schema:       ow["SEQUENCE_OWNER"],
			SequenceName: ow["SEQUENCE_NAME"],
			OrderFlag:    ow["ORDER_FLAG"],
			Length:       ow["LENGTH_OVER"],
		})
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeCheck,
		AssessName:    constant.AssessNameSequenceNameLengthCheck,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

/*
Oracle Database Reference
*/
func AssessDatabaseSchemaOverview(databaseS database.IDatabase, schemaNames []string) ([]SchemaTableSizeData, ReportSummary, error) {
	overview, err := databaseS.GetDatabaseSchemaTableIndexOverview(schemaNames)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(overview) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaTableSizeData
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range overview {
		listData = append(listData, SchemaTableSizeData{
			Schema:        ow["SCHEMA"],
			TableSize:     ow["TABLE"],
			IndexSize:     ow["INDEX"],
			LobTableSize:  ow["LOBTABLE"],
			LobIndexSize:  ow["LOBINDEX"],
			AllTablesRows: ow["ROWCOUNT"],
		})
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeRelated,
		AssessName:    constant.AssessNameSchemaDataSizeRelated,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseMaxActiveSessionCount(databaseS database.IDatabase, schemaNames []string) ([]SchemaActiveSession, ReportSummary, error) {
	listActiveSession, err := databaseS.GetDatabaseSessionMaxActiveCount()
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(listActiveSession) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaActiveSession
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range listActiveSession {
		listData = append(listData, SchemaActiveSession{
			Rownum:         ow["ROWNUM"],
			DBID:           ow["DBID"],
			InstanceNumber: ow["INSTANCE_NUMBER"],
			SampleID:       ow["SAMPLE_ID"],
			SampleTime:     ow["SAMPLE_TIME"],
			SessionCounts:  ow["SESSION_COUNT"],
		})
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeRelated,
		AssessName:    constant.AssessNameSchemaActiveSessionRelated,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseSchemaTableRowsTOP(databaseS database.IDatabase, schemaNames []string, top int) ([]SchemaTableRowsTOP, ReportSummary, error) {
	overview, err := databaseS.GetDatabaseSchemaTableRowsTOP(schemaNames, top)
	if err != nil {
		return nil, ReportSummary{}, err
	}
	if len(overview) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaTableRowsTOP
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range overview {
		listData = append(listData, SchemaTableRowsTOP{
			Schema:    ow["SCHEMA"],
			TableName: ow["SEGMENT_NAME"],
			TableType: ow["SEGMENT_TYPE"],
			TableSize: ow["TABLE_SIZE"],
		})
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeRelated,
		AssessName:    constant.AssessNameSchemaTableRowsTopRelated,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseSchemaCodeOverview(databaseS database.IDatabase, schemaNames []string) ([]SchemaCodeObject, ReportSummary, error) {
	overview, err := databaseS.GetDatabaseSchemaCodeObject(schemaNames)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(overview) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaCodeObject
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range overview {
		listData = append(listData, SchemaCodeObject{
			Schema:     ow["OWNER"],
			ObjectName: ow["NAME"],
			ObjectType: ow["TYPE"],
			Lines:      ow["LINES"],
		})

	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeRelated,
		AssessName:    constant.AssessNameSchemaCodeObjectRelated,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseSchemaSynonymOverview(databaseS database.IDatabase, schemaNames []string) ([]SchemaSynonymObject, ReportSummary, error) {
	overview, err := databaseS.GetDatabaseSchemaCodeObject(schemaNames)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(overview) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaSynonymObject
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range overview {
		listData = append(listData, SchemaSynonymObject{
			Schema:      ow["OWNER"],
			SynonymName: ow["SYNONYM_NAME"],
			TableOwner:  ow["TABLE_OWNER"],
			TableName:   ow["TABLE_NAME"],
		})

	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeRelated,
		AssessName:    constant.AssessNameSchemaSynonymObjectRelated,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseSchemaMaterializedViewOverview(databaseS database.IDatabase, schemaNames []string) ([]SchemaMaterializedViewObject, ReportSummary, error) {
	overview, err := databaseS.GetDatabaseSchemaMaterializedViewObject(schemaNames)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(overview) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaMaterializedViewObject
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range overview {
		listData = append(listData, SchemaMaterializedViewObject{
			Schema:            ow["OWNER"],
			MviewName:         ow["MVIEW_NAME"],
			RewriteCapability: ow["REWRITE_CAPABILITY"],
			RefreshMode:       ow["REFRESH_MODE"],
			RefreshMethod:     ow["REFRESH_METHOD"],
			FastRefreshable:   ow["FAST_REFRESHABLE"],
		})

	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeRelated,
		AssessName:    constant.AssessNameSchemaMaterializedViewRelated,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseSchemaTableAvgRowLengthTOP(databaseS database.IDatabase, schemaNames []string, top int) ([]SchemaTableAvgRowLengthTOP, ReportSummary, error) {
	synonymInfo, err := databaseS.GetDatabaseSchemaTableAvgRowLengthTOP(schemaNames, top)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaTableAvgRowLengthTOP
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, SchemaTableAvgRowLengthTOP{
			Schema:       ow["OWNER"],
			TableName:    ow["TABLE_NAME"],
			AvgRowLength: ow["AVG_ROW_LEN"],
		})
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeRelated,
		AssessName:    constant.AssessNameSchemaTableAvgRowLengthTopRelated,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}

func AssessDatabaseSchemaTableNumberTypeEqualZero(databaseS database.IDatabase, schemaNames []string, specifyDatatype string) ([]SchemaTableNumberTypeEqualZero, ReportSummary, error) {
	synonymInfo, err := databaseS.GetDatabaseSchemaTableSpecialDatatype(schemaNames, specifyDatatype)
	if err != nil {
		return nil, ReportSummary{}, err
	}

	if len(synonymInfo) == 0 {
		return nil, ReportSummary{}, nil
	}

	var listData []SchemaTableNumberTypeEqualZero
	assessTotal := 0
	assessComp := 0
	assessInComp := 0
	assessConvert := 0
	assessInConvert := 0

	for _, ow := range synonymInfo {
		listData = append(listData, SchemaTableNumberTypeEqualZero{
			Schema:        ow["OWNER"],
			TableName:     ow["TABLE_NAME"],
			ColumnName:    ow["COLUMN_NAME"],
			DataPrecision: ow["DATA_PRECISION"],
			DataScale:     ow["DATA_SCALE"],
		})
	}

	return listData, ReportSummary{
		AssessType:    constant.AssessTypeObjectTypeRelated,
		AssessName:    constant.AssessNameSchemaTableNumberTypeEqual0,
		AssessTotal:   assessTotal,
		Compatible:    assessComp,
		Incompatible:  assessInComp,
		Convertible:   assessConvert,
		InConvertible: assessInConvert,
	}, nil
}
