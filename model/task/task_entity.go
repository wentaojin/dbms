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
package task

import (
	"time"

	"github.com/wentaojin/dbms/model/common"
)

type Task struct {
	ID              uint64     `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName        string     `gorm:"type:varchar(100);not null;uniqueIndex:uniq_task_name;comment:task name" json:"taskName"`
	TaskMode        string     `gorm:"type:varchar(100);not null;comment:task mode" json:"taskMode"`
	TaskFlow        string     `gorm:"type:varchar(100);not null;comment:task flow" json:"taskFlow"`
	DatasourceNameS string     `gorm:"type:varchar(300);not null;comment:source datasource of task" json:"datasourceNameS"`
	DatasourceNameT string     `gorm:"type:varchar(300);not null;comment:target datasource of task" json:"datasourceNameT"`
	WorkerAddr      string     `gorm:"type:varchar(30);comment:worker addr" json:"workerAddr"`
	CaseFieldRuleS  string     `gorm:"type:varchar(30);comment:source case field rule" json:"caseFieldRuleS"`
	CaseFieldRuleT  string     `gorm:"type:varchar(30);comment:target case field rule" json:"CaseFieldRuleT"`
	TaskStatus      string     `gorm:"type:varchar(30);comment:task status" json:"taskStatus"`
	StartTime       *time.Time `gorm:"default:null;comment:task start running time" json:"startTime"`
	EndTime         *time.Time `gorm:"default:null;comment:task end running time" json:"endTime"`
	*common.Entity
}

type Log struct {
	ID          uint64 `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName    string `gorm:"type:varchar(100);not null;index:idx_task_name;comment:task name" json:"taskName"`
	SchemaNameS string `gorm:"type:varchar(60);index:schema_table_name;comment:source schema name" json:"schemaNameS"`
	TableNameS  string `gorm:"type:varchar(60);index:schema_table_name;comment:source table name" json:"tableNameS"`
	LogDetail   string `gorm:"type:longtext;comment:task running log" json:"logDetail"`
	*common.Entity
}

type StructMigrateSummary struct {
	ID           uint64  `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName     string  `gorm:"type:varchar(100);not null;uniqueIndex:uniq_schema_table_name_complex;index:idx_task_name;comment:task name" json:"taskName"`
	SchemaNameS  string  `gorm:"type:varchar(60);not null;uniqueIndex:uniq_schema_table_name_complex;comment:source schema name" json:"schemaNameS"`
	TableTotals  uint64  `gorm:"type:int;comment:source table table totals" json:"tableTotals"`
	TableSuccess uint64  `gorm:"type:int;comment:source table table success" json:"tableSuccess"`
	TableFails   uint64  `gorm:"type:int;comment:source table table fails" json:"tableFails"`
	TableWaits   uint64  `gorm:"type:int;comment:source table table waits" json:"tableWaits"`
	TableRuns    uint64  `gorm:"type:int;comment:source table table runs" json:"tableRuns"`
	TableStops   uint64  `gorm:"type:int;comment:source table table stops" json:"tableStops"`
	Duration     float64 `gorm:"comment:run duration, size: seconds" json:"duration"`
	*common.Entity
}

type StructMigrateTask struct {
	ID              uint64  `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName        string  `gorm:"type:varchar(100);not null;uniqueIndex:uniq_schema_table_name;index:idx_task_name;comment:task name" json:"taskName"`
	SchemaNameS     string  `gorm:"type:varchar(60);not null;uniqueIndex:uniq_schema_table_name;comment:source schema name" json:"schemaNameS"`
	TableNameS      string  `gorm:"type:varchar(60);uniqueIndex:uniq_schema_table_name;comment:source table name" json:"tableNameS"`
	TableTypeS      string  `gorm:"type:varchar(60);comment:source table type" json:"tableTypeS"`
	SchemaNameT     string  `gorm:"type:varchar(60);comment:target schema name" json:"schemaNameT"`
	TableNameT      string  `gorm:"type:varchar(60);comment:target table name" json:"tableNameT"`
	TaskStatus      string  `gorm:"type:varchar(50);not null;comment:task run status" json:"taskStatus"`
	SourceSqlDigest string  `gorm:"type:longtext;comment:origin sql digest" json:"sourceSqlDigest"`
	IncompSqlDigest string  `gorm:"type:longtext;comment:incompatible sql digest" json:"incompSqlDigest"`
	TargetSqlDigest string  `gorm:"type:longtext;comment:target sql digest" json:"targetSqlDigest"`
	TableAttrOption string  `gorm:"type:varchar(300);comment:source column datatype" json:"tableAttrOption"`
	ErrorDetail     string  `gorm:"type:longtext;comment:error detail" json:"errorDetail"`
	IsSchemaCreate  string  `gorm:"type:varchar(10);default:NO;comment:is schema create sql" json:"isSchemaCreate"`
	Duration        float64 `gorm:"comment:run duration, size: seconds" json:"duration"`
	*common.Entity
}

type DataMigrateSummary struct {
	ID           uint64  `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName     string  `gorm:"type:varchar(100);not null;uniqueIndex:uniq_schema_table_name_complex;index:idx_task_name;comment:task name" json:"taskName"`
	SchemaNameS  string  `gorm:"type:varchar(60);not null;uniqueIndex:uniq_schema_table_name_complex;comment:source schema name" json:"schemaNameS"`
	TableNameS   string  `gorm:"type:varchar(60);uniqueIndex:uniq_schema_table_name_complex;comment:source table name" json:"tableNameS"`
	SchemaNameT  string  `gorm:"type:varchar(60);not null;comment:target schema name" json:"schemaNameT"`
	TableNameT   string  `gorm:"type:varchar(60);not null;comment:target table name" json:"tableNameT"`
	GlobalScnS   uint64  `gorm:"type:bigint;comment:source global scn" json:"globalScnS"`
	TableRowsS   uint64  `gorm:"comment:source table rows" json:"tableRowsS"`
	TableSizeS   float64 `gorm:"comment:source table size (MB)" json:"tableSizeS"`
	ChunkTotals  uint64  `gorm:"type:int;comment:source table chunk totals" json:"chunkTotals"`
	ChunkSuccess uint64  `gorm:"type:int;comment:source table chunk success" json:"chunkSuccess"`
	ChunkFails   uint64  `gorm:"type:int;comment:source table chunk fails" json:"chunkFails"`
	ChunkWaits   uint64  `gorm:"type:int;comment:source table chunk waits" json:"chunkWaits"`
	ChunkRuns    uint64  `gorm:"type:int;comment:source table chunk runs" json:"chunkRuns"`
	ChunkStops   uint64  `gorm:"type:int;comment:source table chunk stops" json:"chunkStops"`
	Refused      string  `gorm:"type:text;comment:csv migrate table refused" json:"refused"`
	Duration     float64 `gorm:"comment:run duration, size: seconds" json:"duration"`
	*common.Entity
}

type DataMigrateTask struct {
	ID              uint64  `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName        string  `gorm:"type:varchar(100);not null;uniqueIndex:uniq_schema_table_name_complex;index:idx_task_name;comment:task name" json:"taskName"`
	SchemaNameS     string  `gorm:"type:varchar(60);not null;uniqueIndex:uniq_schema_table_name_complex;comment:source schema name" json:"schemaNameS"`
	TableNameS      string  `gorm:"type:varchar(60);not null;uniqueIndex:uniq_schema_table_name_complex;comment:source table name" json:"tableNameS"`
	SchemaNameT     string  `gorm:"type:varchar(60);not null;comment:target schema name" json:"schemaNameT"`
	TableNameT      string  `gorm:"type:varchar(60);not null;comment:target table name" json:"tableNameT"`
	TableTypeS      string  `gorm:"type:varchar(60);comment:source table type" json:"tableTypeS"`
	GlobalScnS      uint64  `gorm:"type:bigint;comment:source global scn" json:"globalScnS"`
	ColumnDetailO   string  `gorm:"type:longtext;comment:source column store origin information" json:"columnDetailO"`
	ColumnDetailS   string  `gorm:"type:longtext;comment:source column used to query information" json:"columnDetailS"`
	ColumnDetailT   string  `gorm:"type:longtext;comment:source column used to query information" json:"columnDetailT"`
	SqlHintS        string  `gorm:"type:varchar(300);comment:source sql hint" json:"sqlHintS"`
	SqlHintT        string  `gorm:"type:varchar(300);comment:target sql hint" json:"sqlHintT"`
	ChunkDetailS    string  `gorm:"type:varchar(300);not null;uniqueIndex:uniq_schema_table_name_complex;comment:source table chunk detail" json:"chunkDetailS"`
	ConsistentReadS string  `gorm:"type:varchar(10);comment:source sql consistent read" json:"consistentReadS"`
	TaskStatus      string  `gorm:"type:varchar(50);not null;comment:task run status" json:"taskStatus"`
	CsvFile         string  `gorm:"type:varchar(300);comment:csv exporter file path" json:"csvFile"`
	ErrorDetail     string  `gorm:"type:longtext;comment:error detail" json:"errorDetail"`
	Duration        float64 `gorm:"comment:run duration, size: seconds" json:"duration"`
	*common.Entity
}

type SqlMigrateSummary struct {
	ID         uint64  `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName   string  `gorm:"type:varchar(100);not null;uniqueIndex:uniq_schema_table_complex;comment:task name" json:"taskName"`
	SqlTotals  uint64  `gorm:"type:int;comment:source table sql totals" json:"sqlTotals"`
	SqlSuccess uint64  `gorm:"type:int;comment:source table sql success" json:"sqlSuccess"`
	SqlFails   uint64  `gorm:"type:int;comment:source table sql fails" json:"sqlFails"`
	SqlWaits   uint64  `gorm:"type:int;comment:source table sql waits" json:"sqlWaits"`
	SqlRuns    uint64  `gorm:"type:int;comment:source table sql runs" json:"sqlRuns"`
	SqlStops   uint64  `gorm:"type:int;comment:source table sql stops" json:"sqlStops"`
	Duration   float64 `gorm:"comment:run duration, size: seconds" json:"duration"`
	*common.Entity
}

type SqlMigrateTask struct {
	ID              uint64  `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName        string  `gorm:"type:varchar(100);not null;uniqueIndex:uniq_schema_table_name_complex;" json:"taskName"`
	SchemaNameT     string  `gorm:"type:varchar(60);not null;uniqueIndex:uniq_schema_table_name_complex;comment:target schema name" json:"schemaNameT"`
	TableNameT      string  `gorm:"type:varchar(60);not null;uniqueIndex:uniq_schema_table_name_complex;comment:target table name" json:"tableNameT"`
	GlobalScnS      uint64  `gorm:"type:bigint;comment:source global scn" json:"globalScnS"`
	ColumnDetailO   string  `gorm:"type:longtext;comment:source column store origin information" json:"columnDetailO"`
	ColumnDetailS   string  `gorm:"type:longtext;comment:source column information" json:"columnDetailS"`
	ColumnDetailT   string  `gorm:"type:longtext;comment:source column information" json:"columnDetailT"`
	SqlHintT        string  `gorm:"type:varchar(300);comment:target sql hint" json:"sqlHintT"`
	ConsistentReadS string  `gorm:"type:varchar(10);comment:source sql consistent read" json:"consistentReadS"`
	SqlQueryS       string  `gorm:"type:varchar(300);not null;uniqueIndex:uniq_schema_table_name_complex;comment:source sql query" json:"sqlQueryS"`
	TaskStatus      string  `gorm:"type:varchar(50);not null;comment:task run status" json:"taskStatus"`
	ErrorDetail     string  `gorm:"type:longtext;comment:error detail" json:"errorDetail"`
	Duration        float64 `gorm:"comment:run duration, size: seconds" json:"duration"`
	*common.Entity
}

type DataCompareSummary struct {
	ID             uint64  `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName       string  `gorm:"type:varchar(100);not null;uniqueIndex:uniq_schema_table_name_complex;index:idx_task_name;comment:task name" json:"taskName"`
	SchemaNameS    string  `gorm:"type:varchar(60);not null;uniqueIndex:uniq_schema_table_name_complex;comment:source schema name" json:"schemaNameS"`
	TableNameS     string  `gorm:"type:varchar(60);uniqueIndex:uniq_schema_table_name_complex;comment:source table name" json:"tableNameS"`
	SchemaNameT    string  `gorm:"type:varchar(60);not null;comment:target schema name" json:"schemaNameT"`
	TableNameT     string  `gorm:"type:varchar(60);not null;comment:target table name" json:"tableNameT"`
	GlobalScnS     uint64  `gorm:"type:bigint;comment:source global scn" json:"globalScnS"`
	TableRowsS     uint64  `gorm:"comment:source table rows" json:"tableRowsS"`
	TableSizeS     float64 `gorm:"comment:source table size (MB)" json:"tableSizeS"`
	ChunkTotals    uint64  `gorm:"type:int;comment:source table chunk totals" json:"chunkTotals"`
	ChunkEquals    uint64  `gorm:"type:int;comment:source table chunk equals" json:"chunkEquals"`
	ChunkNotEquals uint64  `gorm:"type:int;comment:source table chunk not equals" json:"chunkNotEquals"`
	ChunkFails     uint64  `gorm:"type:int;comment:source table chunk fails" json:"chunkFails"`
	ChunkWaits     uint64  `gorm:"type:int;comment:source table chunk waits" json:"chunkWaits"`
	ChunkRuns      uint64  `gorm:"type:int;comment:source table chunk runs" json:"chunkRuns"`
	ChunkStops     uint64  `gorm:"type:int;comment:source table chunk stops" json:"chunkStops"`
	Duration       float64 `gorm:"comment:run duration, size: seconds" json:"duration"`
	*common.Entity
}

type DataCompareTask struct {
	ID              uint64  `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName        string  `gorm:"type:varchar(100);not null;uniqueIndex:uniq_schema_table_name_complex;index:idx_task_name;comment:task name" json:"taskName"`
	SchemaNameS     string  `gorm:"type:varchar(60);not null;uniqueIndex:uniq_schema_table_name_complex;comment:source schema name" json:"schemaNameS"`
	TableNameS      string  `gorm:"type:varchar(60);not null;uniqueIndex:uniq_schema_table_name_complex;comment:source table name" json:"tableNameS"`
	SchemaNameT     string  `gorm:"type:varchar(60);not null;comment:target schema name" json:"schemaNameT"`
	TableNameT      string  `gorm:"type:varchar(60);not null;comment:target table name" json:"tableNameT"`
	TableTypeS      string  `gorm:"type:varchar(60);comment:source table type" json:"tableTypeS"`
	GlobalScnS      uint64  `gorm:"type:bigint;comment:source global scn" json:"globalScnS"`
	CompareMethod   string  `gorm:"type:varchar(50);not null;comment:compare method status" json:"compareMethod"`
	ColumnDetailSO  string  `gorm:"type:longtext;not null;comment:source table column origin detail" json:"columnDetailSO"`
	ColumnDetailS   string  `gorm:"type:longtext;comment:source column used to query information" json:"columnDetailS"`
	ColumnDetailTO  string  `gorm:"type:longtext;not null;comment:target table column origin detail" json:"columnDetailTO"`
	ColumnDetailT   string  `gorm:"type:longtext;comment:source column used to query information" json:"columnDetailT"`
	SqlHintS        string  `gorm:"type:varchar(300);comment:source sql hint" json:"sqlHintS"`
	SqlHintT        string  `gorm:"type:varchar(300);comment:target sql hint" json:"sqlHintT"`
	ChunkDetailS    string  `gorm:"type:varchar(500);not null;uniqueIndex:uniq_schema_table_name_complex;comment:source table chunk detail" json:"chunkDetailS"`
	ChunkDetailT    string  `gorm:"type:varchar(500);not null;comment:target table chunk detail" json:"chunkDetailT"`
	ConsistentReadS string  `gorm:"type:varchar(10);comment:source sql consistent read" json:"consistentReadS"`
	TaskStatus      string  `gorm:"type:varchar(50);not null;comment:task run status" json:"taskStatus"`
	FixDetailAddT   string  `gorm:"type:longtext;comment:fix detail infos used to query information" json:"fixDetailAddT"`
	FixDetailDelT   string  `gorm:"type:longtext;comment:fix detail infos used to query information" json:"fixDetailDelT"`
	ErrorDetail     string  `gorm:"type:longtext;comment:error detail" json:"errorDetail"`
	Duration        float64 `gorm:"comment:run duration, size: seconds" json:"duration"`
	*common.Entity
}

type StructGroupStatusResult struct {
	TaskName     string `json:"taskName"`
	TaskStatus   string `json:"taskStatus"`
	StatusCounts int64  `json:"statusCounts"`
}

type DataGroupChunkResult struct {
	TaskName    string `json:"taskName"`
	SchemaNameS string `json:"schemaNameS"`
	TableNameS  string `json:"tableNameS"`
	ChunkTotals int64  `json:"chunkTotals"`
}

type DataGroupStatusResult struct {
	TaskName     string `json:"taskName"`
	TaskStatus   string `json:"taskStatus"`
	StatusCounts int64  `json:"statusCounts"`
}

type DataGroupTaskStatusResult struct {
	TaskName     string `json:"taskName"`
	SchemaNameS  string `json:"schemaNameS"`
	TableNameS   string `json:"tableNameS"`
	TaskStatus   string `json:"taskStatus"`
	StatusTotals int64  `json:"statusTotals"`
}
