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
	SchemaNameS string `gorm:"type:varchar(120);index:schema_table_name;comment:source schema name" json:"schemaNameS"`
	TableNameS  string `gorm:"type:varchar(120);index:schema_table_name;comment:source table name" json:"tableNameS"`
	LogDetail   string `gorm:"type:longtext;comment:task running log" json:"logDetail"`
	*common.Entity
}

type StructMigrateTask struct {
	ID              uint64  `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName        string  `gorm:"type:varchar(120);not null;uniqueIndex:uniq_schema_table_name;index:idx_task_name;comment:task name"`
	SchemaNameS     string  `gorm:"type:varchar(120);not null;uniqueIndex:uniq_schema_table_name;comment:source schema name" json:"schemaNameS"`
	TableNameS      string  `gorm:"type:varchar(120);uniqueIndex:uniq_schema_table_name;comment:source table name" json:"tableNameS"`
	TableTypeS      string  `gorm:"type:varchar(120);comment:source table type" json:"tableTypeS"`
	SchemaNameT     string  `gorm:"type:varchar(120);comment:target schema name" json:"schemaNameT"`
	TableNameT      string  `gorm:"type:varchar(120);comment:target table name" json:"tableNameT"`
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

type DataMigrateTask struct {
	ID            uint64  `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName      string  `gorm:"type:varchar(120);not null;uniqueIndex:uniq_schema_table_name;index:idx_task_name;comment:task name"`
	SchemaNameS   string  `gorm:"type:varchar(120);not null;uniqueIndex:uniq_schema_table_name_complex;comment:source schema name" json:"schemaNameS"`
	TableNameS    string  `gorm:"type:varchar(120);not null;uniqueIndex:uniq_schema_table_name_complex;comment:source table name" json:"tableNameS"`
	SchemaNameT   string  `gorm:"type:varchar(120);not null;comment:target schema name" json:"schemaNameT"`
	TableNameT    string  `gorm:"type:varchar(120);not null;comment:target table name" json:"tableNameT"`
	ColumnDetailS string  `gorm:"type:text;comment:source column information" json:"columnDetailS"`
	ColumnDetailT string  `gorm:"type:text;comment:source column information" json:"columnDetailT"`
	SqlHintS      string  `gorm:"type:varchar(300);comment:source sql hint" json:"sqlHintS"`
	SqlHintT      string  `gorm:"type:varchar(300);comment:target sql hint" json:"sqlHintT"`
	ChunkDetailS  string  `gorm:"type:varchar(300);not null;uniqueIndex:uniq_schema_table_name_complex;comment:source table chunk detail" json:"chunkDetailS"`
	TaskStatus    string  `gorm:"type:varchar(50);not null;comment:task run status" json:"taskStatus"`
	CSVFile       string  `gorm:"type:varchar(300);comment:csv exporter file path" json:"csvFile"`
	ErrorDetail   string  `gorm:"type:longtext;comment:error detail" json:"errorDetail"`
	Duration      float64 `gorm:"comment:run duration, size: seconds" json:"duration"`
	*common.Entity
}
