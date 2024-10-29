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
package migrate

import "github.com/wentaojin/dbms/model/common"

type TaskStructRule struct {
	ID            uint   `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName      string `gorm:"type:varchar(100);not null;index:idx_task_datatype_complex;index:idx_task_defaultval_complex;comment:task name" json:"taskName"`
	ColumnTypeS   string `gorm:"type:varchar(100);not null;index:idx_task_datatype_complex;comment:source column datatype" json:"columnTypeS"`
	ColumnTypeT   string `gorm:"type:varchar(100);not null;comment:target column datatype" json:"columnTypeT"`
	DefaultValueS string `gorm:"type:varchar(200);not null;index:idx_task_defaultval_complex;comment:source column default value" json:"defaultValueS"`
	DefaultValueT string `gorm:"type:varchar(100);not null;comment:target column default value" json:"defaultValueT"`
	*common.Entity
}

type SchemaStructRule struct {
	ID            uint   `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName      string `gorm:"type:varchar(100);not null;index:idx_schema_datatype_complex;index:idx_task_defaultval_complex;comment:task name" json:"taskName"`
	SchemaNameS   string `gorm:"type:varchar(150);not null;index:idx_schema_datatype_complex;index:idx_task_defaultval_complex;comment:source schema" json:"schemaNameS"`
	ColumnTypeS   string `gorm:"type:varchar(100);not null;index:idx_schema_datatype_complex;comment:source column datatype" json:"schemaNameT"`
	ColumnTypeT   string `gorm:"type:varchar(100);not null;comment:target column datatype" json:"columnTypeT"`
	DefaultValueS string `gorm:"type:varchar(200);not null;index:idx_task_defaultval_complex;comment:source column default value" json:"defaultValueS"`
	DefaultValueT string `gorm:"type:varchar(100);not null;comment:target column default value" json:"defaultValueT"`
	*common.Entity
}

type TableStructRule struct {
	ID            uint   `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName      string `gorm:"type:varchar(100);not null;index:idx_table_datatype_complex;index:idx_task_defaultval_complex;comment:task name" json:"taskName"`
	SchemaNameS   string `gorm:"type:varchar(150);not null;index:idx_table_datatype_complex;index:idx_task_defaultval_complex;comment:source schema" json:"schemaNameS"`
	TableNameS    string `gorm:"type:varchar(150);not null;index:idx_table_datatype_complex;index:idx_task_defaultval_complex;comment:source table" json:"tableNameS"`
	ColumnTypeS   string `gorm:"type:varchar(100);not null;index:idx_table_datatype_complex;comment:source column datatype" json:"schemaNameT"`
	ColumnTypeT   string `gorm:"type:varchar(100);not null;comment:target column datatype" json:"columnTypeT"`
	DefaultValueS string `gorm:"type:varchar(200);not null;index:idx_task_defaultval_complex;comment:source column default value" json:"defaultValueS"`
	DefaultValueT string `gorm:"type:varchar(100);not null;comment:target column default value" json:"defaultValueT"`
	*common.Entity
}

type ColumnStructRule struct {
	ID            uint   `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName      string `gorm:"type:varchar(100);not null;index:idx_column_datatype_complex;index:idx_task_defaultval_complex;comment:task name" json:"taskName"`
	SchemaNameS   string `gorm:"type:varchar(150);not null;index:idx_column_datatype_complex;index:idx_task_defaultval_complex;comment:source schema" json:"schemaNameS"`
	TableNameS    string `gorm:"type:varchar(150);not null;index:idx_column_datatype_complex;index:idx_task_defaultval_complex;comment:source table" json:"tableNameS"`
	ColumnNameS   string `gorm:"type:varchar(150);not null;index:idx_column_datatype_complex;index:idx_task_defaultval_complex;comment:source column name" json:"columnNameS"`
	ColumnTypeS   string `gorm:"type:varchar(100);not null;index:idx_column_datatype_complex;comment:source column datatype" json:"schemaNameT"`
	ColumnTypeT   string `gorm:"type:varchar(100);not null;comment:target column datatype" json:"columnTypeT"`
	DefaultValueS string `gorm:"type:varchar(200);not null;index:idx_task_defaultval_complex;comment:source column default value" json:"defaultValueS"`
	DefaultValueT string `gorm:"type:varchar(100);not null;comment:target column default value" json:"defaultValueT"`
	*common.Entity
}

type TableAttrsRule struct {
	ID          uint   `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName    string `gorm:"type:varchar(100);not null;index:idx_task_table_complex;comment:task name" json:"taskName"`
	SchemaNameS string `gorm:"type:varchar(150);not null;index:idx_task_table_complex;comment:source schema" json:"schemaNameS"`
	TableNameS  string `gorm:"type:varchar(150);not null;index:idx_task_table_complex;comment:source table" json:"tableNameS"`
	TableAttrT  string `gorm:"type:varchar(200);not null;comment:target table suffix attr" json:"tableAttrT"`
}
