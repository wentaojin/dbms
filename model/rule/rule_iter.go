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
package rule

import "context"

type ISchemaRouteRule interface {
	CreateSchemaRouteRule(ctx context.Context, rule *SchemaRouteRule) (*SchemaRouteRule, error)
	UpdateSchemaRouteRule(ctx context.Context, rule *SchemaRouteRule) (*SchemaRouteRule, error)
	GetSchemaRouteRule(ctx context.Context, rule *SchemaRouteRule) (*SchemaRouteRule, error)
	ListSchemaRouteRule(ctx context.Context, page uint64, pageSize uint64) ([]*SchemaRouteRule, error)
	DeleteSchemaRouteRule(ctx context.Context, taskNames []string) error
}

type IMigrateTaskTable interface {
	CreateMigrateTaskTable(ctx context.Context, rule *MigrateTaskTable) (*MigrateTaskTable, error)
	UpdateMigrateTaskTable(ctx context.Context, rule *MigrateTaskTable) (*MigrateTaskTable, error)
	GetMigrateTaskTable(ctx context.Context, rule *MigrateTaskTable) (*MigrateTaskTable, error)
	ListMigrateTaskTable(ctx context.Context, page uint64, pageSize uint64) ([]*MigrateTaskTable, error)
	DeleteMigrateTaskTable(ctx context.Context, taskNames []string) error
	DeleteMigrateTaskTableByTaskIsExclude(ctx context.Context, rule *MigrateTaskTable) error
	FindMigrateTaskTable(ctx context.Context, rule *MigrateTaskTable) ([]*MigrateTaskTable, error)
}

type ITableRouteRule interface {
	CreateTableRouteRule(ctx context.Context, rule *TableRouteRule) (*TableRouteRule, error)
	CreateInBatchTableRouteRule(ctx context.Context, rule []*TableRouteRule, thread, batchSize int) ([]*TableRouteRule, error)
	UpdateTableRouteRule(ctx context.Context, rule *TableRouteRule) (*TableRouteRule, error)
	GetTableRouteRule(ctx context.Context, rule *TableRouteRule) (*TableRouteRule, error)
	ListTableRouteRule(ctx context.Context, page uint64, pageSize uint64) ([]*TableRouteRule, error)
	DeleteTableRouteRule(ctx context.Context, taskNames []string) error
	FindTableRouteRule(ctx context.Context, rule *TableRouteRule) ([]*TableRouteRule, error)
}

type IColumnRouteRule interface {
	CreateColumnRouteRule(ctx context.Context, rule *ColumnRouteRule) (*ColumnRouteRule, error)
	CreateInBatchColumnRouteRule(ctx context.Context, rule []*ColumnRouteRule, thread, batchSize int) ([]*ColumnRouteRule, error)
	UpdateColumnRouteRule(ctx context.Context, rule *ColumnRouteRule) (*ColumnRouteRule, error)
	GetColumnRouteRule(ctx context.Context, rule *ColumnRouteRule) (*ColumnRouteRule, error)
	ListColumnRouteRule(ctx context.Context, page uint64, pageSize uint64) ([]*ColumnRouteRule, error)
	DeleteColumnRouteRule(ctx context.Context, taskNames []string) error
	FindColumnRouteRule(ctx context.Context, rule *ColumnRouteRule) ([]*ColumnRouteRule, error)
}

type IDataMigrateRule interface {
	CreateDataMigrateRule(ctx context.Context, rule *DataMigrateRule) (*DataMigrateRule, error)
	CreateInBatchDataMigrateRule(ctx context.Context, rule []*DataMigrateRule, thread, batchSize int) ([]*DataMigrateRule, error)
	GetDataMigrateRule(ctx context.Context, rule *DataMigrateRule) (*DataMigrateRule, error)
	DeleteDataMigrateRule(ctx context.Context, taskNames []string) error
	FindDataMigrateRule(ctx context.Context, rule *DataMigrateRule) ([]*DataMigrateRule, error)
	IsContainedDataMigrateRuleRecord(ctx context.Context, rule *DataMigrateRule) (bool, error)
}

type IDataCompareRule interface {
	CreateDataCompareRule(ctx context.Context, rule *DataCompareRule) (*DataCompareRule, error)
	CreateInBatchDataCompareRule(ctx context.Context, rule []*DataCompareRule, thread, batchSize int) ([]*DataCompareRule, error)
	GetDataCompareRule(ctx context.Context, rule *DataCompareRule) (*DataCompareRule, error)
	DeleteDataCompareRule(ctx context.Context, taskNames []string) error
	FindDataCompareRule(ctx context.Context, rule *DataCompareRule) ([]*DataCompareRule, error)
	IsContainedDataCompareRuleRecord(ctx context.Context, rule *DataCompareRule) (bool, error)
}

type IDataScanRule interface {
	CreateDataScanRule(ctx context.Context, rule *DataScanRule) (*DataScanRule, error)
	CreateInBatchDataScanRule(ctx context.Context, rule []*DataScanRule, thread, batchSize int) ([]*DataScanRule, error)
	GetDataScanRule(ctx context.Context, rule *DataScanRule) (*DataScanRule, error)
	DeleteDataScanRule(ctx context.Context, taskNames []string) error
	FindDataScanRule(ctx context.Context, rule *DataScanRule) ([]*DataScanRule, error)
	IsContainedDataScanRuleRecord(ctx context.Context, rule *DataScanRule) (bool, error)
}

type ISqlMigrateRule interface {
	CreateSqlMigrateRule(ctx context.Context, rule *SqlMigrateRule) (*SqlMigrateRule, error)
	CreateInBatchSqlMigrateRule(ctx context.Context, rule []*SqlMigrateRule, thread, batchSize int) ([]*SqlMigrateRule, error)
	UpdateSqlMigrateRule(ctx context.Context, rule *SqlMigrateRule) (*SqlMigrateRule, error)
	GetSqlMigrateRule(ctx context.Context, rule *SqlMigrateRule) (*SqlMigrateRule, error)
	DeleteSqlMigrateRule(ctx context.Context, taskNames []string) error
	FindSqlMigrateRule(ctx context.Context, rule *SqlMigrateRule) ([]*SqlMigrateRule, error)
	FindSqlMigrateRuleGroupBySchemaTable(ctx context.Context) ([]*SqlMigrateRuleGroupSchemaTableTResult, error)
}
