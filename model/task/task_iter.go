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

import "context"

type ITask interface {
	CreateTask(ctx context.Context, task *Task) (*Task, error)
	UpdateTask(ctx context.Context, task *Task, updates map[string]interface{}) (*Task, error)
	GetTask(ctx context.Context, task *Task) (*Task, error)
	ListTask(ctx context.Context, page uint64, pageSize uint64) ([]*Task, error)
	DeleteTask(ctx context.Context, taskName []string) error
}

type ILog interface {
	CreateLog(ctx context.Context, l *Log) (*Log, error)
	UpdateLog(ctx context.Context, l *Log, updates map[string]interface{}) (*Log, error)
	QueryLog(ctx context.Context, l *Log, last int) ([]*Log, error)
	ListLog(ctx context.Context, page uint64, pageSize uint64) ([]*Log, error)
	DeleteLog(ctx context.Context, taskName []string) error
}

type IAssessMigrateTask interface {
	CreateAssessMigrateTask(ctx context.Context, task *AssessMigrateTask) (*AssessMigrateTask, error)
	GetAssessMigrateTask(ctx context.Context, task *AssessMigrateTask) ([]*AssessMigrateTask, error)
	QueryAssessMigrateTask(ctx context.Context, task *AssessMigrateTask) ([]*AssessMigrateTask, error)
	UpdateAssessMigrateTask(ctx context.Context, task *AssessMigrateTask, updates map[string]interface{}) (*AssessMigrateTask, error)
	ListAssessMigrateTask(ctx context.Context, page uint64, pageSize uint64) ([]*AssessMigrateTask, error)
	DeleteAssessMigrateTask(ctx context.Context, id uint64) error
	DeleteAssessMigrateTaskName(ctx context.Context, taskName []string) error
}

type IStructMigrateSummary interface {
	CreateStructMigrateSummary(ctx context.Context, task *StructMigrateSummary) (*StructMigrateSummary, error)
	GetStructMigrateSummary(ctx context.Context, task *StructMigrateSummary) (*StructMigrateSummary, error)
	UpdateStructMigrateSummary(ctx context.Context, task *StructMigrateSummary, updates map[string]interface{}) (*StructMigrateSummary, error)
	FindStructMigrateSummary(ctx context.Context, task *StructMigrateSummary) ([]*StructMigrateSummary, error)
	DeleteStructMigrateSummary(ctx context.Context, task *StructMigrateSummary) error
	DeleteStructMigrateSummaryName(ctx context.Context, taskName []string) error
}

type IStructMigrateTask interface {
	CreateStructMigrateTask(ctx context.Context, task *StructMigrateTask) (*StructMigrateTask, error)
	GetStructMigrateTask(ctx context.Context, task *StructMigrateTask) ([]*StructMigrateTask, error)
	GetStructMigrateTaskTable(ctx context.Context, task *StructMigrateTask) ([]*StructMigrateTask, error)
	UpdateStructMigrateTask(ctx context.Context, task *StructMigrateTask, updates map[string]interface{}) (*StructMigrateTask, error)
	BatchUpdateStructMigrateTask(ctx context.Context, task *StructMigrateTask, updates map[string]interface{}) (*StructMigrateTask, error)
	FindStructMigrateTask(ctx context.Context, task *StructMigrateTask) ([]*StructMigrateTask, error)
	FindStructMigrateTaskGroupByTaskStatus(ctx context.Context, taskName string) ([]*StructGroupStatusResult, error)
	BatchFindStructMigrateTask(ctx context.Context, task *StructMigrateTask) ([]*StructMigrateTask, error)
	QueryStructMigrateTask(ctx context.Context, task *StructMigrateTask) ([]*StructMigrateTask, error)
	ListStructMigrateTask(ctx context.Context, page uint64, pageSize uint64) ([]*StructMigrateTask, error)
	DeleteStructMigrateTask(ctx context.Context, id uint64) error
	DeleteStructMigrateTaskName(ctx context.Context, taskName []string) error
}

type IStructCompareSummary interface {
	CreateStructCompareSummary(ctx context.Context, task *StructCompareSummary) (*StructCompareSummary, error)
	GetStructCompareSummary(ctx context.Context, task *StructCompareSummary) (*StructCompareSummary, error)
	UpdateStructCompareSummary(ctx context.Context, task *StructCompareSummary, updates map[string]interface{}) (*StructCompareSummary, error)
	FindStructCompareSummary(ctx context.Context, task *StructCompareSummary) ([]*StructCompareSummary, error)
	DeleteStructCompareSummary(ctx context.Context, task *StructCompareSummary) error
	DeleteStructCompareSummaryName(ctx context.Context, taskName []string) error
}

type IStructCompareTask interface {
	CreateStructCompareTask(ctx context.Context, task *StructCompareTask) (*StructCompareTask, error)
	GetStructCompareTask(ctx context.Context, task *StructCompareTask) ([]*StructCompareTask, error)
	GetStructCompareTaskTable(ctx context.Context, task *StructCompareTask) ([]*StructCompareTask, error)
	UpdateStructCompareTask(ctx context.Context, task *StructCompareTask, updates map[string]interface{}) (*StructCompareTask, error)
	BatchUpdateStructCompareTask(ctx context.Context, task *StructCompareTask, updates map[string]interface{}) (*StructCompareTask, error)
	FindStructCompareTask(ctx context.Context, task *StructCompareTask) ([]*StructCompareTask, error)
	FindStructCompareTaskGroupByTaskStatus(ctx context.Context, taskName string) ([]*StructGroupStatusResult, error)
	BatchFindStructCompareTask(ctx context.Context, task *StructCompareTask) ([]*StructCompareTask, error)
	ListStructCompareTask(ctx context.Context, page uint64, pageSize uint64) ([]*StructCompareTask, error)
	DeleteStructCompareTask(ctx context.Context, id uint64) error
	DeleteStructCompareTaskName(ctx context.Context, taskName []string) error
}

type IDataMigrateSummary interface {
	CreateDataMigrateSummary(ctx context.Context, task *DataMigrateSummary) (*DataMigrateSummary, error)
	GetDataMigrateSummary(ctx context.Context, task *DataMigrateSummary) (*DataMigrateSummary, error)
	UpdateDataMigrateSummary(ctx context.Context, task *DataMigrateSummary, updates map[string]interface{}) (*DataMigrateSummary, error)
	FindDataMigrateSummary(ctx context.Context, task *DataMigrateSummary) ([]*DataMigrateSummary, error)
	DeleteDataMigrateSummary(ctx context.Context, task *DataMigrateSummary) error
	DeleteDataMigrateSummaryName(ctx context.Context, taskName []string) error
}

type IDataMigrateTask interface {
	CreateDataMigrateTask(ctx context.Context, task *DataMigrateTask) (*DataMigrateTask, error)
	CreateInBatchDataMigrateTask(ctx context.Context, task []*DataMigrateTask, thread, batchSize int) error
	UpdateDataMigrateTask(ctx context.Context, task *DataMigrateTask, updates map[string]interface{}) (*DataMigrateTask, error)
	BatchUpdateDataMigrateTask(ctx context.Context, task *DataMigrateTask, updates map[string]interface{}) (*DataMigrateTask, error)
	GetDataMigrateTask(ctx context.Context, task *DataMigrateTask) (*DataMigrateTask, error)
	FindDataMigrateTask(ctx context.Context, task *DataMigrateTask) ([]*DataMigrateTask, error)
	QueryDataMigrateTask(ctx context.Context, task *DataMigrateTask) ([]*DataMigrateTask, error)
	FindDataMigrateTaskBySchemaTableChunkStatus(ctx context.Context, task *DataMigrateTask) ([]*DataGroupTaskStatusResult, error)
	FindDataMigrateTaskGroupByTaskSchemaTable(ctx context.Context, taskName string) ([]*DataGroupChunkResult, error)
	FindDataMigrateTaskGroupByTaskSchemaTableStatus(ctx context.Context, taskName string) ([]*DataGroupTaskStatusResult, error)
	FindDataMigrateTaskGroupByTaskStatus(ctx context.Context, taskName string) ([]*DataGroupStatusResult, error)
	ListDataMigrateTask(ctx context.Context, page uint64, pageSize uint64) ([]*DataMigrateTask, error)
	DeleteDataMigrateTask(ctx context.Context, task *DataMigrateTask) error
	DeleteDataMigrateTaskName(ctx context.Context, taskName []string) error
}

type IDataCompareSummary interface {
	CreateDataCompareSummary(ctx context.Context, task *DataCompareSummary) (*DataCompareSummary, error)
	GetDataCompareSummary(ctx context.Context, task *DataCompareSummary) (*DataCompareSummary, error)
	UpdateDataCompareSummary(ctx context.Context, task *DataCompareSummary, updates map[string]interface{}) (*DataCompareSummary, error)
	FindDataCompareSummary(ctx context.Context, task *DataCompareSummary) ([]*DataCompareSummary, error)
	DeleteDataCompareSummary(ctx context.Context, task *DataCompareSummary) error
	DeleteDataCompareSummaryName(ctx context.Context, taskName []string) error
}

type IDataCompareTask interface {
	CreateDataCompareTask(ctx context.Context, task *DataCompareTask) (*DataCompareTask, error)
	CreateInBatchDataCompareTask(ctx context.Context, task []*DataCompareTask, thread, batchSize int) error
	UpdateDataCompareTask(ctx context.Context, task *DataCompareTask, updates map[string]interface{}) (*DataCompareTask, error)
	BatchUpdateDataCompareTask(ctx context.Context, task *DataCompareTask, updates map[string]interface{}) (*DataCompareTask, error)
	FindDataCompareTask(ctx context.Context, task *DataCompareTask) ([]*DataCompareTask, error)
	QueryDataCompareTask(ctx context.Context, task *DataCompareTask) ([]*DataCompareTask, error)
	FindDataCompareTaskBySchemaTableChunkStatus(ctx context.Context, task *DataCompareTask) ([]*DataGroupTaskStatusResult, error)
	FindDataCompareTaskGroupByTaskSchemaTable(ctx context.Context, taskName string) ([]*DataGroupChunkResult, error)
	FindDataCompareTaskGroupByTaskSchemaTableStatus(ctx context.Context, taskName string) ([]*DataGroupTaskStatusResult, error)
	FindDataCompareTaskGroupByTaskStatus(ctx context.Context, taskName string) ([]*DataGroupStatusResult, error)
	QueryDataCompareTaskByTaskStatus(ctx context.Context, task *DataCompareTask) ([]*DataCompareTask, error)
	ListDataCompareTask(ctx context.Context, page uint64, pageSize uint64) ([]*DataCompareTask, error)
	DeleteDataCompareTask(ctx context.Context, task *DataCompareTask) error
	DeleteDataCompareTaskName(ctx context.Context, taskName []string) error
}

type IDataCompareResult interface {
	CreateDataCompareResult(ctx context.Context, task *DataCompareResult) (*DataCompareResult, error)
	FindDataCompareResult(ctx context.Context, task *DataCompareResult) ([]*DataCompareResult, error)
	DeleteDataCompareResult(ctx context.Context, task *DataCompareResult) error
	DeleteDataCompareResultName(ctx context.Context, taskName []string) error
}

type ISqlMigrateSummary interface {
	CreateSqlMigrateSummary(ctx context.Context, task *SqlMigrateSummary) (*SqlMigrateSummary, error)
	GetSqlMigrateSummary(ctx context.Context, task *SqlMigrateSummary) (*SqlMigrateSummary, error)
	UpdateSqlMigrateSummary(ctx context.Context, task *SqlMigrateSummary, updates map[string]interface{}) (*SqlMigrateSummary, error)
	DeleteSqlMigrateSummary(ctx context.Context, task *SqlMigrateSummary) error
	DeleteSqlMigrateSummaryName(ctx context.Context, taskName []string) error
}

type ISqlMigrateTask interface {
	CreateSqlMigrateTask(ctx context.Context, task *SqlMigrateTask) (*SqlMigrateTask, error)
	CreateInBatchSqlMigrateTask(ctx context.Context, task []*SqlMigrateTask, thread, batchSize int) error
	UpdateSqlMigrateTask(ctx context.Context, task *SqlMigrateTask, updates map[string]interface{}) (*SqlMigrateTask, error)
	BatchUpdateSqlMigrateTask(ctx context.Context, task *SqlMigrateTask, updates map[string]interface{}) (*SqlMigrateTask, error)
	FindSqlMigrateTaskByTaskStatus(ctx context.Context, task *SqlMigrateTask) ([]*SqlMigrateTask, error)
	FindSqlMigrateTaskGroupByTaskStatus(ctx context.Context, taskName string) ([]*DataGroupStatusResult, error)
	ListSqlMigrateTask(ctx context.Context, page uint64, pageSize uint64) ([]*SqlMigrateTask, error)
	DeleteSqlMigrateTask(ctx context.Context, task *SqlMigrateTask) error
	DeleteSqlMigrateTaskName(ctx context.Context, taskName []string) error
}

type IDataScanSummary interface {
	CreateDataScanSummary(ctx context.Context, task *DataScanSummary) (*DataScanSummary, error)
	GetDataScanSummary(ctx context.Context, task *DataScanSummary) (*DataScanSummary, error)
	UpdateDataScanSummary(ctx context.Context, task *DataScanSummary, updates map[string]interface{}) (*DataScanSummary, error)
	DeleteDataScanSummary(ctx context.Context, task *DataScanSummary) error
	DeleteDataScanSummaryName(ctx context.Context, taskName []string) error
	FindDataScanSummary(ctx context.Context, task *DataScanSummary) ([]*DataScanSummary, error)
}

type IDataScanTask interface {
	CreateDataScanTask(ctx context.Context, task *DataScanTask) (*DataScanTask, error)
	CreateInBatchDataScanTask(ctx context.Context, task []*DataScanTask, thread, batchSize int) error
	UpdateDataScanTask(ctx context.Context, task *DataScanTask, updates map[string]interface{}) (*DataScanTask, error)
	BatchUpdateDataScanTask(ctx context.Context, task *DataScanTask, updates map[string]interface{}) (*DataScanTask, error)
	ListDataScanTask(ctx context.Context, page uint64, pageSize uint64) ([]*DataScanTask, error)
	DeleteDataScanTask(ctx context.Context, task *DataScanTask) error
	DeleteDataScanTaskName(ctx context.Context, taskName []string) error
	FindDataScanTask(ctx context.Context, task *DataScanTask) ([]*DataScanTask, error)
	QueryDataScanTask(ctx context.Context, task *DataScanTask) ([]*DataScanTask, error)
	FindDataScanTaskGroupByTaskStatus(ctx context.Context, taskName string) ([]*DataGroupStatusResult, error)
	FindDataScanTaskGroupByTaskSchemaTable(ctx context.Context, taskName string) ([]*DataGroupChunkResult, error)
	FindDataScanTaskBySchemaTableChunkStatus(ctx context.Context, task *DataScanTask) ([]*DataGroupTaskStatusResult, error)
	QueryDataScanTaskByTaskStatus(ctx context.Context, task *DataScanTask) ([]*DataScanTask, error)
}
