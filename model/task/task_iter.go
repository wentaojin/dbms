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
	QueryLog(ctx context.Context, l *Log) ([]*Log, error)
	ListLog(ctx context.Context, page uint64, pageSize uint64) ([]*Log, error)
	DeleteLog(ctx context.Context, taskName []string) error
}

type IStructMigrateTask interface {
	CreateStructMigrateTask(ctx context.Context, task *StructMigrateTask) (*StructMigrateTask, error)
	GetStructMigrateTask(ctx context.Context, task *StructMigrateTask) ([]*StructMigrateTask, error)
	GetStructMigrateTaskTable(ctx context.Context, task *StructMigrateTask) ([]*StructMigrateTask, error)
	UpdateStructMigrateTask(ctx context.Context, task *StructMigrateTask, updates map[string]interface{}) (*StructMigrateTask, error)
	BatchUpdateStructMigrateTask(ctx context.Context, task *StructMigrateTask, updates map[string]interface{}) (*StructMigrateTask, error)
	FindStructMigrateTask(ctx context.Context, task *StructMigrateTask) ([]*StructMigrateTask, error)
	BatchFindStructMigrateTask(ctx context.Context, task *StructMigrateTask) ([]*StructMigrateTask, error)
	QueryStructMigrateTask(ctx context.Context, task *StructMigrateTask) ([]*StructMigrateTask, error)
	ListStructMigrateTask(ctx context.Context, page uint64, pageSize uint64) ([]*StructMigrateTask, error)
	DeleteStructMigrateTask(ctx context.Context, taskID uint64) error
}

type IDataMigrateTask interface {
	CreateDataMigrateTask(ctx context.Context, task *DataMigrateTask) (*DataMigrateTask, error)
	UpdateDataMigrateTask(ctx context.Context, task *DataMigrateTask, updates map[string]interface{}) (*DataMigrateTask, error)
	QueryDataMigrateTask(ctx context.Context, task *DataMigrateTask) ([]*DataMigrateTask, error)
	ListDataMigrateTask(ctx context.Context, page uint64, pageSize uint64) ([]*DataMigrateTask, error)
	DeleteDataMigrateTask(ctx context.Context, taskID uint64) error
}
