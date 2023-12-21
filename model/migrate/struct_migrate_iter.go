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

import "context"

type IStructMigrateTaskRule interface {
	CreateTaskStructRule(ctx context.Context, data *TaskStructRule) (*TaskStructRule, error)
	ListTaskStructRule(ctx context.Context, page uint64, pageSize uint64) ([]*TaskStructRule, error)
	QueryTaskStructRule(ctx context.Context, data *TaskStructRule) ([]*TaskStructRule, error)
	DeleteTaskStructRule(ctx context.Context, taskName []string) error
}

type IStructMigrateSchemaRule interface {
	CreateSchemaStructRule(ctx context.Context, data *SchemaStructRule) (*SchemaStructRule, error)
	ListSchemaStructRule(ctx context.Context, page uint64, pageSize uint64) ([]*SchemaStructRule, error)
	QuerySchemaStructRule(ctx context.Context, data *SchemaStructRule) ([]*SchemaStructRule, error)
	DeleteSchemaStructRule(ctx context.Context, taskName []string) error
	FindSchemaStructRule(ctx context.Context, taskName string) ([]*SchemaStructRule, error)
}

type IStructMigrateTableRule interface {
	CreateTableStructRule(ctx context.Context, data *TableStructRule) (*TableStructRule, error)
	ListTableStructRule(ctx context.Context, page uint64, pageSize uint64) ([]*TableStructRule, error)
	QueryTableStructRule(ctx context.Context, data *TableStructRule) ([]*TableStructRule, error)
	DeleteTableStructRule(ctx context.Context, taskName []string) error
	FindTableStructRule(ctx context.Context, taskName string) ([]*TableStructRule, error)
}

type IStructMigrateColumnRule interface {
	CreateColumnStructRule(ctx context.Context, data *ColumnStructRule) (*ColumnStructRule, error)
	ListColumnStructRule(ctx context.Context, page uint64, pageSize uint64) ([]*ColumnStructRule, error)
	QueryColumnStructRule(ctx context.Context, data *ColumnStructRule) ([]*ColumnStructRule, error)
	DeleteColumnStructRule(ctx context.Context, taskName []string) error
	FindColumnStructRule(ctx context.Context, taskName string) ([]*ColumnStructRule, error)
}

type IStructMigrateTableAttrsRule interface {
	CreateTableAttrsRule(ctx context.Context, data *TableAttrsRule) (*TableAttrsRule, error)
	ListTableAttrsRule(ctx context.Context, page uint64, pageSize uint64) ([]*TableAttrsRule, error)
	GetTableAttrsRule(ctx context.Context, data *TableAttrsRule) (*TableAttrsRule, error)
	DeleteTableAttrsRule(ctx context.Context, taskName []string) error
	FindTableAttrsRule(ctx context.Context, taskName string) ([]*TableAttrsRule, error)
}
