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

import (
	"context"
	"fmt"
	"reflect"

	"gorm.io/gorm"

	"github.com/wentaojin/dbms/model/common"
)

type RWStructMigrateTaskRule struct {
	common.GormDB
}

func NewStructMigrateTaskRuleRW(db *gorm.DB) *RWStructMigrateTaskRule {
	m := &RWStructMigrateTaskRule{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWStructMigrateTaskRule) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(TaskStructRule{}).Name())
}

func (rw *RWStructMigrateTaskRule) CreateTaskStructRule(ctx context.Context, data *TaskStructRule) (*TaskStructRule, error) {
	err := rw.DB(ctx).Create(data).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return data, nil
}

func (rw *RWStructMigrateTaskRule) ListTaskStructRule(ctx context.Context, page uint64, pageSize uint64) ([]*TaskStructRule, error) {
	var dataS []*TaskStructRule
	if page == 0 && pageSize == 0 {
		err := rw.DB(ctx).Model(&TaskStructRule{}).Find(&dataS).Error
		if err != nil {
			return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
		}
		return dataS, nil
	}
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&TaskStructRule{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateTaskRule) QueryTaskStructRule(ctx context.Context, data *TaskStructRule) ([]*TaskStructRule, error) {
	var dataS []*TaskStructRule
	err := rw.DB(ctx).Model(&TaskStructRule{}).Where("task_name = ?", data.TaskName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateTaskRule) DeleteTaskStructRule(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&TaskStructRule{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

type RWStructMigrateSchemaRule struct {
	common.GormDB
}

func NewStructMigrateSchemaRuleRW(db *gorm.DB) *RWStructMigrateSchemaRule {
	m := &RWStructMigrateSchemaRule{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWStructMigrateSchemaRule) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(SchemaStructRule{}).Name())
}

func (rw *RWStructMigrateSchemaRule) CreateSchemaStructRule(ctx context.Context, data *SchemaStructRule) (*SchemaStructRule, error) {
	err := rw.DB(ctx).Create(data).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return data, nil
}

func (rw *RWStructMigrateSchemaRule) ListSchemaStructRule(ctx context.Context, page uint64, pageSize uint64) ([]*SchemaStructRule, error) {
	var dataS []*SchemaStructRule
	if page == 0 && pageSize == 0 {
		err := rw.DB(ctx).Model(&SchemaStructRule{}).Find(&dataS).Error
		if err != nil {
			return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
		}
		return dataS, nil
	}
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&SchemaStructRule{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateSchemaRule) QuerySchemaStructRule(ctx context.Context, data *SchemaStructRule) ([]*SchemaStructRule, error) {
	var dataS []*SchemaStructRule
	err := rw.DB(ctx).Model(&SchemaStructRule{}).Where("task_name = ? AND schema_name_s = ?", data.TaskName, data.SchemaNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateSchemaRule) DeleteSchemaStructRule(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&SchemaStructRule{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWStructMigrateSchemaRule) FindSchemaStructRule(ctx context.Context, taskName string) ([]*SchemaStructRule, error) {
	var dataS []*SchemaStructRule
	err := rw.DB(ctx).Model(&SchemaStructRule{}).Where("task_name = ?", taskName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

type RWStructMigrateTableRule struct {
	common.GormDB
}

func NewStructMigrateTableRuleRW(db *gorm.DB) *RWStructMigrateTableRule {
	m := &RWStructMigrateTableRule{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWStructMigrateTableRule) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(TableStructRule{}).Name())
}

func (rw *RWStructMigrateTableRule) CreateTableStructRule(ctx context.Context, data *TableStructRule) (*TableStructRule, error) {
	err := rw.DB(ctx).Create(data).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return data, nil
}

func (rw *RWStructMigrateTableRule) ListTableStructRule(ctx context.Context, page uint64, pageSize uint64) ([]*TableStructRule, error) {
	var dataS []*TableStructRule
	if page == 0 && pageSize == 0 {
		err := rw.DB(ctx).Model(&TableStructRule{}).Find(&dataS).Error
		if err != nil {
			return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
		}
		return dataS, nil
	}
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&TableStructRule{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateTableRule) QueryTableStructRule(ctx context.Context, data *TableStructRule) ([]*TableStructRule, error) {
	var dataS []*TableStructRule
	err := rw.DB(ctx).Model(&TableStructRule{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", data.TaskName, data.SchemaNameS, data.TableNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateTableRule) DeleteTableStructRule(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&TableStructRule{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWStructMigrateTableRule) FindTableStructRule(ctx context.Context, taskName string) ([]*TableStructRule, error) {
	var dataS []*TableStructRule
	err := rw.DB(ctx).Model(&TableStructRule{}).Where("task_name = ?", taskName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

type RWStructMigrateColumnRule struct {
	common.GormDB
}

func NewStructMigrateColumnRuleRW(db *gorm.DB) *RWStructMigrateColumnRule {
	m := &RWStructMigrateColumnRule{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWStructMigrateColumnRule) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(ColumnStructRule{}).Name())
}

func (rw *RWStructMigrateColumnRule) CreateColumnStructRule(ctx context.Context, data *ColumnStructRule) (*ColumnStructRule, error) {
	err := rw.DB(ctx).Create(data).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return data, nil
}

func (rw *RWStructMigrateColumnRule) ListColumnStructRule(ctx context.Context, page uint64, pageSize uint64) ([]*ColumnStructRule, error) {
	var dataS []*ColumnStructRule
	if page == 0 && pageSize == 0 {
		err := rw.DB(ctx).Model(&ColumnStructRule{}).Find(&dataS).Error
		if err != nil {
			return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
		}
		return dataS, nil
	}
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&ColumnStructRule{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateColumnRule) QueryColumnStructRule(ctx context.Context, data *ColumnStructRule) ([]*ColumnStructRule, error) {
	var dataS []*ColumnStructRule
	err := rw.DB(ctx).Model(&ColumnStructRule{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", data.TaskName, data.SchemaNameS, data.TableNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateColumnRule) DeleteColumnStructRule(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&ColumnStructRule{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWStructMigrateColumnRule) FindColumnStructRule(ctx context.Context, taskName string) ([]*ColumnStructRule, error) {
	var dataS []*ColumnStructRule
	err := rw.DB(ctx).Model(&ColumnStructRule{}).Where("task_name = ?", taskName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

type RWStructMigrateTableAttrsRule struct {
	common.GormDB
}

func NewStructMigrateTableAttrsRuleRW(db *gorm.DB) *RWStructMigrateTableAttrsRule {
	m := &RWStructMigrateTableAttrsRule{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWStructMigrateTableAttrsRule) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(TableAttrsRule{}).Name())
}

func (rw *RWStructMigrateTableAttrsRule) CreateTableAttrsRule(ctx context.Context, data *TableAttrsRule) (*TableAttrsRule, error) {
	err := rw.DB(ctx).Create(data).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return data, nil
}

func (rw *RWStructMigrateTableAttrsRule) ListTableAttrsRule(ctx context.Context, page uint64, pageSize uint64) ([]*TableAttrsRule, error) {
	var dataS []*TableAttrsRule
	if page == 0 && pageSize == 0 {
		err := rw.DB(ctx).Model(&TableAttrsRule{}).Find(&dataS).Error
		if err != nil {
			return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
		}
		return dataS, nil
	}
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&TableAttrsRule{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateTableAttrsRule) GetTableAttrsRule(ctx context.Context, data *TableAttrsRule) ([]*TableAttrsRule, error) {
	var dataS []*TableAttrsRule
	err := rw.DB(ctx).Model(&TableAttrsRule{}).Where(
		rw.DB(ctx).Model(&TableAttrsRule{}).
			Where("task_name = ? AND schema_name_s = ?", data.TaskName, data.SchemaNameS).
			Where(
				rw.DB(ctx).Model(&TableAttrsRule{}).
					Where("table_name_s = ?", data.TableNameS).
					Or("table_name_s = ?", "*")),
	).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateTableAttrsRule) DeleteTableAttrsRule(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&TableAttrsRule{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWStructMigrateTableAttrsRule) FindTableAttrsRule(ctx context.Context, taskName string) ([]*TableAttrsRule, error) {
	var dataS []*TableAttrsRule
	err := rw.DB(ctx).Model(&TableAttrsRule{}).Where("task_name = ?", taskName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}
