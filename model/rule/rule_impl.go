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

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"gorm.io/gorm/clause"

	"github.com/wentaojin/dbms/model/common"
	"gorm.io/gorm"
)

type RWRule struct {
	common.GormDB
}

func NewMigrateTaskRuleRW(db *gorm.DB) *RWRule {
	m := &RWRule{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWRule) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(Rule{}).Name())
}

func (rw *RWRule) CreateRule(ctx context.Context, rule *Rule) (*Rule, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_rule_name"}},
		UpdateAll: true,
	}).Create(rule).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWRule) UpdateRule(ctx context.Context, rule *Rule) (*Rule, error) {
	err := rw.DB(ctx).Model(&Rule{}).Where("task_rule_name = ?", rule.TaskRuleName).Save(&rule).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWRule) GetRule(ctx context.Context, rule *Rule) (*Rule, error) {
	var dataS *Rule
	err := rw.DB(ctx).Model(&Rule{}).Where("task_rule_name = ?", rule.TaskRuleName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWRule) ListRule(ctx context.Context, page uint64, pageSize uint64) ([]*Rule, error) {
	var dataS []*Rule
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&Rule{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWRule) DeleteRule(ctx context.Context, taskRuleName []string) error {
	err := rw.DB(ctx).Where("task_rule_name IN (?)", taskRuleName).Delete(&Rule{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWRule) IsContainedRuleRecord(ctx context.Context, rule *Rule) (bool, error) {
	err := rw.DB(ctx).Where("task_rule_name = ?", rule.TaskRuleName).First(&Rule{}).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("get table [%s] record is contained failed: %v", rw.TableName(ctx), err)
	}
	return true, nil
}

func (rw *RWRule) FindRule(ctx context.Context, rule *Rule) ([]*Rule, error) {
	var dataS []*Rule
	err := rw.DB(ctx).Model(&Rule{}).Where("task_rule_name = ?", rule.TaskRuleName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

type RWSchemaRouteRule struct {
	common.GormDB
}

func NewSchemaRouteRuleRW(db *gorm.DB) *RWSchemaRouteRule {
	m := &RWSchemaRouteRule{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWSchemaRouteRule) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(SchemaRouteRule{}).Name())
}

func (rw *RWSchemaRouteRule) CreateSchemaRouteRule(ctx context.Context, rule *SchemaRouteRule) (*SchemaRouteRule, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_rule_name"}, {Name: "schema_name_s"}},
		UpdateAll: true,
	}).Create(rule).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWSchemaRouteRule) UpdateSchemaRouteRule(ctx context.Context, rule *SchemaRouteRule) (*SchemaRouteRule, error) {
	err := rw.DB(ctx).Model(&SchemaRouteRule{}).Where("task_rule_name = ? AND schema_name_s = ?", rule.TaskRuleName, rule.SchemaNameS).Save(&rule).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWSchemaRouteRule) GetSchemaRouteRule(ctx context.Context, rule *SchemaRouteRule) (*SchemaRouteRule, error) {
	var dataS *SchemaRouteRule
	err := rw.DB(ctx).Model(&SchemaRouteRule{}).Where("task_rule_name = ? AND schema_name_s = ?", rule.TaskRuleName, rule.SchemaNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSchemaRouteRule) ListSchemaRouteRule(ctx context.Context, page uint64, pageSize uint64) ([]*SchemaRouteRule, error) {
	var dataS []*SchemaRouteRule
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&SchemaRouteRule{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSchemaRouteRule) DeleteSchemaRouteRule(ctx context.Context, taskRuleName []string) error {
	err := rw.DB(ctx).Where("task_rule_name IN (?)", taskRuleName).Delete(&SchemaRouteRule{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWSchemaRouteRule) IsContainedSchemaRouteRuleRecord(ctx context.Context, rule *SchemaRouteRule) (bool, error) {
	err := rw.DB(ctx).Where("task_rule_name = ? AND schema_name_s = ?", rule.TaskRuleName, rule.SchemaNameS).First(&SchemaRouteRule{}).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return false, nil
		}
		return false, fmt.Errorf("get table [%s] record is contained failed: %v", rw.TableName(ctx), err)
	}
	return true, nil
}

func (rw *RWSchemaRouteRule) FindSchemaRouteRule(ctx context.Context, rule *SchemaRouteRule) ([]*SchemaRouteRule, error) {
	var dataS []*SchemaRouteRule
	err := rw.DB(ctx).Model(&SchemaRouteRule{}).Where("task_rule_name = ?", rule.TaskRuleName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

type RWMigrateTaskTable struct {
	common.GormDB
}

func NewMigrateTaskTableRW(db *gorm.DB) *RWMigrateTaskTable {
	m := &RWMigrateTaskTable{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWMigrateTaskTable) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(MigrateTaskTable{}).Name())
}

func (rw *RWMigrateTaskTable) CreateMigrateTaskTable(ctx context.Context, rule *MigrateTaskTable) (*MigrateTaskTable, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_rule_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
		UpdateAll: true,
	}).Create(rule).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWMigrateTaskTable) UpdateMigrateTaskTable(ctx context.Context, rule *MigrateTaskTable) (*MigrateTaskTable, error) {
	err := rw.DB(ctx).Model(&SchemaRouteRule{}).Where("task_rule_name = ? AND schema_name_s = ? AND table_name_s = ?", rule.TaskRuleName, rule.SchemaNameS, rule.TableNameS).Save(&rule).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWMigrateTaskTable) GetMigrateTaskTable(ctx context.Context, rule *MigrateTaskTable) (*MigrateTaskTable, error) {
	var dataS *MigrateTaskTable
	err := rw.DB(ctx).Model(&SchemaRouteRule{}).Where("task_rule_name = ? AND schema_name_s = ? AND table_name_s = ?", rule.TaskRuleName, rule.SchemaNameS, rule.TableNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWMigrateTaskTable) ListMigrateTaskTable(ctx context.Context, page uint64, pageSize uint64) ([]*MigrateTaskTable, error) {
	var dataS []*MigrateTaskTable
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&MigrateTaskTable{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWMigrateTaskTable) DeleteMigrateTaskTable(ctx context.Context, taskRuleName []string) error {
	err := rw.DB(ctx).Where("task_rule_name IN (?)", taskRuleName).Delete(&MigrateTaskTable{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWMigrateTaskTable) IsContainedMigrateTaskTableRecord(ctx context.Context, rule *MigrateTaskTable) (bool, error) {
	err := rw.DB(ctx).Where("task_rule_name = ? AND schema_name_s = ? AND table_name_s = ?", rule.TaskRuleName, rule.SchemaNameS, rule.TableNameS).First(&MigrateTaskTable{}).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("get table [%s] record is contained failed: %v", rw.TableName(ctx), err)
	}
	return true, nil
}

func (rw *RWMigrateTaskTable) FindMigrateTaskTable(ctx context.Context, rule *MigrateTaskTable) ([]*MigrateTaskTable, error) {
	var dataS []*MigrateTaskTable
	err := rw.DB(ctx).Model(&MigrateTaskTable{}).Where("task_rule_name = ? AND schema_name_s = ?", rule.TaskRuleName, rule.SchemaNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

type RWTableRouteRule struct {
	common.GormDB
}

func NewTableRouteRuleRW(db *gorm.DB) *RWTableRouteRule {
	m := &RWTableRouteRule{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWTableRouteRule) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(TableRouteRule{}).Name())
}

func (rw *RWTableRouteRule) CreateTableRouteRule(ctx context.Context, rule *TableRouteRule) (*TableRouteRule, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_rule_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
		UpdateAll: true,
	}).Create(rule).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWTableRouteRule) CreateInBatchTableRouteRule(ctx context.Context, rule []*TableRouteRule, batchSize int) ([]*TableRouteRule, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_rule_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
		UpdateAll: true,
	}).CreateInBatches(rule, batchSize).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record by batch failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWTableRouteRule) UpdateTableRouteRule(ctx context.Context, rule *TableRouteRule) (*TableRouteRule, error) {
	err := rw.DB(ctx).Model(&TableRouteRule{}).Where("task_rule_name = ? AND schema_name_s = ? AND table_name_s = ?",
		rule.TaskRuleName,
		rule.SchemaNameS,
		rule.TableNameS).Save(&rule).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWTableRouteRule) GetTableRouteRule(ctx context.Context, rule *TableRouteRule) (*TableRouteRule, error) {
	var dataS *TableRouteRule
	err := rw.DB(ctx).Model(&TableRouteRule{}).Where("task_rule_name = ? AND schema_name_s = ? AND table_name_s = ?", rule.TaskRuleName, rule.SchemaNameS, rule.TableNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWTableRouteRule) ListTableRouteRule(ctx context.Context, page uint64, pageSize uint64) ([]*TableRouteRule, error) {
	var dataS []*TableRouteRule
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&TableRouteRule{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWTableRouteRule) DeleteTableRouteRule(ctx context.Context, taskRuleName []string) error {
	err := rw.DB(ctx).Where("task_rule_name IN (?)", taskRuleName).Delete(&TableRouteRule{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWTableRouteRule) IsContainedTableRouteRuleRecord(ctx context.Context, rule *TableRouteRule) (bool, error) {
	err := rw.DB(ctx).Where("task_rule_name = ? AND schema_name_s = ? AND table_name_s = ?", rule.TaskRuleName,
		rule.SchemaNameS,
		rule.TableNameS).First(&TableRouteRule{}).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("get table [%s] record is contained failed: %v", rw.TableName(ctx), err)
	}
	return true, nil
}

func (rw *RWTableRouteRule) FindTableRouteRule(ctx context.Context, rule *TableRouteRule) ([]*TableRouteRule, error) {
	var dataS []*TableRouteRule
	err := rw.DB(ctx).Model(&TableRouteRule{}).Where("task_rule_name = ? AND schema_name_s = ?", rule.TaskRuleName, rule.SchemaNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

type RWColumnRouteRule struct {
	common.GormDB
}

func NewColumnRouteRuleRW(db *gorm.DB) *RWColumnRouteRule {
	m := &RWColumnRouteRule{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWColumnRouteRule) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(ColumnRouteRule{}).Name())
}

func (rw *RWColumnRouteRule) CreateColumnRouteRule(ctx context.Context, rule *ColumnRouteRule) (*ColumnRouteRule, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_rule_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}, {Name: "column_name_s"}},
		UpdateAll: true,
	}).Create(rule).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWColumnRouteRule) CreateInBatchColumnRouteRule(ctx context.Context, rule []*ColumnRouteRule, batchSize int) ([]*ColumnRouteRule, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_rule_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}, {Name: "column_name_s"}},
		UpdateAll: true,
	}).CreateInBatches(rule, batchSize).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWColumnRouteRule) UpdateColumnRouteRule(ctx context.Context, rule *ColumnRouteRule) (*ColumnRouteRule, error) {
	err := rw.DB(ctx).Model(&ColumnRouteRule{}).Where("task_rule_name = ? AND schema_name_s = ? AND table_name_s = ? AND column_name_s = ?",
		rule.TaskRuleName,
		rule.SchemaNameS,
		rule.TableNameS,
		rule.ColumnNameS).Save(&rule).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWColumnRouteRule) GetColumnRouteRule(ctx context.Context, rule *ColumnRouteRule) (*ColumnRouteRule, error) {
	var dataS *ColumnRouteRule
	err := rw.DB(ctx).Model(&ColumnRouteRule{}).Where("task_rule_name = ? AND schema_name_s = ? AND table_name_s = ? AND column_name_s = ?",
		rule.TaskRuleName,
		rule.SchemaNameS,
		rule.TableNameS,
		rule.ColumnNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWColumnRouteRule) ListColumnRouteRule(ctx context.Context, page uint64, pageSize uint64) ([]*ColumnRouteRule, error) {
	var dataS []*ColumnRouteRule
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&ColumnRouteRule{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWColumnRouteRule) DeleteColumnRouteRule(ctx context.Context, taskRuleName []string) error {
	err := rw.DB(ctx).Where("task_rule_name IN (?)", taskRuleName).Delete(&ColumnRouteRule{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWColumnRouteRule) IsContainedColumnRouteRuleRecord(ctx context.Context, rule *ColumnRouteRule) (bool, error) {
	err := rw.DB(ctx).Where("task_rule_name = ? AND schema_name_s = ? AND table_name_s = ? AND column_name_s = ?", rule.TaskRuleName,
		rule.SchemaNameS,
		rule.TableNameS, rule.ColumnNameS).First(&ColumnRouteRule{}).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return false, nil
		}
		return false, fmt.Errorf("get table [%s] record is contained failed: %v", rw.TableName(ctx), err)
	}
	return true, nil
}

func (rw *RWColumnRouteRule) FindColumnRouteRule(ctx context.Context, rule *ColumnRouteRule) ([]*ColumnRouteRule, error) {
	var dataS []*ColumnRouteRule
	err := rw.DB(ctx).Model(&ColumnRouteRule{}).Where("task_rule_name = ? AND schema_name_s = ? AND table_name_s = ?", rule.TaskRuleName, rule.SchemaNameS, rule.TableNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}
