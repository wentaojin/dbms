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
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}},
		UpdateAll: true,
	}).Create(rule).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWSchemaRouteRule) UpdateSchemaRouteRule(ctx context.Context, rule *SchemaRouteRule) (*SchemaRouteRule, error) {
	err := rw.DB(ctx).Model(&SchemaRouteRule{}).Where("task_name = ? AND schema_name_s = ?", rule.TaskName, rule.SchemaNameS).Save(&rule).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWSchemaRouteRule) GetSchemaRouteRule(ctx context.Context, rule *SchemaRouteRule) (*SchemaRouteRule, error) {
	var dataS *SchemaRouteRule
	err := rw.DB(ctx).Model(&SchemaRouteRule{}).Where("task_name = ?", rule.TaskName).Find(&dataS).Error
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

func (rw *RWSchemaRouteRule) DeleteSchemaRouteRule(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&SchemaRouteRule{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWSchemaRouteRule) IsContainedSchemaRouteRuleRecord(ctx context.Context, rule *SchemaRouteRule) (bool, error) {
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ?", rule.TaskName, rule.SchemaNameS).First(&SchemaRouteRule{}).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return false, nil
		}
		return false, fmt.Errorf("get table [%s] record is contained failed: %v", rw.TableName(ctx), err)
	}
	return true, nil
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
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
		UpdateAll: true,
	}).Create(rule).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWMigrateTaskTable) UpdateMigrateTaskTable(ctx context.Context, rule *MigrateTaskTable) (*MigrateTaskTable, error) {
	err := rw.DB(ctx).Model(&SchemaRouteRule{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", rule.TaskName, rule.SchemaNameS, rule.TableNameS).Save(&rule).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWMigrateTaskTable) GetMigrateTaskTable(ctx context.Context, rule *MigrateTaskTable) (*MigrateTaskTable, error) {
	var dataS *MigrateTaskTable
	err := rw.DB(ctx).Model(&SchemaRouteRule{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", rule.TaskName, rule.SchemaNameS, rule.TableNameS).Find(&dataS).Error
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

func (rw *RWMigrateTaskTable) DeleteMigrateTaskTable(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&MigrateTaskTable{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWMigrateTaskTable) IsContainedMigrateTaskTableRecord(ctx context.Context, rule *MigrateTaskTable) (bool, error) {
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", rule.TaskName, rule.SchemaNameS, rule.TableNameS).First(&MigrateTaskTable{}).Error
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
	err := rw.DB(ctx).Model(&MigrateTaskTable{}).Where("task_name = ? AND schema_name_s = ?", rule.TaskName, rule.SchemaNameS).Find(&dataS).Error
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
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
		UpdateAll: true,
	}).Create(rule).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWTableRouteRule) CreateInBatchTableRouteRule(ctx context.Context, rule []*TableRouteRule, batchSize int) ([]*TableRouteRule, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
		UpdateAll: true,
	}).CreateInBatches(rule, batchSize).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record by batch failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWTableRouteRule) UpdateTableRouteRule(ctx context.Context, rule *TableRouteRule) (*TableRouteRule, error) {
	err := rw.DB(ctx).Model(&TableRouteRule{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?",
		rule.TaskName,
		rule.SchemaNameS,
		rule.TableNameS).Save(&rule).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWTableRouteRule) GetTableRouteRule(ctx context.Context, rule *TableRouteRule) (*TableRouteRule, error) {
	var dataS *TableRouteRule
	err := rw.DB(ctx).Model(&TableRouteRule{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", rule.TaskName, rule.SchemaNameS, rule.TableNameS).Find(&dataS).Error
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

func (rw *RWTableRouteRule) DeleteTableRouteRule(ctx context.Context, taskNames []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskNames).Delete(&TableRouteRule{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWTableRouteRule) IsContainedTableRouteRuleRecord(ctx context.Context, rule *TableRouteRule) (bool, error) {
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", rule.TaskName,
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
	err := rw.DB(ctx).Model(&TableRouteRule{}).Where("task_name = ? AND schema_name_s = ?", rule.TaskName, rule.SchemaNameS).Find(&dataS).Error
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
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}, {Name: "column_name_s"}},
		UpdateAll: true,
	}).Create(rule).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWColumnRouteRule) CreateInBatchColumnRouteRule(ctx context.Context, rule []*ColumnRouteRule, batchSize int) ([]*ColumnRouteRule, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}, {Name: "column_name_s"}},
		UpdateAll: true,
	}).CreateInBatches(rule, batchSize).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWColumnRouteRule) UpdateColumnRouteRule(ctx context.Context, rule *ColumnRouteRule) (*ColumnRouteRule, error) {
	err := rw.DB(ctx).Model(&ColumnRouteRule{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ? AND column_name_s = ?",
		rule.TaskName,
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
	err := rw.DB(ctx).Model(&ColumnRouteRule{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ? AND column_name_s = ?",
		rule.TaskName,
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

func (rw *RWColumnRouteRule) DeleteColumnRouteRule(ctx context.Context, taskNames []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskNames).Delete(&ColumnRouteRule{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWColumnRouteRule) IsContainedColumnRouteRuleRecord(ctx context.Context, rule *ColumnRouteRule) (bool, error) {
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ? AND column_name_s = ?", rule.TaskName,
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
	err := rw.DB(ctx).Model(&ColumnRouteRule{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", rule.TaskName, rule.SchemaNameS, rule.TableNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

type RWTableMigrateRule struct {
	common.GormDB
}

func NewTableMigrateRuleRW(db *gorm.DB) *RWTableMigrateRule {
	m := &RWTableMigrateRule{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWTableMigrateRule) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(TableMigrateRule{}).Name())
}

func (rw *RWTableMigrateRule) CreateTableMigrateRule(ctx context.Context, rule *TableMigrateRule) (*TableMigrateRule, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
		UpdateAll: true,
	}).Create(rule).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWTableMigrateRule) CreateInBatchTableMigrateRule(ctx context.Context, rule []*TableMigrateRule, batchSize int) ([]*TableMigrateRule, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
		UpdateAll: true,
	}).CreateInBatches(rule, batchSize).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWTableMigrateRule) UpdateTableMigrateRule(ctx context.Context, rule *TableMigrateRule) (*TableMigrateRule, error) {
	err := rw.DB(ctx).Model(&TableMigrateRule{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?",
		rule.TaskName,
		rule.SchemaNameS,
		rule.TableNameS).Save(&rule).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWTableMigrateRule) GetTableMigrateRule(ctx context.Context, rule *TableMigrateRule) (*TableMigrateRule, error) {
	var dataS *TableMigrateRule
	err := rw.DB(ctx).Model(&TableMigrateRule{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?",
		rule.TaskName,
		rule.SchemaNameS,
		rule.TableNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWTableMigrateRule) IsContainedTableMigrateRuleRecord(ctx context.Context, rule *TableMigrateRule) (bool, error) {
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?",
		rule.TaskName,
		rule.SchemaNameS,
		rule.TableNameS).First(&TableMigrateRule{}).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return false, nil
		}
		return false, fmt.Errorf("get table [%s] record is contained failed: %v", rw.TableName(ctx), err)
	}
	return true, nil
}

func (rw *RWTableMigrateRule) DeleteTableMigrateRule(ctx context.Context, taskNames []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskNames).Delete(&TableMigrateRule{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWTableMigrateRule) FindTableMigrateRule(ctx context.Context, rule *TableMigrateRule) ([]*TableMigrateRule, error) {
	var dataS []*TableMigrateRule
	err := rw.DB(ctx).Model(&TableMigrateRule{}).Where("task_name = ? AND schema_name_s = ?", rule.TaskName, rule.SchemaNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

type RWSqlRouteRule struct {
	common.GormDB
}

func NewSqlRouteRuleRW(db *gorm.DB) *RWSqlRouteRule {
	m := &RWSqlRouteRule{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWSqlRouteRule) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(SqlRouteRule{}).Name())
}

func (rw *RWSqlRouteRule) CreateSqlRouteRule(ctx context.Context, rule *SqlRouteRule) (*SqlRouteRule, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "schema_name_t"}, {Name: "table_name_t"}},
		UpdateAll: true,
	}).Create(rule).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWSqlRouteRule) CreateInBatchSqlRouteRule(ctx context.Context, rule []*SqlRouteRule, batchSize int) ([]*SqlRouteRule, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "schema_name_t"}, {Name: "table_name_t"}},
		UpdateAll: true,
	}).CreateInBatches(rule, batchSize).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWSqlRouteRule) UpdateSqlRouteRule(ctx context.Context, rule *SqlRouteRule) (*SqlRouteRule, error) {
	err := rw.DB(ctx).Model(&TableMigrateRule{}).Where("task_name = ? AND schema_name_t = ? AND table_name_t = ?",
		rule.TaskName,
		rule.SchemaNameT,
		rule.TableNameT).Save(&rule).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWSqlRouteRule) GetSqlRouteRule(ctx context.Context, rule *SqlRouteRule) (*SqlRouteRule, error) {
	var dataS *SqlRouteRule
	err := rw.DB(ctx).Model(&SqlRouteRule{}).Where("task_name = ? AND schema_name_t = ? AND table_name_t = ?",
		rule.TaskName,
		rule.SchemaNameT,
		rule.TableNameT).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSqlRouteRule) DeleteSqlRouteRule(ctx context.Context, taskNames []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskNames).Delete(&SqlRouteRule{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWSqlRouteRule) FindSqlRouteRule(ctx context.Context, rule *SqlRouteRule) ([]*SqlRouteRule, error) {
	var dataS []*SqlRouteRule
	err := rw.DB(ctx).Model(&SqlRouteRule{}).Where("task_name = ?", rule.TaskName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSqlRouteRule) FindSqlRouteRuleGroupBySchemaTable(ctx context.Context) ([]*SqlRouteRuleGroupSchemaTableTResult, error) {
	var dataS []*SqlRouteRuleGroupSchemaTableTResult
	err := rw.DB(ctx).Model(&SqlRouteRule{}).Select("task_name,schema_name_t,table_name_t,count(1) as row_totals").Group("task_name,schema_name_t,table_name_t").Order("row_totals desc").Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] group by the the task_name and schema_name_t and table_name_t record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}
