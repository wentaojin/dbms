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
	"fmt"
	"github.com/wentaojin/dbms/utils/stringutil"
	"golang.org/x/sync/errgroup"
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

func (rw *RWMigrateTaskTable) DeleteMigrateTaskTableByTaskIsExclude(ctx context.Context, rule *MigrateTaskTable) error {
	err := rw.DB(ctx).Where("task_name = ? AND is_exclude = ?", rule.TaskName, rule.IsExclude).Delete(&MigrateTaskTable{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task and exclude record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWMigrateTaskTable) FindMigrateTaskTable(ctx context.Context, rule *MigrateTaskTable) ([]*MigrateTaskTable, error) {
	var dataS []*MigrateTaskTable
	err := rw.DB(ctx).Model(&MigrateTaskTable{}).Where("task_name = ? AND schema_name_s = ?", rule.TaskName, rule.SchemaNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

type RWMigrateTaskSequence struct {
	common.GormDB
}

func NewMigrateTaskSequenceRW(db *gorm.DB) *RWMigrateTaskSequence {
	m := &RWMigrateTaskSequence{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWMigrateTaskSequence) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(MigrateTaskSequence{}).Name())
}

func (rw *RWMigrateTaskSequence) CreateMigrateTaskSequence(ctx context.Context, rule *MigrateTaskSequence) (*MigrateTaskSequence, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "sequence_name_s"}},
		UpdateAll: true,
	}).Create(rule).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWMigrateTaskSequence) UpdateMigrateTaskSequence(ctx context.Context, rule *MigrateTaskSequence) (*MigrateTaskSequence, error) {
	err := rw.DB(ctx).Model(&SchemaRouteRule{}).Where("task_name = ? AND schema_name_s = ? AND sequence_name_s = ?", rule.TaskName, rule.SchemaNameS, rule.SequenceNameS).Save(&rule).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWMigrateTaskSequence) GetMigrateTaskSequence(ctx context.Context, rule *MigrateTaskSequence) (*MigrateTaskSequence, error) {
	var dataS *MigrateTaskSequence
	err := rw.DB(ctx).Model(&SchemaRouteRule{}).Where("task_name = ? AND schema_name_s = ? AND sequence_name_s = ?", rule.TaskName, rule.SchemaNameS, rule.SequenceNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWMigrateTaskSequence) ListMigrateTaskSequence(ctx context.Context, page uint64, pageSize uint64) ([]*MigrateTaskSequence, error) {
	var dataS []*MigrateTaskSequence
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&MigrateTaskSequence{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWMigrateTaskSequence) DeleteMigrateTaskSequence(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&MigrateTaskSequence{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWMigrateTaskSequence) DeleteMigrateTaskSequenceByTaskIsExclude(ctx context.Context, rule *MigrateTaskSequence) error {
	err := rw.DB(ctx).Where("task_name = ? AND is_exclude = ?", rule.TaskName, rule.IsExclude).Delete(&MigrateTaskSequence{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task and exclude record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWMigrateTaskSequence) FindMigrateTaskSequence(ctx context.Context, rule *MigrateTaskSequence) ([]*MigrateTaskSequence, error) {
	var dataS []*MigrateTaskSequence
	err := rw.DB(ctx).Model(&MigrateTaskSequence{}).Where("task_name = ? AND schema_name_s = ?", rule.TaskName, rule.SchemaNameS).Find(&dataS).Error
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

func (rw *RWTableRouteRule) CreateInBatchTableRouteRule(ctx context.Context, rule []*TableRouteRule, thread, batchSize int) ([]*TableRouteRule, error) {
	values := stringutil.AnySliceSplit(rule, batchSize)
	g := errgroup.Group{}
	g.SetLimit(thread)
	for _, v := range values {
		val := v
		g.Go(func() error {
			err := rw.DB(ctx).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
				UpdateAll: true,
			}).Create(val).Error
			if err != nil {
				return fmt.Errorf("create table [%s] record by batch failed: %v", rw.TableName(ctx), err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
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

func (rw *RWColumnRouteRule) CreateInBatchColumnRouteRule(ctx context.Context, rule []*ColumnRouteRule, thread, batchSize int) ([]*ColumnRouteRule, error) {
	values := stringutil.AnySliceSplit(rule, batchSize)
	g := &errgroup.Group{}
	g.SetLimit(thread)
	for _, v := range values {
		val := v
		g.Go(func() error {
			err := rw.DB(ctx).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}, {Name: "column_name_s"}},
				UpdateAll: true,
			}).Create(val).Error
			if err != nil {
				return fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
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

func (rw *RWColumnRouteRule) FindColumnRouteRule(ctx context.Context, rule *ColumnRouteRule) ([]*ColumnRouteRule, error) {
	var dataS []*ColumnRouteRule
	err := rw.DB(ctx).Model(&ColumnRouteRule{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", rule.TaskName, rule.SchemaNameS, rule.TableNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

type RWDataMigrateRule struct {
	common.GormDB
}

func NewDataMigrateRuleRW(db *gorm.DB) *RWDataMigrateRule {
	m := &RWDataMigrateRule{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWDataMigrateRule) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(DataMigrateRule{}).Name())
}

func (rw *RWDataMigrateRule) CreateDataMigrateRule(ctx context.Context, rule *DataMigrateRule) (*DataMigrateRule, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
		UpdateAll: true,
	}).Create(rule).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWDataMigrateRule) CreateInBatchDataMigrateRule(ctx context.Context, rule []*DataMigrateRule, thread, batchSize int) ([]*DataMigrateRule, error) {
	values := stringutil.AnySliceSplit(rule, batchSize)
	g := &errgroup.Group{}
	g.SetLimit(thread)

	for _, v := range values {
		val := v
		g.Go(func() error {
			err := rw.DB(ctx).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
				UpdateAll: true,
			}).Create(val).Error
			if err != nil {
				return fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return rule, nil
}

func (rw *RWDataMigrateRule) UpdateDataMigrateRule(ctx context.Context, rule *DataMigrateRule) (*DataMigrateRule, error) {
	err := rw.DB(ctx).Model(&DataMigrateRule{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?",
		rule.TaskName,
		rule.SchemaNameS,
		rule.TableNameS).Save(&rule).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWDataMigrateRule) GetDataMigrateRule(ctx context.Context, rule *DataMigrateRule) (*DataMigrateRule, error) {
	var dataS *DataMigrateRule
	err := rw.DB(ctx).Model(&DataMigrateRule{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?",
		rule.TaskName,
		rule.SchemaNameS,
		rule.TableNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataMigrateRule) DeleteDataMigrateRule(ctx context.Context, taskNames []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskNames).Delete(&DataMigrateRule{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWDataMigrateRule) FindDataMigrateRule(ctx context.Context, rule *DataMigrateRule) ([]*DataMigrateRule, error) {
	var dataS []*DataMigrateRule
	err := rw.DB(ctx).Model(&DataMigrateRule{}).Where("task_name = ? AND schema_name_s = ?", rule.TaskName, rule.SchemaNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataMigrateRule) IsContainedDataMigrateRuleRecord(ctx context.Context, rule *DataMigrateRule) (bool, error) {
	var dataS []*DataMigrateRule
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?",
		rule.TaskName,
		rule.SchemaNameS,
		rule.TableNameS).Find(&dataS).Error
	if err != nil {
		return false, fmt.Errorf("get table [%s] record is contained failed: %v", rw.TableName(ctx), err)
	}
	if len(dataS) == 0 {
		return false, nil
	}
	return true, nil
}

type RWDataCompareRule struct {
	common.GormDB
}

func NewDataCompareRuleRW(db *gorm.DB) *RWDataCompareRule {
	m := &RWDataCompareRule{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWDataCompareRule) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(DataCompareRule{}).Name())
}

func (rw *RWDataCompareRule) CreateDataCompareRule(ctx context.Context, rule *DataCompareRule) (*DataCompareRule, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
		UpdateAll: true,
	}).Create(rule).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWDataCompareRule) CreateInBatchDataCompareRule(ctx context.Context, rule []*DataCompareRule, thread, batchSize int) ([]*DataCompareRule, error) {
	values := stringutil.AnySliceSplit(rule, batchSize)
	g := &errgroup.Group{}
	g.SetLimit(thread)

	for _, v := range values {
		val := v
		g.Go(func() error {
			err := rw.DB(ctx).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
				UpdateAll: true,
			}).Create(val).Error
			if err != nil {
				return fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return rule, nil
}

func (rw *RWDataCompareRule) UpdateDataCompareRule(ctx context.Context, rule *DataCompareRule) (*DataCompareRule, error) {
	err := rw.DB(ctx).Model(&DataCompareRule{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?",
		rule.TaskName,
		rule.SchemaNameS,
		rule.TableNameS).Save(&rule).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWDataCompareRule) GetDataCompareRule(ctx context.Context, rule *DataCompareRule) (*DataCompareRule, error) {
	var dataS *DataCompareRule
	err := rw.DB(ctx).Model(&DataCompareRule{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?",
		rule.TaskName,
		rule.SchemaNameS,
		rule.TableNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataCompareRule) DeleteDataCompareRule(ctx context.Context, taskNames []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskNames).Delete(&DataCompareRule{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWDataCompareRule) FindDataCompareRule(ctx context.Context, rule *DataCompareRule) ([]*DataCompareRule, error) {
	var dataS []*DataCompareRule
	err := rw.DB(ctx).Model(&DataCompareRule{}).Where("task_name = ? AND schema_name_s = ?", rule.TaskName, rule.SchemaNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataCompareRule) IsContainedDataCompareRuleRecord(ctx context.Context, rule *DataCompareRule) (bool, error) {
	var dataS []*DataCompareRule
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?",
		rule.TaskName,
		rule.SchemaNameS,
		rule.TableNameS).Find(&dataS).Error
	if err != nil {
		return false, fmt.Errorf("get table [%s] record is contained failed: %v", rw.TableName(ctx), err)
	}
	if len(dataS) == 0 {
		return false, nil
	}
	return true, nil
}

type RWSqlMigrateRule struct {
	common.GormDB
}

func NewSqlMigrateRuleRW(db *gorm.DB) *RWSqlMigrateRule {
	m := &RWSqlMigrateRule{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWSqlMigrateRule) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(SqlMigrateRule{}).Name())
}

func (rw *RWSqlMigrateRule) CreateSqlMigrateRule(ctx context.Context, rule *SqlMigrateRule) (*SqlMigrateRule, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "schema_name_t"}, {Name: "table_name_t"}},
		UpdateAll: true,
	}).Create(rule).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWSqlMigrateRule) CreateInBatchSqlMigrateRule(ctx context.Context, rule []*SqlMigrateRule, thread, batchSize int) ([]*SqlMigrateRule, error) {
	values := stringutil.AnySliceSplit(rule, batchSize)
	g := &errgroup.Group{}
	g.SetLimit(thread)

	for _, v := range values {
		val := v
		g.Go(func() error {
			err := rw.DB(ctx).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "schema_name_t"}, {Name: "table_name_t"}},
				UpdateAll: true,
			}).Create(val).Error
			if err != nil {
				return fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return rule, nil
}

func (rw *RWSqlMigrateRule) UpdateSqlMigrateRule(ctx context.Context, rule *SqlMigrateRule) (*SqlMigrateRule, error) {
	err := rw.DB(ctx).Model(&DataMigrateRule{}).Where("task_name = ? AND schema_name_t = ? AND table_name_t = ?",
		rule.TaskName,
		rule.SchemaNameT,
		rule.TableNameT).Save(&rule).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWSqlMigrateRule) GetSqlMigrateRule(ctx context.Context, rule *SqlMigrateRule) (*SqlMigrateRule, error) {
	var dataS *SqlMigrateRule
	err := rw.DB(ctx).Model(&SqlMigrateRule{}).Where("task_name = ? AND schema_name_t = ? AND table_name_t = ?",
		rule.TaskName,
		rule.SchemaNameT,
		rule.TableNameT).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSqlMigrateRule) DeleteSqlMigrateRule(ctx context.Context, taskNames []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskNames).Delete(&SqlMigrateRule{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWSqlMigrateRule) FindSqlMigrateRule(ctx context.Context, rule *SqlMigrateRule) ([]*SqlMigrateRule, error) {
	var dataS []*SqlMigrateRule
	err := rw.DB(ctx).Model(&SqlMigrateRule{}).Where("task_name = ?", rule.TaskName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSqlMigrateRule) FindSqlMigrateRuleGroupBySchemaTable(ctx context.Context) ([]*SqlMigrateRuleGroupSchemaTableTResult, error) {
	var dataS []*SqlMigrateRuleGroupSchemaTableTResult
	err := rw.DB(ctx).Model(&SqlMigrateRule{}).Select("task_name,schema_name_t,table_name_t,count(1) as row_totals").Group("task_name,schema_name_t,table_name_t").Order("row_totals desc").Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] group by the the task_name and schema_name_t and table_name_t record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

type RWDataScanRule struct {
	common.GormDB
}

func NewDataScanRuleRW(db *gorm.DB) *RWDataScanRule {
	m := &RWDataScanRule{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWDataScanRule) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(DataScanRule{}).Name())
}

func (rw *RWDataScanRule) CreateDataScanRule(ctx context.Context, rule *DataScanRule) (*DataScanRule, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
		UpdateAll: true,
	}).Create(rule).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWDataScanRule) CreateInBatchDataScanRule(ctx context.Context, rule []*DataScanRule, thread, batchSize int) ([]*DataScanRule, error) {
	values := stringutil.AnySliceSplit(rule, batchSize)
	g := &errgroup.Group{}
	g.SetLimit(thread)
	for _, v := range values {
		val := v
		g.Go(func() error {
			err := rw.DB(ctx).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
				UpdateAll: true,
			}).Create(val).Error
			if err != nil {
				return fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return rule, nil
}

func (rw *RWDataScanRule) UpdateDataScanRule(ctx context.Context, rule *DataScanRule) (*DataScanRule, error) {
	err := rw.DB(ctx).Model(&DataScanRule{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?",
		rule.TaskName,
		rule.SchemaNameS,
		rule.TableNameS).Save(&rule).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rule, nil
}

func (rw *RWDataScanRule) GetDataScanRule(ctx context.Context, rule *DataScanRule) (*DataScanRule, error) {
	var dataS *DataScanRule
	err := rw.DB(ctx).Model(&DataScanRule{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?",
		rule.TaskName,
		rule.SchemaNameS,
		rule.TableNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataScanRule) DeleteDataScanRule(ctx context.Context, taskNames []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskNames).Delete(&DataScanRule{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWDataScanRule) FindDataScanRule(ctx context.Context, rule *DataScanRule) ([]*DataScanRule, error) {
	var dataS []*DataScanRule
	err := rw.DB(ctx).Model(&DataScanRule{}).Where("task_name = ? AND schema_name_s = ?", rule.TaskName, rule.SchemaNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataScanRule) IsContainedDataScanRuleRecord(ctx context.Context, rule *DataScanRule) (bool, error) {
	var dataS []*DataScanRule
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?",
		rule.TaskName,
		rule.SchemaNameS,
		rule.TableNameS).Find(&dataS).Error
	if err != nil {
		return false, fmt.Errorf("get table [%s] record is contained failed: %v", rw.TableName(ctx), err)
	}
	if len(dataS) == 0 {
		return false, nil
	}
	return true, nil
}
