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
	"context"
	"errors"
	"fmt"
	"github.com/wentaojin/dbms/utils/stringutil"
	"golang.org/x/sync/errgroup"
	"reflect"

	"gorm.io/gorm/clause"

	"github.com/wentaojin/dbms/model/common"
	"gorm.io/gorm"
)

type RWTask struct {
	common.GormDB
}

func NewTaskRW(db *gorm.DB) *RWTask {
	m := &RWTask{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWTask) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(Task{}).Name())
}

func (rw *RWTask) CreateTask(ctx context.Context, task *Task) (*Task, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWTask) UpdateTask(ctx context.Context, task *Task, updates map[string]interface{}) (*Task, error) {
	err := rw.DB(ctx).Model(&Task{}).Where("task_name = ?", task.TaskName).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWTask) GetTask(ctx context.Context, task *Task) (*Task, error) {
	var dataS *Task
	err := rw.DB(ctx).Model(&Task{}).Where("task_name = ?", task.TaskName).First(&dataS).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return dataS, nil
		}
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWTask) ListTask(ctx context.Context, page uint64, pageSize uint64) ([]*Task, error) {
	var dataS []*Task
	if page == 0 && pageSize == 0 {
		err := rw.DB(ctx).Model(&Task{}).Find(&dataS).Error
		if err != nil {
			return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
		}
		return dataS, nil
	}
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&Task{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWTask) DeleteTask(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&Task{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

type RWLog struct {
	common.GormDB
}

func NewLogRW(db *gorm.DB) *RWLog {
	m := &RWLog{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWLog) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(Log{}).Name())
}

func (rw *RWLog) CreateLog(ctx context.Context, task *Log) (*Log, error) {
	err := rw.DB(ctx).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWLog) UpdateLog(ctx context.Context, task *Log, updates map[string]interface{}) (*Log, error) {
	err := rw.DB(ctx).Model(&Log{}).Where("task_name = ?", task.ID).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWLog) QueryLog(ctx context.Context, task *Log, last int) ([]*Log, error) {
	var dataS []*Log
	err := rw.DB(ctx).Model(&Log{}).Where("task_name = ? order by created_at desc", task.TaskName).Limit(last).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWLog) ListLog(ctx context.Context, page uint64, pageSize uint64) ([]*Log, error) {
	var dataS []*Log
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&Log{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWLog) DeleteLog(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&Log{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

type RWAssessMigrateTask struct {
	common.GormDB
}

func NewAssessMigrateTaskRW(db *gorm.DB) *RWAssessMigrateTask {
	m := &RWAssessMigrateTask{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWAssessMigrateTask) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(AssessMigrateTask{}).Name())
}

func (rw *RWAssessMigrateTask) CreateAssessMigrateTask(ctx context.Context, task *AssessMigrateTask) (*AssessMigrateTask, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWAssessMigrateTask) GetAssessMigrateTask(ctx context.Context, task *AssessMigrateTask) (*AssessMigrateTask, error) {
	var dataS *AssessMigrateTask
	err := rw.DB(ctx).Model(&AssessMigrateTask{}).Where("task_name = ? AND schema_name_s = ?", task.TaskName, task.SchemaNameS).First(&dataS).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return dataS, nil
		}
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWAssessMigrateTask) QueryAssessMigrateTask(ctx context.Context, task *AssessMigrateTask) ([]*AssessMigrateTask, error) {
	var dataS []*AssessMigrateTask
	err := rw.DB(ctx).Model(&AssessMigrateTask{}).Where("task_name = ? AND task_status = ?", task.TaskName, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWAssessMigrateTask) UpdateAssessMigrateTask(ctx context.Context, task *AssessMigrateTask, updates map[string]interface{}) (*AssessMigrateTask, error) {
	err := rw.DB(ctx).Model(&AssessMigrateTask{}).Where("task_name = ? AND schema_name_s = ?", task.TaskName, task.SchemaNameS).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWAssessMigrateTask) ListAssessMigrateTask(ctx context.Context, page uint64, pageSize uint64) ([]*AssessMigrateTask, error) {
	var dataS []*AssessMigrateTask
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&AssessMigrateTask{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWAssessMigrateTask) DeleteAssessMigrateTask(ctx context.Context, taskID uint64) error {
	err := rw.DB(ctx).Where("id = ?", taskID).Delete(&AssessMigrateTask{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] id [%v] record failed: %v", rw.TableName(ctx), taskID, err)
	}
	return nil
}

func (rw *RWAssessMigrateTask) DeleteAssessMigrateTaskName(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&AssessMigrateTask{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task [%v] record failed: %v", rw.TableName(ctx), taskName, err)
	}
	return nil
}

type RWStructMigrateSummary struct {
	common.GormDB
}

func NewStructMigrateSummaryRW(db *gorm.DB) *RWStructMigrateSummary {
	m := &RWStructMigrateSummary{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWStructMigrateSummary) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(StructMigrateSummary{}).Name())
}

func (rw *RWStructMigrateSummary) CreateStructMigrateSummary(ctx context.Context, task *StructMigrateSummary) (*StructMigrateSummary, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWStructMigrateSummary) GetStructMigrateSummary(ctx context.Context, task *StructMigrateSummary) (*StructMigrateSummary, error) {
	var dataS *StructMigrateSummary
	err := rw.DB(ctx).Model(&StructMigrateSummary{}).Where("task_name = ? AND schema_name_s = ?", task.TaskName, task.SchemaNameS).First(&dataS).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return dataS, nil
		}
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateSummary) UpdateStructMigrateSummary(ctx context.Context, task *StructMigrateSummary, updates map[string]interface{}) (*StructMigrateSummary, error) {
	err := rw.DB(ctx).Model(&StructMigrateSummary{}).Where("task_name = ? AND schema_name_s = ?", task.TaskName, task.SchemaNameS).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWStructMigrateSummary) FindStructMigrateSummary(ctx context.Context, task *StructMigrateSummary) ([]*StructMigrateSummary, error) {
	var dataS []*StructMigrateSummary
	err := rw.DB(ctx).Model(&StructMigrateSummary{}).Where("task_name = ? AND schema_name_s = ?", task.TaskName, task.SchemaNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateSummary) DeleteStructMigrateSummary(ctx context.Context, task *StructMigrateSummary) error {
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ?", task.TaskName, task.SchemaNameS).Delete(&StructMigrateSummary{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWStructMigrateSummary) DeleteStructMigrateSummaryName(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&StructMigrateSummary{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task [%v] record failed: %v", rw.TableName(ctx), taskName, err)
	}
	return nil
}

type RWSequenceMigrateSummary struct {
	common.GormDB
}

func NewSequenceMigrateSummaryRW(db *gorm.DB) *RWSequenceMigrateSummary {
	m := &RWSequenceMigrateSummary{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWSequenceMigrateSummary) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(SequenceMigrateSummary{}).Name())
}

func (rw *RWSequenceMigrateSummary) CreateSequenceMigrateSummary(ctx context.Context, task *SequenceMigrateSummary) (*SequenceMigrateSummary, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWSequenceMigrateSummary) GetSequenceMigrateSummary(ctx context.Context, task *SequenceMigrateSummary) (*SequenceMigrateSummary, error) {
	var dataS *SequenceMigrateSummary
	err := rw.DB(ctx).Model(&SequenceMigrateSummary{}).Where("task_name = ? AND schema_name_s = ?", task.TaskName, task.SchemaNameS).First(&dataS).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return dataS, nil
		}
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSequenceMigrateSummary) UpdateSequenceMigrateSummary(ctx context.Context, task *SequenceMigrateSummary, updates map[string]interface{}) (*SequenceMigrateSummary, error) {
	err := rw.DB(ctx).Model(&SequenceMigrateSummary{}).Where("task_name = ? AND schema_name_s = ?", task.TaskName, task.SchemaNameS).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWSequenceMigrateSummary) FindSequenceMigrateSummary(ctx context.Context, task *SequenceMigrateSummary) ([]*SequenceMigrateSummary, error) {
	var dataS []*SequenceMigrateSummary
	err := rw.DB(ctx).Model(&SequenceMigrateSummary{}).Where("task_name = ? AND schema_name_s = ?", task.TaskName, task.SchemaNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSequenceMigrateSummary) DeleteSequenceMigrateSummary(ctx context.Context, task *SequenceMigrateSummary) error {
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ?", task.TaskName, task.SchemaNameS).Delete(&SequenceMigrateSummary{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWSequenceMigrateSummary) DeleteSequenceMigrateSummaryName(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&SequenceMigrateSummary{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task [%v] record failed: %v", rw.TableName(ctx), taskName, err)
	}
	return nil
}

type RWStructMigrateTask struct {
	common.GormDB
}

func NewStructMigrateTaskRW(db *gorm.DB) *RWStructMigrateTask {
	m := &RWStructMigrateTask{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWStructMigrateTask) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(StructMigrateTask{}).Name())
}

func (rw *RWStructMigrateTask) CreateStructMigrateTask(ctx context.Context, task *StructMigrateTask) (*StructMigrateTask, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWStructMigrateTask) GetStructMigrateTask(ctx context.Context, task *StructMigrateTask) ([]*StructMigrateTask, error) {
	var dataS []*StructMigrateTask
	err := rw.DB(ctx).Model(&StructMigrateTask{}).Where("task_name = ? AND schema_name_s = ? AND task_status = ?", task.TaskName, task.SchemaNameS, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateTask) UpdateStructMigrateTask(ctx context.Context, task *StructMigrateTask, updates map[string]interface{}) (*StructMigrateTask, error) {
	err := rw.DB(ctx).Model(&StructMigrateTask{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWStructMigrateTask) GetStructMigrateTaskTable(ctx context.Context, task *StructMigrateTask) ([]*StructMigrateTask, error) {
	var dataS []*StructMigrateTask

	err := rw.DB(ctx).Model(&StructMigrateTask{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] table record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateTask) BatchUpdateStructMigrateTask(ctx context.Context, task *StructMigrateTask, updates map[string]interface{}) (*StructMigrateTask, error) {
	err := rw.DB(ctx).Model(&StructMigrateTask{}).Where("task_name = ? AND task_status = ?", task.TaskName, task.TaskStatus).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWStructMigrateTask) QueryStructMigrateTask(ctx context.Context, task *StructMigrateTask) ([]*StructMigrateTask, error) {
	var dataS []*StructMigrateTask
	err := rw.DB(ctx).Model(&StructMigrateTask{}).Where("task_name = ? AND task_status = ?", task.TaskName, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateTask) FindStructMigrateTask(ctx context.Context, task *StructMigrateTask) ([]*StructMigrateTask, error) {
	var dataS []*StructMigrateTask
	err := rw.DB(ctx).Model(&StructMigrateTask{}).Where("task_name = ? AND task_status = ?", task.TaskName, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateTask) FindStructMigrateTaskGroupByTaskStatus(ctx context.Context, taskName string) ([]*StructGroupStatusResult, error) {
	var dataS []*StructGroupStatusResult
	err := rw.DB(ctx).Model(&StructMigrateTask{}).Select("task_name,task_status,count(1) as status_counts").Where("task_name = ?", taskName).Group("task_name,task_status").Order("status_counts desc").Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] group by the task_name and task_status record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateTask) BatchFindStructMigrateTask(ctx context.Context, task *StructMigrateTask) ([]*StructMigrateTask, error) {
	var dataS []*StructMigrateTask
	err := rw.DB(ctx).Model(&StructMigrateTask{}).Where("task_name = ?", task.TaskName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("batch find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateTask) ListStructMigrateTask(ctx context.Context, page uint64, pageSize uint64) ([]*StructMigrateTask, error) {
	var dataS []*StructMigrateTask
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&StructMigrateTask{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructMigrateTask) DeleteStructMigrateTask(ctx context.Context, taskID uint64) error {
	err := rw.DB(ctx).Where("id = ?", taskID).Delete(&StructMigrateTask{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] id [%v] record failed: %v", rw.TableName(ctx), taskID, err)
	}
	return nil
}

func (rw *RWStructMigrateTask) DeleteStructMigrateTaskName(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&StructMigrateTask{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task [%v] record failed: %v", rw.TableName(ctx), taskName, err)
	}
	return nil
}

type RWSchemaMigrateTask struct {
	common.GormDB
}

func NewSchemaMigrateTaskRW(db *gorm.DB) *RWSchemaMigrateTask {
	m := &RWSchemaMigrateTask{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWSchemaMigrateTask) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(SchemaMigrateTask{}).Name())
}

func (rw *RWSchemaMigrateTask) CreateSchemaMigrateTask(ctx context.Context, task *SchemaMigrateTask) (*SchemaMigrateTask, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWSchemaMigrateTask) GetSchemaMigrateTask(ctx context.Context, task *SchemaMigrateTask) ([]*SchemaMigrateTask, error) {
	var dataS []*SchemaMigrateTask
	err := rw.DB(ctx).Model(&SchemaMigrateTask{}).Where("task_name = ? AND schema_name_s = ? AND task_status = ?", task.TaskName, task.SchemaNameS, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSchemaMigrateTask) UpdateSchemaMigrateTask(ctx context.Context, task *SchemaMigrateTask, updates map[string]interface{}) (*SchemaMigrateTask, error) {
	err := rw.DB(ctx).Model(&SchemaMigrateTask{}).Where("task_name = ? AND schema_name_s = ?", task.TaskName, task.SchemaNameS).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWSchemaMigrateTask) GetSchemaMigrateTaskTable(ctx context.Context, task *SchemaMigrateTask) ([]*SchemaMigrateTask, error) {
	var dataS []*SchemaMigrateTask

	err := rw.DB(ctx).Model(&SchemaMigrateTask{}).Where("task_name = ? AND schema_name_s = ? AND task_status = ?", task.TaskName, task.SchemaNameS, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] table record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSchemaMigrateTask) BatchUpdateSchemaMigrateTask(ctx context.Context, task *SchemaMigrateTask, updates map[string]interface{}) (*SchemaMigrateTask, error) {
	err := rw.DB(ctx).Model(&SchemaMigrateTask{}).Where("task_name = ? AND task_status = ?", task.TaskName, task.TaskStatus).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWSchemaMigrateTask) QuerySchemaMigrateTask(ctx context.Context, task *SchemaMigrateTask) ([]*SchemaMigrateTask, error) {
	var dataS []*SchemaMigrateTask
	err := rw.DB(ctx).Model(&SchemaMigrateTask{}).Where("task_name = ? AND task_status = ?", task.TaskName, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSchemaMigrateTask) FindSchemaMigrateTask(ctx context.Context, task *SchemaMigrateTask) ([]*SchemaMigrateTask, error) {
	var dataS []*SchemaMigrateTask
	err := rw.DB(ctx).Model(&SchemaMigrateTask{}).Where("task_name = ? AND task_status = ?", task.TaskName, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSchemaMigrateTask) FindSchemaMigrateTaskGroupByTaskStatus(ctx context.Context, taskName string) ([]*StructGroupStatusResult, error) {
	var dataS []*StructGroupStatusResult
	err := rw.DB(ctx).Model(&SchemaMigrateTask{}).Select("task_name,task_status,count(1) as status_counts").Where("task_name = ?", taskName).Group("task_name,task_status").Order("status_counts desc").Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] group by the task_name and task_status record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSchemaMigrateTask) BatchFindSchemaMigrateTask(ctx context.Context, task *SchemaMigrateTask) ([]*SchemaMigrateTask, error) {
	var dataS []*SchemaMigrateTask
	err := rw.DB(ctx).Model(&SchemaMigrateTask{}).Where("task_name = ?", task.TaskName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("batch find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSchemaMigrateTask) ListSchemaMigrateTask(ctx context.Context, page uint64, pageSize uint64) ([]*SchemaMigrateTask, error) {
	var dataS []*SchemaMigrateTask
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&SchemaMigrateTask{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSchemaMigrateTask) DeleteSchemaMigrateTask(ctx context.Context, taskID uint64) error {
	err := rw.DB(ctx).Where("id = ?", taskID).Delete(&SchemaMigrateTask{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] id [%v] record failed: %v", rw.TableName(ctx), taskID, err)
	}
	return nil
}

func (rw *RWSchemaMigrateTask) DeleteSchemaMigrateTaskName(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&SchemaMigrateTask{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task [%v] record failed: %v", rw.TableName(ctx), taskName, err)
	}
	return nil
}

type RWSequenceMigrateTask struct {
	common.GormDB
}

func NewSequenceMigrateTaskRW(db *gorm.DB) *RWSequenceMigrateTask {
	m := &RWSequenceMigrateTask{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWSequenceMigrateTask) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(SequenceMigrateTask{}).Name())
}

func (rw *RWSequenceMigrateTask) CreateSequenceMigrateTask(ctx context.Context, task *SequenceMigrateTask) (*SequenceMigrateTask, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "sequence_name_s"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWSequenceMigrateTask) GetSequenceMigrateTask(ctx context.Context, task *SequenceMigrateTask) ([]*SequenceMigrateTask, error) {
	var dataS []*SequenceMigrateTask
	err := rw.DB(ctx).Model(&SequenceMigrateTask{}).Where("task_name = ? AND schema_name_s = ? AND task_status = ?", task.TaskName, task.SchemaNameS, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSequenceMigrateTask) UpdateSequenceMigrateTask(ctx context.Context, task *SequenceMigrateTask, updates map[string]interface{}) (*SequenceMigrateTask, error) {
	err := rw.DB(ctx).Model(&SequenceMigrateTask{}).Where("task_name = ? AND schema_name_s = ? AND sequence_name_s = ?", task.TaskName, task.SchemaNameS, task.SequenceNameS).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWSequenceMigrateTask) GetSequenceMigrateTaskTable(ctx context.Context, task *SequenceMigrateTask) ([]*SequenceMigrateTask, error) {
	var dataS []*SequenceMigrateTask

	err := rw.DB(ctx).Model(&SequenceMigrateTask{}).Where("task_name = ? AND schema_name_s = ? AND sequence_name_s = ?", task.TaskName, task.SchemaNameS, task.SequenceNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] table record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSequenceMigrateTask) BatchUpdateSequenceMigrateTask(ctx context.Context, task *SequenceMigrateTask, updates map[string]interface{}) (*SequenceMigrateTask, error) {
	err := rw.DB(ctx).Model(&SequenceMigrateTask{}).Where("task_name = ? AND task_status = ?", task.TaskName, task.TaskStatus).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWSequenceMigrateTask) QuerySequenceMigrateTask(ctx context.Context, task *SequenceMigrateTask) ([]*SequenceMigrateTask, error) {
	var dataS []*SequenceMigrateTask
	err := rw.DB(ctx).Model(&SequenceMigrateTask{}).Where("task_name = ? AND task_status = ?", task.TaskName, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSequenceMigrateTask) FindSequenceMigrateTask(ctx context.Context, task *SequenceMigrateTask) ([]*SequenceMigrateTask, error) {
	var dataS []*SequenceMigrateTask
	err := rw.DB(ctx).Model(&SequenceMigrateTask{}).Where("task_name = ? AND schema_name_s = ? AND task_status = ?", task.TaskName, task.SchemaNameS, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSequenceMigrateTask) FindSequenceMigrateTaskGroupByTaskStatus(ctx context.Context, taskName string) ([]*StructGroupStatusResult, error) {
	var dataS []*StructGroupStatusResult
	err := rw.DB(ctx).Model(&SequenceMigrateTask{}).Select("task_name,task_status,count(1) as status_counts").Where("task_name = ?", taskName).Group("task_name,task_status").Order("status_counts desc").Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] group by the task_name and task_status record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSequenceMigrateTask) BatchFindSequenceMigrateTask(ctx context.Context, task *SequenceMigrateTask) ([]*SequenceMigrateTask, error) {
	var dataS []*SequenceMigrateTask
	err := rw.DB(ctx).Model(&SequenceMigrateTask{}).Where("task_name = ?", task.TaskName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("batch find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSequenceMigrateTask) ListSequenceMigrateTask(ctx context.Context, page uint64, pageSize uint64) ([]*SequenceMigrateTask, error) {
	var dataS []*SequenceMigrateTask
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&SequenceMigrateTask{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSequenceMigrateTask) DeleteSequenceMigrateTask(ctx context.Context, taskID uint64) error {
	err := rw.DB(ctx).Where("id = ?", taskID).Delete(&SequenceMigrateTask{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] id [%v] record failed: %v", rw.TableName(ctx), taskID, err)
	}
	return nil
}

func (rw *RWSequenceMigrateTask) DeleteSequenceMigrateTaskName(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&SequenceMigrateTask{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task [%v] record failed: %v", rw.TableName(ctx), taskName, err)
	}
	return nil
}

type RWStructCompareSummary struct {
	common.GormDB
}

func NewStructCompareSummaryRW(db *gorm.DB) *RWStructCompareSummary {
	m := &RWStructCompareSummary{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWStructCompareSummary) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(StructCompareSummary{}).Name())
}

func (rw *RWStructCompareSummary) CreateStructCompareSummary(ctx context.Context, task *StructCompareSummary) (*StructCompareSummary, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWStructCompareSummary) GetStructCompareSummary(ctx context.Context, task *StructCompareSummary) (*StructCompareSummary, error) {
	var dataS *StructCompareSummary
	err := rw.DB(ctx).Model(&StructCompareSummary{}).Where("task_name = ? AND schema_name_s = ?", task.TaskName, task.SchemaNameS).First(&dataS).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return dataS, nil
		}
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructCompareSummary) UpdateStructCompareSummary(ctx context.Context, task *StructCompareSummary, updates map[string]interface{}) (*StructCompareSummary, error) {
	err := rw.DB(ctx).Model(&StructCompareSummary{}).Where("task_name = ? AND schema_name_s = ?", task.TaskName, task.SchemaNameS).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWStructCompareSummary) FindStructCompareSummary(ctx context.Context, task *StructCompareSummary) ([]*StructCompareSummary, error) {
	var dataS []*StructCompareSummary
	err := rw.DB(ctx).Model(&StructCompareSummary{}).Where("task_name = ? AND schema_name_s = ?", task.TaskName, task.SchemaNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructCompareSummary) DeleteStructCompareSummary(ctx context.Context, task *StructCompareSummary) error {
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ?", task.TaskName, task.SchemaNameS).Delete(&StructCompareSummary{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWStructCompareSummary) DeleteStructCompareSummaryName(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&StructCompareSummary{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task [%v] record failed: %v", rw.TableName(ctx), taskName, err)
	}
	return nil
}

type RWStructCompareTask struct {
	common.GormDB
}

func NewStructCompareTaskRW(db *gorm.DB) *RWStructCompareTask {
	m := &RWStructCompareTask{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWStructCompareTask) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(StructCompareTask{}).Name())
}

func (rw *RWStructCompareTask) CreateStructCompareTask(ctx context.Context, task *StructCompareTask) (*StructCompareTask, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWStructCompareTask) GetStructCompareTask(ctx context.Context, task *StructCompareTask) ([]*StructCompareTask, error) {
	var dataS []*StructCompareTask
	err := rw.DB(ctx).Model(&StructCompareTask{}).Where("task_name = ? AND schema_name_s = ? AND task_status = ?", task.TaskName, task.SchemaNameS, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructCompareTask) UpdateStructCompareTask(ctx context.Context, task *StructCompareTask, updates map[string]interface{}) (*StructCompareTask, error) {
	err := rw.DB(ctx).Model(&StructCompareTask{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWStructCompareTask) GetStructCompareTaskTable(ctx context.Context, task *StructCompareTask) ([]*StructCompareTask, error) {
	var dataS []*StructCompareTask

	err := rw.DB(ctx).Model(&StructCompareTask{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] table record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructCompareTask) BatchUpdateStructCompareTask(ctx context.Context, task *StructCompareTask, updates map[string]interface{}) (*StructCompareTask, error) {
	err := rw.DB(ctx).Model(&StructCompareTask{}).Where("task_name = ? AND task_status = ?", task.TaskName, task.TaskStatus).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWStructCompareTask) FindStructCompareTask(ctx context.Context, task *StructCompareTask) ([]*StructCompareTask, error) {
	var dataS []*StructCompareTask
	err := rw.DB(ctx).Model(&StructCompareTask{}).Where("task_name = ? AND task_status = ?", task.TaskName, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructCompareTask) FindStructCompareTaskGroupByTaskStatus(ctx context.Context, taskName string) ([]*StructGroupStatusResult, error) {
	var dataS []*StructGroupStatusResult
	err := rw.DB(ctx).Model(&StructCompareTask{}).Select("task_name,task_status,count(1) as status_counts").Where("task_name = ?", taskName).Group("task_name,task_status").Order("status_counts desc").Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] group by the task_name and task_status record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructCompareTask) BatchFindStructCompareTask(ctx context.Context, task *StructCompareTask) ([]*StructCompareTask, error) {
	var dataS []*StructCompareTask
	err := rw.DB(ctx).Model(&StructCompareTask{}).Where("task_name = ?", task.TaskName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("batch find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructCompareTask) ListStructCompareTask(ctx context.Context, page uint64, pageSize uint64) ([]*StructCompareTask, error) {
	var dataS []*StructCompareTask
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&StructCompareTask{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWStructCompareTask) DeleteStructCompareTask(ctx context.Context, taskID uint64) error {
	err := rw.DB(ctx).Where("id = ?", taskID).Delete(&StructCompareTask{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] id [%v] record failed: %v", rw.TableName(ctx), taskID, err)
	}
	return nil
}

func (rw *RWStructCompareTask) DeleteStructCompareTaskName(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&StructCompareTask{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task [%v] record failed: %v", rw.TableName(ctx), taskName, err)
	}
	return nil
}

type RWDataMigrateSummary struct {
	common.GormDB
}

func NewDataMigrateSummaryRW(db *gorm.DB) *RWDataMigrateSummary {
	m := &RWDataMigrateSummary{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWDataMigrateSummary) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(DataMigrateSummary{}).Name())
}

func (rw *RWDataMigrateSummary) CreateDataMigrateSummary(ctx context.Context, task *DataMigrateSummary) (*DataMigrateSummary, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWDataMigrateSummary) GetDataMigrateSummary(ctx context.Context, task *DataMigrateSummary) (*DataMigrateSummary, error) {
	var dataS *DataMigrateSummary
	err := rw.DB(ctx).Model(&DataMigrateSummary{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).First(&dataS).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return dataS, nil
		}
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataMigrateSummary) UpdateDataMigrateSummary(ctx context.Context, task *DataMigrateSummary, updates map[string]interface{}) (*DataMigrateSummary, error) {
	err := rw.DB(ctx).Model(&DataMigrateSummary{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWDataMigrateSummary) FindDataMigrateSummary(ctx context.Context, task *DataMigrateSummary) ([]*DataMigrateSummary, error) {
	var dataS []*DataMigrateSummary
	err := rw.DB(ctx).Model(&DataMigrateSummary{}).Where("task_name = ? AND schema_name_s = ?", task.TaskName, task.SchemaNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataMigrateSummary) QueryDataMigrateSummaryFlag(ctx context.Context, task *DataMigrateSummary) ([]*DataMigrateSummary, error) {
	var dataS []*DataMigrateSummary
	err := rw.DB(ctx).Model(&DataMigrateSummary{}).Where("task_name = ? AND schema_name_s = ? AND init_flag = ? AND migrate_flag = ?", task.TaskName, task.SchemaNameS, task.InitFlag, task.MigrateFlag).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataMigrateSummary) DeleteDataMigrateSummary(ctx context.Context, task *DataMigrateSummary) error {
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Delete(&DataMigrateSummary{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWDataMigrateSummary) DeleteDataMigrateSummaryName(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&DataMigrateSummary{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task [%v] record failed: %v", rw.TableName(ctx), taskName, err)
	}
	return nil
}

type RWDataMigrateTask struct {
	common.GormDB
}

func NewDataMigrateTaskRW(db *gorm.DB) *RWDataMigrateTask {
	m := &RWDataMigrateTask{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWDataMigrateTask) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(DataMigrateTask{}).Name())
}

func (rw *RWDataMigrateTask) CreateDataMigrateTask(ctx context.Context, task *DataMigrateTask) (*DataMigrateTask, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}, {Name: "chunk_id"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWDataMigrateTask) CreateInBatchDataMigrateTask(ctx context.Context, task []*DataMigrateTask, thread, batchSize int) error {
	values := stringutil.AnySliceSplit(task, batchSize)
	g := &errgroup.Group{}
	g.SetLimit(thread)
	for _, v := range values {
		val := v
		g.Go(func() error {
			err := rw.DB(ctx).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}, {Name: "chunk_id"}},
				UpdateAll: true,
			}).Create(val).Error
			if err != nil {
				return fmt.Errorf("create table [%s] batch record failed: %v", rw.TableName(ctx), err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (rw *RWDataMigrateTask) UpdateDataMigrateTask(ctx context.Context, task *DataMigrateTask, updates map[string]interface{}) (*DataMigrateTask, error) {
	err := rw.DB(ctx).Model(&DataMigrateTask{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ? AND chunk_id = ?", task.TaskName, task.SchemaNameS, task.TableNameS, task.ChunkID).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWDataMigrateTask) BatchUpdateDataMigrateTask(ctx context.Context, task *DataMigrateTask, updates map[string]interface{}) (*DataMigrateTask, error) {
	err := rw.DB(ctx).Model(&DataMigrateTask{}).Where("task_name = ? AND task_status = ?", task.TaskName, task.TaskStatus).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWDataMigrateTask) GetDataMigrateTask(ctx context.Context, task *DataMigrateTask) (*DataMigrateTask, error) {
	var dataS *DataMigrateTask
	err := rw.DB(ctx).Model(&DataMigrateTask{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ? ", task.TaskName, task.SchemaNameS, task.TableNameS).First(&dataS).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return dataS, nil
		}
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataMigrateTask) QueryDataMigrateTask(ctx context.Context, task *DataMigrateTask) ([]*DataMigrateTask, error) {
	var dataS []*DataMigrateTask
	err := rw.DB(ctx).Model(&DataMigrateTask{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ? ", task.TaskName, task.SchemaNameS, task.TableNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataMigrateTask) FindDataMigrateTask(ctx context.Context, task *DataMigrateTask) ([]*DataMigrateTask, error) {
	var dataS []*DataMigrateTask
	err := rw.DB(ctx).Model(&DataMigrateTask{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ? AND task_status = ?", task.TaskName, task.SchemaNameS, task.TableNameS, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataMigrateTask) FindDataMigrateTaskTableStatus(ctx context.Context, taskName, schemaName, tableName string, taskStatus []string) ([]*DataMigrateTask, error) {
	var dataS []*DataMigrateTask
	err := rw.DB(ctx).Model(&DataMigrateTask{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ? AND task_status IN (?)", taskName, schemaName, tableName, taskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataMigrateTask) FindDataMigrateTaskBySchemaTableChunkStatus(ctx context.Context, task *DataMigrateTask) ([]*DataGroupTaskStatusResult, error) {
	var dataS []*DataGroupTaskStatusResult
	err := rw.DB(ctx).Model(&DataMigrateTask{}).Select("task_name,schema_name_s,table_name_s,task_status,count(1) as status_totals").Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Group("task_name,schema_name_s,table_name_s,task_status").Order("status_totals desc").Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] group by the the task_name and schema_name_s and table_name_s ans task_status record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataMigrateTask) FindDataMigrateTaskGroupByTaskSchemaTableStatus(ctx context.Context, taskName string) ([]*DataGroupTaskStatusResult, error) {
	var dataS []*DataGroupTaskStatusResult
	err := rw.DB(ctx).Model(&DataMigrateTask{}).Select("task_name,task_status,count(1) as status_totals").Where("task_name = ?", taskName).Group("task_name,task_status").Order("status_totals desc").Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] group by the the task_name and schema_name_s and table_name_s record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataMigrateTask) FindDataMigrateTaskGroupByTaskStatus(ctx context.Context, taskName string) ([]*DataGroupStatusResult, error) {
	var dataS []*DataGroupStatusResult
	err := rw.DB(ctx).Model(&DataMigrateTask{}).Select("task_name,task_status,count(1) as status_counts").Where("task_name = ?", taskName).Group("task_name,task_status").Order("status_counts desc").Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] group by the task_name and task_status record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataMigrateTask) ListDataMigrateTask(ctx context.Context, page uint64, pageSize uint64) ([]*DataMigrateTask, error) {
	var dataS []*DataMigrateTask
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&DataMigrateTask{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataMigrateTask) DeleteDataMigrateTask(ctx context.Context, task *DataMigrateTask) error {
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Delete(&DataMigrateTask{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWDataMigrateTask) DeleteDataMigrateTaskName(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&DataMigrateTask{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task [%v] record failed: %v", rw.TableName(ctx), taskName, err)
	}
	return nil
}

type RWSqlMigrateSummary struct {
	common.GormDB
}

func NewSqlMigrateSummaryRW(db *gorm.DB) *RWSqlMigrateSummary {
	m := &RWSqlMigrateSummary{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWSqlMigrateSummary) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(SqlMigrateSummary{}).Name())
}

func (rw *RWSqlMigrateSummary) CreateSqlMigrateSummary(ctx context.Context, task *SqlMigrateSummary) (*SqlMigrateSummary, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWSqlMigrateSummary) GetSqlMigrateSummary(ctx context.Context, task *SqlMigrateSummary) (*SqlMigrateSummary, error) {
	var dataS *SqlMigrateSummary
	err := rw.DB(ctx).Model(&SqlMigrateSummary{}).Where("task_name = ?", task.TaskName).First(&dataS).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return dataS, nil
		}
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSqlMigrateSummary) UpdateSqlMigrateSummary(ctx context.Context, task *SqlMigrateSummary, updates map[string]interface{}) (*SqlMigrateSummary, error) {
	err := rw.DB(ctx).Model(&SqlMigrateSummary{}).Where("task_name = ?", task.TaskName).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWSqlMigrateSummary) DeleteSqlMigrateSummary(ctx context.Context, task *SqlMigrateSummary) error {
	err := rw.DB(ctx).Where("task_name = ?", task.TaskName).Delete(&SqlMigrateSummary{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWSqlMigrateSummary) DeleteSqlMigrateSummaryName(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&SqlMigrateSummary{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task [%v] record failed: %v", rw.TableName(ctx), taskName, err)
	}
	return nil
}

type RWSqlMigrateTask struct {
	common.GormDB
}

func NewSqlMigrateTaskRW(db *gorm.DB) *RWSqlMigrateTask {
	m := &RWSqlMigrateTask{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWSqlMigrateTask) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(SqlMigrateTask{}).Name())
}

func (rw *RWSqlMigrateTask) CreateSqlMigrateTask(ctx context.Context, task *SqlMigrateTask) (*SqlMigrateTask, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_t"}, {Name: "table_name_t"}, {Name: "sql_query_s"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWSqlMigrateTask) CreateInBatchSqlMigrateTask(ctx context.Context, task []*SqlMigrateTask, thread, batchSize int) error {
	values := stringutil.AnySliceSplit(task, batchSize)
	g := &errgroup.Group{}
	g.SetLimit(thread)
	for _, v := range values {
		val := v
		g.Go(func() error {
			err := rw.DB(ctx).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_t"}, {Name: "table_name_t"}, {Name: "sql_query_s"}},
				UpdateAll: true,
			}).Create(val).Error
			if err != nil {
				return fmt.Errorf("create table [%s] batch record failed: %v", rw.TableName(ctx), err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (rw *RWSqlMigrateTask) UpdateSqlMigrateTask(ctx context.Context, task *SqlMigrateTask, updates map[string]interface{}) (*SqlMigrateTask, error) {
	err := rw.DB(ctx).Model(&SqlMigrateTask{}).Where("task_name = ? AND schema_name_t = ? AND table_name_t = ? AND sql_query_s = ?", task.TaskName, task.SchemaNameT, task.TableNameT, task.SqlQueryS).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWSqlMigrateTask) BatchUpdateSqlMigrateTask(ctx context.Context, task *SqlMigrateTask, updates map[string]interface{}) (*SqlMigrateTask, error) {
	err := rw.DB(ctx).Model(&SqlMigrateTask{}).Where("task_name = ? AND task_status = ?", task.TaskName, task.TaskStatus).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWSqlMigrateTask) FindSqlMigrateTaskByTaskStatus(ctx context.Context, task *SqlMigrateTask) ([]*SqlMigrateTask, error) {
	var dataS []*SqlMigrateTask
	err := rw.DB(ctx).Model(&SqlMigrateTask{}).Where("task_name = ? AND task_status = ?", task.TaskName, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSqlMigrateTask) FindSqlMigrateTaskGroupByTaskStatus(ctx context.Context, taskName string) ([]*DataGroupStatusResult, error) {
	var dataS []*DataGroupStatusResult
	err := rw.DB(ctx).Model(&SqlMigrateTask{}).Select("task_name,task_status,count(1) as status_counts").Where("task_name = ?", taskName).Group("task_name,task_status").Order("status_counts desc").Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] group by the task_name and task_status record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSqlMigrateTask) ListSqlMigrateTask(ctx context.Context, page uint64, pageSize uint64) ([]*SqlMigrateTask, error) {
	var dataS []*SqlMigrateTask
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&SqlMigrateTask{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSqlMigrateTask) DeleteSqlMigrateTask(ctx context.Context, task *SqlMigrateTask) error {
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_t = ? AND table_name= ? AND sql_query_s = ?", task.TaskName, task.SchemaNameT, task.TableNameT, task.SqlQueryS).Delete(&SqlMigrateTask{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWSqlMigrateTask) DeleteSqlMigrateTaskName(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&SqlMigrateTask{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task [%v] record failed: %v", rw.TableName(ctx), taskName, err)
	}
	return nil
}

type RWDataCompareSummary struct {
	common.GormDB
}

func NewDataCompareSummaryRW(db *gorm.DB) *RWDataCompareSummary {
	m := &RWDataCompareSummary{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWDataCompareSummary) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(DataCompareSummary{}).Name())
}

func (rw *RWDataCompareSummary) CreateDataCompareSummary(ctx context.Context, task *DataCompareSummary) (*DataCompareSummary, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWDataCompareSummary) GetDataCompareSummary(ctx context.Context, task *DataCompareSummary) (*DataCompareSummary, error) {
	var dataS *DataCompareSummary
	err := rw.DB(ctx).Model(&DataCompareSummary{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).First(&dataS).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return dataS, nil
		}
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataCompareSummary) QueryDataCompareSummaryFlag(ctx context.Context, task *DataCompareSummary) ([]*DataCompareSummary, error) {
	var dataS []*DataCompareSummary
	err := rw.DB(ctx).Model(&DataCompareSummary{}).Where("task_name = ? AND schema_name_s = ? AND init_flag = ? AND compare_flag = ?", task.TaskName, task.SchemaNameS, task.InitFlag, task.CompareFlag).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataCompareSummary) UpdateDataCompareSummary(ctx context.Context, task *DataCompareSummary, updates map[string]interface{}) (*DataCompareSummary, error) {
	err := rw.DB(ctx).Model(&DataCompareSummary{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWDataCompareSummary) FindDataCompareSummary(ctx context.Context, task *DataCompareSummary) ([]*DataCompareSummary, error) {
	var dataS []*DataCompareSummary
	err := rw.DB(ctx).Model(&DataCompareSummary{}).Where("task_name = ? AND schema_name_s = ?", task.TaskName, task.SchemaNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataCompareSummary) DeleteDataCompareSummary(ctx context.Context, task *DataCompareSummary) error {
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Delete(&DataCompareSummary{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWDataCompareSummary) DeleteDataCompareSummaryName(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&DataCompareSummary{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task [%v] record failed: %v", rw.TableName(ctx), taskName, err)
	}
	return nil
}

type RWDataCompareTask struct {
	common.GormDB
}

func NewDataCompareTaskRW(db *gorm.DB) *RWDataCompareTask {
	m := &RWDataCompareTask{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWDataCompareTask) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(DataCompareTask{}).Name())
}

func (rw *RWDataCompareTask) CreateDataCompareTask(ctx context.Context, task *DataCompareTask) (*DataCompareTask, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}, {Name: "chunk_id"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWDataCompareTask) CreateInBatchDataCompareTask(ctx context.Context, task []*DataCompareTask, thread, batchSize int) error {
	values := stringutil.AnySliceSplit(task, batchSize)
	g := &errgroup.Group{}
	g.SetLimit(thread)
	for _, v := range values {
		val := v
		g.Go(func() error {
			err := rw.DB(ctx).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}, {Name: "chunk_id"}},
				UpdateAll: true,
			}).Create(val).Error
			if err != nil {
				return fmt.Errorf("create table [%s] batch record failed: %v", rw.TableName(ctx), err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (rw *RWDataCompareTask) UpdateDataCompareTask(ctx context.Context, task *DataCompareTask, updates map[string]interface{}) (*DataCompareTask, error) {
	err := rw.DB(ctx).Model(&DataCompareTask{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ? AND chunk_id = ?", task.TaskName, task.SchemaNameS, task.TableNameS, task.ChunkID).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWDataCompareTask) BatchUpdateDataCompareTask(ctx context.Context, task *DataCompareTask, updates map[string]interface{}) (*DataCompareTask, error) {
	err := rw.DB(ctx).Model(&DataCompareTask{}).Where("task_name = ? AND task_status = ?", task.TaskName, task.TaskStatus).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWDataCompareTask) FindDataCompareTask(ctx context.Context, task *DataCompareTask) ([]*DataCompareTask, error) {
	var dataS []*DataCompareTask
	err := rw.DB(ctx).Model(&DataCompareTask{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ? AND task_status = ?", task.TaskName, task.SchemaNameS, task.TableNameS, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataCompareTask) FindDataCompareTaskBySchemaTableChunkStatus(ctx context.Context, task *DataCompareTask) ([]*DataGroupTaskStatusResult, error) {
	var dataS []*DataGroupTaskStatusResult
	err := rw.DB(ctx).Model(&DataCompareTask{}).Select("task_name,schema_name_s,table_name_s,task_status,count(1) as status_totals").Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Group("task_name,schema_name_s,table_name_s,task_status").Order("status_totals desc").Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] group by the the task_name and schema_name_s and table_name_s ans task_status record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataCompareTask) FindDataCompareTaskGroupByTaskSchemaTable(ctx context.Context, taskName string) ([]*DataGroupChunkResult, error) {
	var dataS []*DataGroupChunkResult
	err := rw.DB(ctx).Model(&DataCompareTask{}).Select("task_name,schema_name_s,table_name_s,count(1) as chunk_totals").Where("task_name = ?", taskName).Group("task_name,schema_name_s,table_name_s").Order("chunk_totals desc").Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] group by the the task_name and schema_name_s and table_name_s record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataCompareTask) FindDataCompareTaskGroupByTaskSchemaTableStatus(ctx context.Context, taskName string) ([]*DataGroupTaskStatusResult, error) {
	var dataS []*DataGroupTaskStatusResult
	err := rw.DB(ctx).Model(&DataCompareTask{}).Select("task_name,task_status,count(1) as status_totals").Where("task_name = ?", taskName).Group("task_name,task_status").Order("status_totals desc").Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] group by the the task_name and schema_name_s and table_name_s record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataCompareTask) FindDataCompareTaskGroupByTaskStatus(ctx context.Context, taskName string) ([]*DataGroupStatusResult, error) {
	var dataS []*DataGroupStatusResult
	err := rw.DB(ctx).Model(&DataCompareTask{}).Select("task_name,task_status,count(1) as status_counts").Where("task_name = ?", taskName).Group("task_name,task_status").Order("status_counts desc").Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] group by the task_name and task_status record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataCompareTask) ListDataCompareTask(ctx context.Context, page uint64, pageSize uint64) ([]*DataCompareTask, error) {
	var dataS []*DataCompareTask
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&DataCompareTask{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataCompareTask) QueryDataCompareTaskByTaskStatus(ctx context.Context, task *DataCompareTask) ([]*DataCompareTask, error) {
	var dataS []*DataCompareTask
	err := rw.DB(ctx).Model(&DataCompareTask{}).Where("task_name = ? AND task_status = ?", task.TaskName, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataCompareTask) DeleteDataCompareTask(ctx context.Context, task *DataCompareTask) error {
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Delete(&DataCompareTask{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWDataCompareTask) DeleteDataCompareTaskName(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&DataCompareTask{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task [%v] record failed: %v", rw.TableName(ctx), taskName, err)
	}
	return nil
}

func (rw *RWDataCompareTask) QueryDataCompareTask(ctx context.Context, task *DataCompareTask) ([]*DataCompareTask, error) {
	var dataS []*DataCompareTask
	err := rw.DB(ctx).Model(&DataCompareTask{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

type RWDataCompareResult struct {
	common.GormDB
}

func NewDataCompareResultRW(db *gorm.DB) *RWDataCompareResult {
	m := &RWDataCompareResult{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWDataCompareResult) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(DataCompareResult{}).Name())
}

func (rw *RWDataCompareResult) CreateDataCompareResult(ctx context.Context, task *DataCompareResult) (*DataCompareResult, error) {
	err := rw.DB(ctx).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWDataCompareResult) FindDataCompareResult(ctx context.Context, task *DataCompareResult) ([]*DataCompareResult, error) {
	var dataS []*DataCompareResult
	err := rw.DB(ctx).Model(&DataCompareResult{}).Where("task_name = ?", task.TaskName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataCompareResult) DeleteDataCompareResult(ctx context.Context, task *DataCompareResult) error {
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ? AND chunk_id = ?",
		task.TaskName, task.SchemaNameS, task.TableNameS, task.ChunkID).Delete(&DataCompareResult{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWDataCompareResult) DeleteDataCompareResultTable(ctx context.Context, task *DataCompareResult) error {
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Delete(&DataCompareResult{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWDataCompareResult) DeleteDataCompareResultName(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&DataCompareResult{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task [%v] record failed: %v", rw.TableName(ctx), taskName, err)
	}
	return nil
}

type RWDataScanSummary struct {
	common.GormDB
}

func NewDataScanSummaryRW(db *gorm.DB) *RWDataScanSummary {
	m := &RWDataScanSummary{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWDataScanSummary) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(DataScanSummary{}).Name())
}

func (rw *RWDataScanSummary) CreateDataScanSummary(ctx context.Context, task *DataScanSummary) (*DataScanSummary, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWDataScanSummary) GetDataScanSummary(ctx context.Context, task *DataScanSummary) (*DataScanSummary, error) {
	var dataS *DataScanSummary
	err := rw.DB(ctx).Model(&DataScanSummary{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).First(&dataS).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return dataS, nil
		}
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataScanSummary) FindDataScanSummary(ctx context.Context, task *DataScanSummary) ([]*DataScanSummary, error) {
	var dataS []*DataScanSummary
	err := rw.DB(ctx).Model(&DataScanSummary{}).Where("task_name = ? AND schema_name_s = ?", task.TaskName, task.SchemaNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataScanSummary) QueryDataScanSummaryFlag(ctx context.Context, task *DataScanSummary) ([]*DataScanSummary, error) {
	var dataS []*DataScanSummary
	err := rw.DB(ctx).Model(&DataScanSummary{}).Where("task_name = ? AND schema_name_s = ? AND init_flag = ? AND scan_flag = ?", task.TaskName, task.SchemaNameS, task.InitFlag, task.ScanFlag).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataScanSummary) UpdateDataScanSummary(ctx context.Context, task *DataScanSummary, updates map[string]interface{}) (*DataScanSummary, error) {
	err := rw.DB(ctx).Model(&DataScanSummary{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWDataScanSummary) DeleteDataScanSummary(ctx context.Context, task *DataScanSummary) error {
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Delete(&DataScanSummary{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWDataScanSummary) DeleteDataScanSummaryName(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&DataScanSummary{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task [%v] record failed: %v", rw.TableName(ctx), taskName, err)
	}
	return nil
}

type RWDataScanTask struct {
	common.GormDB
}

func NewDataScanTaskRW(db *gorm.DB) *RWDataScanTask {
	m := &RWDataScanTask{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWDataScanTask) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(DataScanTask{}).Name())
}

func (rw *RWDataScanTask) CreateDataScanTask(ctx context.Context, task *DataScanTask) (*DataScanTask, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}, {Name: "chunk_id"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWDataScanTask) CreateInBatchDataScanTask(ctx context.Context, task []*DataScanTask, thread, batchSize int) error {
	values := stringutil.AnySliceSplit(task, batchSize)
	g := &errgroup.Group{}
	g.SetLimit(thread)
	for _, v := range values {
		val := v
		g.Go(func() error {
			err := rw.DB(ctx).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}, {Name: "chunk_id"}},
				UpdateAll: true,
			}).Create(val).Error
			if err != nil {
				return fmt.Errorf("create table [%s] batch record failed: %v", rw.TableName(ctx), err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (rw *RWDataScanTask) UpdateDataScanTask(ctx context.Context, task *DataScanTask, updates map[string]interface{}) (*DataScanTask, error) {
	err := rw.DB(ctx).Model(&DataScanTask{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ? AND chunk_id = ?", task.TaskName, task.SchemaNameS, task.TableNameS, task.ChunkID).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWDataScanTask) BatchUpdateDataScanTask(ctx context.Context, task *DataScanTask, updates map[string]interface{}) (*DataScanTask, error) {
	err := rw.DB(ctx).Model(&DataScanTask{}).Where("task_name = ? AND task_status = ?", task.TaskName, task.TaskStatus).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWDataScanTask) ListDataScanTask(ctx context.Context, page uint64, pageSize uint64) ([]*DataScanTask, error) {
	var dataS []*DataScanTask
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&DataScanTask{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataScanTask) DeleteDataScanTask(ctx context.Context, task *DataScanTask) error {
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Delete(&DataScanTask{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWDataScanTask) DeleteDataScanTaskName(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&DataScanTask{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] task [%v] record failed: %v", rw.TableName(ctx), taskName, err)
	}
	return nil
}

func (rw *RWDataScanTask) FindDataScanTask(ctx context.Context, task *DataScanTask) ([]*DataScanTask, error) {
	var dataS []*DataScanTask
	err := rw.DB(ctx).Model(&DataScanTask{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ? AND task_status = ?", task.TaskName, task.SchemaNameS, task.TableNameS, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataScanTask) QueryDataScanTask(ctx context.Context, task *DataScanTask) ([]*DataScanTask, error) {
	var dataS []*DataScanTask
	err := rw.DB(ctx).Model(&DataScanTask{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataScanTask) FindDataScanTaskGroupByTaskStatus(ctx context.Context, taskName string) ([]*DataGroupStatusResult, error) {
	var dataS []*DataGroupStatusResult
	err := rw.DB(ctx).Model(&DataScanTask{}).Select("task_name,task_status,count(1) as status_counts").Where("task_name = ?", taskName).Group("task_name,task_status").Order("status_counts desc").Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] group by the task_name and task_status record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataScanTask) FindDataScanTaskGroupByTaskSchemaTable(ctx context.Context, taskName string) ([]*DataGroupChunkResult, error) {
	var dataS []*DataGroupChunkResult
	err := rw.DB(ctx).Model(&DataScanTask{}).Select("task_name,schema_name_s,table_name_s,count(1) as chunk_totals").Where("task_name = ?", taskName).Group("task_name,schema_name_s,table_name_s").Order("chunk_totals desc").Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] group by the the task_name and schema_name_s and table_name_s record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataScanTask) FindDataScanTaskBySchemaTableChunkStatus(ctx context.Context, task *DataScanTask) ([]*DataGroupTaskStatusResult, error) {
	var dataS []*DataGroupTaskStatusResult
	err := rw.DB(ctx).Model(&DataScanTask{}).Select("task_name,schema_name_s,table_name_s,task_status,count(1) as status_totals").Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Group("task_name,schema_name_s,table_name_s,task_status").Order("status_totals desc").Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] group by the the task_name and schema_name_s and table_name_s ans task_status record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataScanTask) QueryDataScanTaskByTaskStatus(ctx context.Context, task *DataScanTask) ([]*DataScanTask, error) {
	var dataS []*DataScanTask
	err := rw.DB(ctx).Model(&DataScanTask{}).Where("task_name = ? AND task_status = ?", task.TaskName, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}
