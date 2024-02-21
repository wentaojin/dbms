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
	"fmt"
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
	err := rw.DB(ctx).Model(&Task{}).Where("task_name = ?", task.TaskName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWTask) ListTask(ctx context.Context, page uint64, pageSize uint64) ([]*Task, error) {
	var dataS []*Task
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

func (rw *RWLog) QueryLog(ctx context.Context, task *Log) ([]*Log, error) {
	var dataS []*Log
	err := rw.DB(ctx).Model(&Log{}).Where("task_name = ? order by created_at desc", task.TaskName).Find(&dataS).Error
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
	err := rw.DB(ctx).Model(&StructMigrateTask{}).Where("task_name = ? AND schema_name_s = ? AND task_status = ? AND is_schema_create = ?", task.TaskName, task.SchemaNameS, task.TaskStatus, task.IsSchemaCreate).Find(&dataS).Error
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
	err := rw.DB(ctx).Model(&StructMigrateTask{}).Where("task_name = ? AND task_status = ? AND is_schema_create = ?", task.TaskName, task.TaskStatus, task.IsSchemaCreate).Find(&dataS).Error
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

func (rw *RWStructMigrateTask) FindStructMigrateTaskGroupByTaskStatus(ctx context.Context, taskName string) ([]*StructMigrateGroupStatusResult, error) {
	var dataS []*StructMigrateGroupStatusResult
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
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
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
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWDataMigrateTask) CreateInBatchDataMigrateTask(ctx context.Context, task []*DataMigrateTask, batchSize int) error {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_s"}, {Name: "table_name_s"}},
		UpdateAll: true,
	}).CreateInBatches(task, batchSize).Error
	if err != nil {
		return fmt.Errorf("create table [%s] batch record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWDataMigrateTask) UpdateDataMigrateTask(ctx context.Context, task *DataMigrateTask, updates map[string]interface{}) (*DataMigrateTask, error) {
	err := rw.DB(ctx).Model(&DataMigrateTask{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ? AND chunk_detail_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS, task.ChunkDetailS).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWDataMigrateTask) FindDataMigrateTask(ctx context.Context, task *DataMigrateTask) ([]*DataMigrateTask, error) {
	var dataS []*DataMigrateTask
	err := rw.DB(ctx).Model(&DataMigrateTask{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ? AND task_status = ?", task.TaskName, task.SchemaNameS, task.TableNameS, task.TaskStatus).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataMigrateTask) FindDataMigrateTaskBySchemaTableChunkCounts(ctx context.Context, task *DataMigrateTask) (int64, error) {
	var totals int64
	err := rw.DB(ctx).Model(&DataMigrateTask{}).Where("task_name = ? AND schema_name_s = ? AND table_name_s = ?", task.TaskName, task.SchemaNameS, task.TableNameS).Count(&totals).Error
	if err != nil {
		return totals, fmt.Errorf("find the task schema table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return totals, nil
}

func (rw *RWDataMigrateTask) FindDataMigrateTaskGroupByTaskSchemaTable(ctx context.Context) ([]*DataMigrateGroupChunkResult, error) {
	var dataS []*DataMigrateGroupChunkResult
	err := rw.DB(ctx).Model(&DataMigrateTask{}).Select("task_name,schema_name_s,table_name_s,count(1) as chunk_totals").Group("task_name,schema_name_s,table_name_s").Order("chunk_totals desc").Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("find table [%s] group by the the task_name and schema_name_s and table_name_s record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDataMigrateTask) FindDataMigrateTaskGroupByTaskStatus(ctx context.Context, taskName string) ([]*DataMigrateGroupStatusResult, error) {
	var dataS []*DataMigrateGroupStatusResult
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
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "schema_name_t"}, {Name: "table_name_t"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWSqlMigrateSummary) GetSqlMigrateSummary(ctx context.Context, task *SqlMigrateSummary) (*SqlMigrateSummary, error) {
	var dataS *SqlMigrateSummary
	err := rw.DB(ctx).Model(&SqlMigrateSummary{}).Where("task_name = ? AND schema_name_t = ? AND table_name_t = ?", task.TaskName, task.SchemaNameT, task.TableNameT).First(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWSqlMigrateSummary) UpdateSqlMigrateSummary(ctx context.Context, task *SqlMigrateSummary, updates map[string]interface{}) (*SqlMigrateSummary, error) {
	err := rw.DB(ctx).Model(&SqlMigrateSummary{}).Where("task_name = ? AND schema_name_t = ? AND table_name_t = ?", task.TaskName, task.SchemaNameT, task.TableNameT).Updates(updates).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWSqlMigrateSummary) FindSqlMigrateSummary(ctx context.Context, task *SqlMigrateSummary) ([]*SqlMigrateSummary, error) {
	var dataS []*SqlMigrateSummary
	err := rw.DB(ctx).Model(&SqlMigrateSummary{}).Where("task_name = ?", task.TaskName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSqlMigrateSummary) DeleteSqlMigrateSummary(ctx context.Context, task *SqlMigrateSummary) error {
	err := rw.DB(ctx).Where("task_name = ? AND schema_name_t = ? AND table_name_t = ?", task.TaskName, task.SchemaNameT, task.TableNameT).Delete(&SqlMigrateSummary{}).Error
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
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "sql_query_s"}},
		UpdateAll: true,
	}).Create(task).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return task, nil
}

func (rw *RWSqlMigrateTask) CreateInBatchSqlMigrateTask(ctx context.Context, task []*SqlMigrateTask, batchSize int) error {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "sql_query_s"}},
		UpdateAll: true,
	}).CreateInBatches(task, batchSize).Error
	if err != nil {
		return fmt.Errorf("create table [%s] batch record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWSqlMigrateTask) UpdateSqlMigrateTask(ctx context.Context, task *SqlMigrateTask, updates map[string]interface{}) (*SqlMigrateTask, error) {
	err := rw.DB(ctx).Model(&SqlMigrateTask{}).Where("task_name = ? AND sql_query_s = ?", task.TaskName, task.SqlQueryS).Updates(updates).Error
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

func (rw *RWSqlMigrateTask) ListSqlMigrateTask(ctx context.Context, page uint64, pageSize uint64) ([]*SqlMigrateTask, error) {
	var dataS []*SqlMigrateTask
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&SqlMigrateTask{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWSqlMigrateTask) DeleteSqlMigrateTask(ctx context.Context, task *SqlMigrateTask) error {
	err := rw.DB(ctx).Where("task_name = ? AND sql_query_s = ?", task.TaskName, task.SqlQueryS).Delete(&SqlMigrateTask{}).Error
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
