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
package params

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"gorm.io/gorm/clause"

	"github.com/wentaojin/dbms/model/common"
	"gorm.io/gorm"
)

type RWTaskParams struct {
	common.GormDB
}

func NewTaskParamsRW(db *gorm.DB) *RWTaskParams {
	m := &RWTaskParams{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWTaskParams) TaskDefaultParamTableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(TaskDefaultParam{}).Name())
}

func (rw *RWTaskParams) TaskCustomParamTableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(TaskCustomParam{}).Name())
}

func (rw *RWTaskParams) CreateTaskDefaultParam(ctx context.Context, data *TaskDefaultParam) (*TaskDefaultParam, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_mode"}, {Name: "param_name"}},
		UpdateAll: true,
	}).Create(data).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TaskDefaultParamTableName(ctx), err)
	}
	return data, nil
}

func (rw *RWTaskParams) UpdateTaskDefaultParam(ctx context.Context, data *TaskDefaultParam) (*TaskDefaultParam, error) {
	err := rw.DB(ctx).Model(&TaskDefaultParam{}).Where("task_mode = ? AND param_name = ?",
		data.TaskMode, data.ParamName).Save(&data).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TaskDefaultParamTableName(ctx), err)
	}
	return data, nil
}

func (rw *RWTaskParams) ListTaskDefaultParam(ctx context.Context, page uint64, pageSize uint64) ([]*TaskDefaultParam, error) {
	var dataS []*TaskDefaultParam
	if page == 0 && pageSize == 0 {
		err := rw.DB(ctx).Model(&TaskDefaultParam{}).Find(&dataS).Error
		if err != nil {
			return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TaskDefaultParamTableName(ctx), err)
		}
		return dataS, nil
	}
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&TaskDefaultParam{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TaskDefaultParamTableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWTaskParams) QueryTaskDefaultParam(ctx context.Context, data *TaskDefaultParam) ([]*TaskDefaultParam, error) {
	var dataS []*TaskDefaultParam
	err := rw.DB(ctx).Model(&TaskDefaultParam{}).Where("task_mode = ?", data.TaskMode).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TaskDefaultParamTableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWTaskParams) DeleteTaskDefaultParam(ctx context.Context, ids []uint64) error {
	err := rw.DB(ctx).Delete(&TaskDefaultParam{}, ids).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TaskDefaultParamTableName(ctx), err)
	}
	return nil
}

func (rw *RWTaskParams) CreateTaskCustomParam(ctx context.Context, data *TaskCustomParam) (*TaskCustomParam, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "task_mode"}, {Name: "param_name"}},
		UpdateAll: true,
	}).Create(data).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TaskCustomParamTableName(ctx), err)
	}
	return data, nil
}

func (rw *RWTaskParams) UpdateTaskCustomParam(ctx context.Context, data *TaskCustomParam) (*TaskCustomParam, error) {
	err := rw.DB(ctx).Model(&TaskCustomParam{}).Where("task_name = ? AND task_mode = ? AND param_name = ?", data.TaskName, data.TaskMode, data.ParamName).Save(&data).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TaskCustomParamTableName(ctx), err)
	}
	return data, nil
}

func (rw *RWTaskParams) ListTaskCustomParam(ctx context.Context, page uint64, pageSize uint64) ([]*TaskCustomParam, error) {
	var dataS []*TaskCustomParam
	if page == 0 && pageSize == 0 {
		err := rw.DB(ctx).Model(&TaskCustomParam{}).Find(&dataS).Error
		if err != nil {
			return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TaskCustomParamTableName(ctx), err)
		}
		return dataS, nil
	}
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&TaskCustomParam{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TaskCustomParamTableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWTaskParams) QueryTaskCustomParam(ctx context.Context, data *TaskCustomParam) ([]*TaskCustomParam, error) {
	var dataS []*TaskCustomParam
	err := rw.DB(ctx).Model(&TaskCustomParam{}).Where("task_name = ? AND task_mode = ?", data.TaskName, data.TaskMode).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TaskCustomParamTableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWTaskParams) GetTaskCustomParam(ctx context.Context, data *TaskCustomParam) (*TaskCustomParam, error) {
	var dataS *TaskCustomParam
	err := rw.DB(ctx).Model(&TaskCustomParam{}).Where("task_name = ? AND task_mode = ? AND param_name = ?", data.TaskName, data.TaskMode, data.ParamName).First(&dataS).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return dataS, nil
		}
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TaskCustomParamTableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWTaskParams) DeleteTaskCustomParam(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&TaskCustomParam{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TaskCustomParamTableName(ctx), err)
	}
	return nil
}
