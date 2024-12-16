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
package consume

import (
	"context"
	"fmt"
	"reflect"

	"github.com/wentaojin/dbms/model/common"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type RWMsgTopicPartition struct {
	common.GormDB
}

func NewMsgTopicPartitionRW(db *gorm.DB) *RWMsgTopicPartition {
	m := &RWMsgTopicPartition{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWMsgTopicPartition) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(MsgTopicPartition{}).Name())
}

func (rw *RWMsgTopicPartition) CreateMsgTopicPartition(ctx context.Context, data *MsgTopicPartition) (*MsgTopicPartition, error) {
	err := rw.DB(ctx).Create(data).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return data, nil
}

func (rw *RWMsgTopicPartition) ListMsgTopicPartition(ctx context.Context, page uint64, pageSize uint64) ([]*MsgTopicPartition, error) {
	var dataS []*MsgTopicPartition
	if page == 0 && pageSize == 0 {
		err := rw.DB(ctx).Model(&MsgTopicPartition{}).Find(&dataS).Error
		if err != nil {
			return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
		}
		return dataS, nil
	}
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&MsgTopicPartition{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWMsgTopicPartition) GetMsgTopicPartition(ctx context.Context, data *MsgTopicPartition) (*MsgTopicPartition, error) {
	var dataS []*MsgTopicPartition
	err := rw.DB(ctx).Model(&MsgTopicPartition{}).Where("task_name = ? AND topic = ? and partitions = ?", data.TaskName, data.Topic, data.Partitions).Find(&dataS).Limit(1).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	if len(dataS) > 1 {
		return nil, fmt.Errorf("the table [%s] record counts [%d] are over one, should be one", rw.TableName(ctx), len(dataS))
	}
	return dataS[0], nil
}

func (rw *RWMsgTopicPartition) CountMsgTopicPartition(ctx context.Context, data *MsgTopicPartition) (int, error) {
	var dataS int
	err := rw.DB(ctx).Model(&MsgTopicPartition{}).Select("COUNT(1)").Where("task_name = ? AND topic = ? AND partitions = ?", data.TaskName, data.Topic, data.Partitions).Scan(&dataS).Error
	if err != nil {
		return 0, fmt.Errorf("count table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWMsgTopicPartition) DeleteMsgTopicPartition(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&MsgTopicPartition{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

func (rw *RWMsgTopicPartition) UpdateMsgTopicPartition(ctx context.Context, data *MsgTopicPartition, updates map[string]interface{}) error {
	err := rw.DB(ctx).Model(&MsgTopicPartition{}).Where("task_name = ? AND topic = ? and partitions = ?", data.TaskName, data.Topic, data.Partitions).Updates(updates).Error
	if err != nil {
		return fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}

type RWMsgDdlRewrite struct {
	common.GormDB
}

func NewMsgDdlRewriteRW(db *gorm.DB) *RWMsgDdlRewrite {
	m := &RWMsgDdlRewrite{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWMsgDdlRewrite) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(MsgDdlRewrite{}).Name())
}

func (rw *RWMsgDdlRewrite) CreateMsgDdlRewrite(ctx context.Context, data *MsgDdlRewrite) (*MsgDdlRewrite, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_name"}, {Name: "topic"}, {Name: "ddl_digest"}},
		UpdateAll: true,
	}).Create(data).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return data, nil
}

func (rw *RWMsgDdlRewrite) ListMsgDdlRewrite(ctx context.Context, page uint64, pageSize uint64) ([]*MsgDdlRewrite, error) {
	var dataS []*MsgDdlRewrite
	if page == 0 && pageSize == 0 {
		err := rw.DB(ctx).Model(&MsgDdlRewrite{}).Find(&dataS).Error
		if err != nil {
			return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
		}
		return dataS, nil
	}
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&MsgDdlRewrite{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWMsgDdlRewrite) QueryMsgDdlRewrite(ctx context.Context, data *MsgDdlRewrite) ([]*MsgDdlRewrite, error) {
	var dataS []*MsgDdlRewrite
	err := rw.DB(ctx).Model(&MsgDdlRewrite{}).Where("task_name = ?", data.TaskName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWMsgDdlRewrite) GetMsgDdlRewrite(ctx context.Context, data *MsgDdlRewrite) (*MsgDdlRewrite, error) {
	var dataS []*MsgDdlRewrite
	err := rw.DB(ctx).Model(&MsgDdlRewrite{}).Where("task_name = ? AND topic = ? AND ddl_digest = ?",
		data.TaskName,
		data.Topic,
		data.DdlDigest).Find(&dataS).Limit(1).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	if len(dataS) > 1 {
		return nil, fmt.Errorf("the table [%s] record counts [%d] are over one, should be one", rw.TableName(ctx), len(dataS))
	}
	return dataS[0], nil
}

func (rw *RWMsgDdlRewrite) DeleteMsgDdlRewrite(ctx context.Context, taskName []string) error {
	err := rw.DB(ctx).Where("task_name IN (?)", taskName).Delete(&MsgDdlRewrite{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}
