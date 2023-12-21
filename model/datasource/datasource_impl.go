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
package datasource

import (
	"context"
	"fmt"
	"reflect"

	"gorm.io/gorm/clause"

	"github.com/wentaojin/dbms/model/common"

	"gorm.io/gorm"
)

type RWDatasource struct {
	common.GormDB
}

func NewDatasourceRW(db *gorm.DB) *RWDatasource {
	m := &RWDatasource{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWDatasource) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(Datasource{}).Name())
}

func (rw *RWDatasource) CreateDatasource(ctx context.Context, dataSource *Datasource) (*Datasource, error) {
	err := rw.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "datasource_name"}},
		UpdateAll: true,
	}).Create(dataSource).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataSource, nil
}

func (rw *RWDatasource) UpdateDatasource(ctx context.Context, datasource *Datasource) (*Datasource, error) {
	err := rw.DB(ctx).Model(&Datasource{}).Where("datasource_name = ?", datasource.DatasourceName).Save(&datasource).Error
	if err != nil {
		return nil, fmt.Errorf("update table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return datasource, nil
}

func (rw *RWDatasource) GetDatasource(ctx context.Context, datasourceName string) (*Datasource, error) {
	var dataS *Datasource
	err := rw.DB(ctx).Model(&Datasource{}).Where("datasource_name = ?", datasourceName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDatasource) ListDatasource(ctx context.Context, page uint64, pageSize uint64) ([]*Datasource, error) {
	var dataS []*Datasource
	err := rw.DB(ctx).Scopes(common.Paginate(int(page), int(pageSize))).Model(&Datasource{}).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("list table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

func (rw *RWDatasource) DeleteDatasource(ctx context.Context, datasourceNames []string) error {
	err := rw.DB(ctx).Delete(&Datasource{}, datasourceNames).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return nil
}
