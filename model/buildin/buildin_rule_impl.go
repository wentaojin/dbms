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
package buildin

import (
	"context"
	"fmt"
	"reflect"

	"gorm.io/gorm/clause"

	"github.com/wentaojin/dbms/model/common"
	"gorm.io/gorm"
)

type RWBuildinRuleRecord struct {
	common.GormDB
}

func NewBuildinRuleRecordRW(db *gorm.DB) *RWBuildinRuleRecord {
	m := &RWBuildinRuleRecord{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWBuildinRuleRecord) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(BuildinRuleRecord{}).Name())
}

func (rw *RWBuildinRuleRecord) CreateBuildInRuleRecord(ctx context.Context, rec *BuildinRuleRecord) (*BuildinRuleRecord, error) {
	err := rw.DB(ctx).Clauses(clause.Insert{Modifier: "IGNORE"}).Create(rec).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rec, nil
}

func (rw *RWBuildinRuleRecord) GetBuildInRuleRecord(ctx context.Context, ruleName string) (*BuildinRuleRecord, error) {
	var dataS *BuildinRuleRecord
	err := rw.DB(ctx).Model(&BuildinRuleRecord{}).Where("rule_name = ?", ruleName).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("get table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

type RWBuildinDatatypeRule struct {
	common.GormDB
}

func NewBuildinDatatypeRuleRW(db *gorm.DB) *RWBuildinDatatypeRule {
	m := &RWBuildinDatatypeRule{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWBuildinDatatypeRule) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(BuildinDatatypeRule{}).Name())
}

func (rw *RWBuildinDatatypeRule) CreateBuildInDatatypeRule(ctx context.Context, rec []*BuildinDatatypeRule) ([]*BuildinDatatypeRule, error) {
	err := rw.DB(ctx).Clauses(clause.Insert{Modifier: "IGNORE"}).Create(rec).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rec, nil
}

func (rw *RWBuildinDatatypeRule) QueryBuildInDatatypeRule(ctx context.Context, dbTypeS, dbTypeT string) ([]*BuildinDatatypeRule, error) {
	var dataS []*BuildinDatatypeRule
	err := rw.DB(ctx).Where("db_type_s = ? AND db_type_t = ?", dbTypeS, dbTypeT).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

type RWBuildinDefaultValueRule struct {
	common.GormDB
}

func NewBuildinDefaultValueRuleRW(db *gorm.DB) *RWBuildinDefaultValueRule {
	m := &RWBuildinDefaultValueRule{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWBuildinDefaultValueRule) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(BuildinDefaultvalRule{}).Name())
}

func (rw *RWBuildinDefaultValueRule) CreateBuildInDefaultValueRule(ctx context.Context, rec []*BuildinDefaultvalRule) ([]*BuildinDefaultvalRule, error) {
	err := rw.DB(ctx).Clauses(clause.Insert{Modifier: "IGNORE"}).Create(rec).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rec, nil
}

func (rw *RWBuildinDefaultValueRule) QueryBuildInDefaultValueRule(ctx context.Context, dbTypeS, dbTypeT string) ([]*BuildinDefaultvalRule, error) {
	var dataS []*BuildinDefaultvalRule
	err := rw.DB(ctx).Where("db_type_s = ? AND db_type_t = ?", dbTypeS, dbTypeT).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}

type RWBuildinCompatibleRule struct {
	common.GormDB
}

func NewBuildinCompatibleRuleRW(db *gorm.DB) *RWBuildinCompatibleRule {
	m := &RWBuildinCompatibleRule{
		common.WarpDB(db),
	}
	return m
}

func (rw *RWBuildinCompatibleRule) TableName(ctx context.Context) string {
	return rw.DB(ctx).NamingStrategy.TableName(reflect.TypeOf(BuildinCompatibleRule{}).Name())
}

func (rw *RWBuildinCompatibleRule) CreateBuildInCompatibleRule(ctx context.Context, rec []*BuildinCompatibleRule) ([]*BuildinCompatibleRule, error) {
	err := rw.DB(ctx).Clauses(clause.Insert{Modifier: "IGNORE"}).Create(rec).Error
	if err != nil {
		return nil, fmt.Errorf("create table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return rec, nil
}

func (rw *RWBuildinCompatibleRule) QueryBuildInCompatibleRule(ctx context.Context, dbTypeS, dbTypeT string) ([]*BuildinCompatibleRule, error) {
	var dataS []*BuildinCompatibleRule
	err := rw.DB(ctx).Where("db_type_s = ? AND db_type_t = ?", dbTypeS, dbTypeT).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}
