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

	"github.com/wentaojin/dbms/model/common"
	"gorm.io/gorm"
)

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

func (rw *RWBuildinDatatypeRule) QueryBuildInDatatypeRule(ctx context.Context, taskFlow string) ([]*BuildinDatatypeRule, error) {
	var dataS []*BuildinDatatypeRule
	err := rw.DB(ctx).Where("task_flow = ?", taskFlow).Find(&dataS).Error
	if err != nil {
		return nil, fmt.Errorf("query table [%s] record failed: %v", rw.TableName(ctx), err)
	}
	return dataS, nil
}
