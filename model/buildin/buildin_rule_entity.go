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
	"github.com/wentaojin/dbms/model/common"
)

type BuildinRuleRecord struct {
	ID       uint64 `gorm:"primarykey;autoIncrement;comment:id" json:"id"`
	RuleName string `gorm:"type:varchar(60);default:not null;uniqueIndex:idx_rule_name;comment:rule name" json:"ruleName"`
	RuleInit string `gorm:"type:varchar(10);comment:rule init status" json:"ruleInit"`
	*common.Entity
}

type BuildinDatatypeRule struct {
	ID            uint64 `gorm:"primarykey;autoIncrement;comment:id" json:"id"`
	DBTypeS       string `gorm:"type:varchar(60);uniqueIndex:idx_taskflow_datatype_complex;comment:source db type" json:"dbTypeS"`
	DBTypeT       string `gorm:"type:varchar(60);uniqueIndex:idx_taskflow_datatype_complex;comment:target db type" json:"dbTypeT"`
	DatatypeNameS string `gorm:"type:varchar(300);uniqueIndex:idx_taskflow_datatype_complex;comment:source datatype name" json:"datatypeNameS"`
	DatatypeNameT string `gorm:"type:varchar(300);comment:target datatype name" json:"datatypeNameT"`
	*common.Entity
}

type BuildinDefaultvalRule struct {
	ID            uint64 `gorm:"primarykey;autoIncrement;comment:id" json:"id"`
	DBTypeS       string `gorm:"type:varchar(60);uniqueIndex:idx_taskflow_datatype_complex;comment:source db type" json:"dbTypeS"`
	DBTypeT       string `gorm:"type:varchar(60);uniqueIndex:idx_taskflow_datatype_complex;comment:target db type" json:"dbTypeT"`
	DefaultValueS string `gorm:"type:varchar(300);uniqueIndex:idx_taskflow_datatype_complex;comment:source default value" json:"defaultValueS"`
	DefaultValueT string `gorm:"type:varchar(300);comment:target default value" json:"defaultValueT"`
	*common.Entity
}

type BuildinCompatibleRule struct {
	ID            uint64 `gorm:"primarykey;autoIncrement;comment:id" json:"id"`
	DBTypeS       string `gorm:"type:varchar(60);uniqueIndex:idx_taskflow_datatype_complex;comment:source db type" json:"dbTypeS"`
	DBTypeT       string `gorm:"type:varchar(60);uniqueIndex:idx_taskflow_datatype_complex;comment:target db type" json:"dbTypeT"`
	ObjectNameS   string `gorm:"type:varchar(300);uniqueIndex:idx_taskflow_datatype_complex;comment:source object name" json:"objectNameS"`
	IsCompatible  string `gorm:"type:char(1);comment:is the object compatible" json:"isCompatible"`
	IsConvertible string `gorm:"type:char(1);comment:is the object convertible" json:"isConvertible"`
	*common.Entity
}
