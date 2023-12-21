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

type BuildinDatatypeRule struct {
	ID            uint64 `gorm:"primarykey;autoIncrement;comment:id" json:"id"`
	TaskFlow      string `gorm:"type:varchar(120);uniqueIndex:idx_taskflow_datatype_complex"`
	DatatypeNameS string `gorm:"type:varchar(300);uniqueIndex:idx_taskflow_datatype_complex;comment:source datatype name" json:"datatypeNameS"`
	DatatypeNameT string `gorm:"type:varchar(300);comment:target datatype name" json:"datatypeNameT"`
	*common.Entity
}
