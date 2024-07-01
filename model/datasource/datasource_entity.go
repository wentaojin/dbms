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
	"github.com/wentaojin/dbms/model/common"
)

type Datasource struct {
	ID             uint64 `gorm:"primarykey;autoIncrement;comment:id" json:"id"`
	DatasourceName string `gorm:"not null;type:varchar(30);uniqueIndex:uniq_datasource_name;comment:name of datasource" json:"datasourceName"`
	DbType         string `gorm:"not null;type:varchar(30);uniqueIndex:uniq_datasource_complex;comment:type of datasource, eg.Oracle/Mysql/Tidb" json:"dbType"`
	Username       string `gorm:"type:varchar(100);uniqueIndex:uniq_datasource_complex;comment:username" json:"username"`
	Password       string `gorm:"type:varchar(100);comment:user password" json:"password"`
	Host           string `gorm:"type:varchar(15);uniqueIndex:uniq_datasource_complex;comment:host ip" json:"host"`
	Port           uint64 `gorm:"uniqueIndex:uniq_datasource_complex;comment:host port" json:"port"`
	ConnectParams  string `gorm:"type:varchar(150);comment:connect params" json:"connectParams"`
	ConnectCharset string `gorm:"type:varchar(60);comment:datasource connect charset" json:"connectCharset"`
	ConnectStatus  string `gorm:"type:varchar(1);comment:connection status, eg.Y/N" json:"connectStatus"`

	ServiceName   string `gorm:"type:varchar(100);comment:oracle service name" json:"serviceName"`
	PdbName       string `gorm:"type:varchar(100);comment:oracle pdb name'" json:"pdbName"`
	SessionParams string `gorm:"type:varchar(100);comment:oracle session params" json:"sessionParams"`
	DbName        string `gorm:"type:varchar(100);comment:postgresql database name" json:"dbName"`
	*common.Entity
}
