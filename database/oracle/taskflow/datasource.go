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
package taskflow

import (
	"github.com/wentaojin/dbms/database/oracle"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type Datasource struct {
	DatabaseS        *oracle.Database `json:"-"`
	SchemaNameS      string           `json:"schemaNameS"`
	TableNameS       string           `json:"tableNameS"`
	TableTypeS       string           `json:"tableTypeS"`
	CollationS       bool             `json:"collationS"`
	DBCharsetS       string           `json:"dbCharsetS"`
	DBCharsetT       string           `json:"dbCharsetT"`
	SchemaCollationS string           `json:"schemaCollationS"`
	TableCollationS  string           `json:"tableCollationS"`
	DBNlsCompS       string           `json:"dbNlsCompS"`
}

func (d *Datasource) GetTablePrimaryKey() ([]map[string]string, error) {
	return d.DatabaseS.GetDatabaseTablePrimaryKey(d.SchemaNameS, d.TableNameS)
}

func (d *Datasource) GetTableUniqueKey() ([]map[string]string, error) {
	return d.DatabaseS.GetDatabaseTableUniqueKey(d.SchemaNameS, d.TableNameS)
}

func (d *Datasource) GetTableForeignKey() ([]map[string]string, error) {
	return d.DatabaseS.GetDatabaseTableForeignKey(d.SchemaNameS, d.TableNameS)
}

func (d *Datasource) GetTableCheckKey() ([]map[string]string, error) {
	return d.DatabaseS.GetDatabaseTableCheckKey(d.SchemaNameS, d.TableNameS)
}

func (d *Datasource) GetTableUniqueIndex() ([]map[string]string, error) {
	return d.DatabaseS.GetDatabaseTableUniqueIndex(d.SchemaNameS, d.TableNameS)
}

func (d *Datasource) GetTableNormalIndex() ([]map[string]string, error) {
	return d.DatabaseS.GetDatabaseTableNormalIndex(d.SchemaNameS, d.TableNameS)
}

func (d *Datasource) GetTableComment() ([]map[string]string, error) {
	return d.DatabaseS.GetDatabaseTableComment(d.SchemaNameS, d.TableNameS)
}

func (d *Datasource) GetTableColumns() ([]map[string]string, error) {
	return d.DatabaseS.GetDatabaseTableColumns(d.SchemaNameS, d.TableNameS, d.CollationS)
}

func (d *Datasource) GetTableColumnComment() ([]map[string]string, error) {
	return d.DatabaseS.GetDatabaseTableColumnComment(d.SchemaNameS, d.TableNameS)
}

func (d *Datasource) GetTableOriginStruct() (string, error) {
	return d.DatabaseS.GetDatabaseTableOriginStruct(d.SchemaNameS, d.TableNameS, d.TableTypeS)
}

func (d *Datasource) String() string {
	jsonStr, _ := stringutil.MarshalJSON(d)
	return jsonStr
}
