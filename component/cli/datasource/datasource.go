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
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/wentaojin/dbms/openapi"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type Config struct {
	Datasource []Datasource `toml:"datasource" json:"datasource"`
}

type Datasource struct {
	DatasourceName string `toml:"datasource-name" json:"datasourceName"`
	DbType         string `toml:"db-type" json:"dbType"`
	Username       string `toml:"username" json:"username"`
	Password       string `toml:"password" json:"password"`
	Host           string `toml:"host" json:"host"`
	Port           int64  `toml:"port" json:"port"`
	ConnectCharset string `toml:"connect-charset" json:"connectCharset"`
	ConnectParams  string `toml:"connect-params" json:"connectParams"`
	ConnectStatus  string `toml:"connect-status" json:"connectStatus"`
	ServiceName    string `toml:"service-name" json:"serviceName"`
	PdbName        string `toml:"pdb-name" json:"pdbName"`
	Comment        string `toml:"comment" json:"comment"`
}

func (c *Config) String() string {
	jsonStr, _ := stringutil.MarshalJSON(c)
	return jsonStr
}

func Upsert(serverAddr string, file string) error {
	var cfg = &Config{}
	if _, err := toml.DecodeFile(file, cfg); err != nil {
		return fmt.Errorf("failed decode toml config file %s: %v", file, err)
	}

	resp, err := openapi.Request(openapi.RequestPUTMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APIDatasourcePath), []byte(cfg.String()))
	if err != nil {
		return err
	}

	var jsonData map[string]interface{}
	err = stringutil.UnmarshalJSON(resp, &jsonData)
	if err != nil {
		return fmt.Errorf("error decoding JSON: %v", err)
	}

	formattedJSON, err := stringutil.MarshalIndentJSON(stringutil.FormatJSONFields(jsonData))
	if err != nil {
		return fmt.Errorf("error encoding JSON: %v", err)
	}
	fmt.Println(formattedJSON)

	return nil
}

func Delete(serverAddr string, name []string) error {
	bodyReq := make(map[string]interface{})
	bodyReq["param"] = name

	jsonStr, err := stringutil.MarshalJSON(bodyReq)
	if err != nil {
		return err
	}

	resp, err := openapi.Request(openapi.RequestDELETEMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APIDatasourcePath), []byte(jsonStr))
	if err != nil {
		return err
	}

	var jsonData map[string]interface{}
	err = stringutil.UnmarshalJSON(resp, &jsonData)
	if err != nil {
		return fmt.Errorf("error decoding JSON: %v", err)
	}

	formattedJSON, err := stringutil.MarshalIndentJSON(stringutil.FormatJSONFields(jsonData))
	if err != nil {
		return fmt.Errorf("error encoding JSON: %v", err)
	}
	fmt.Println(formattedJSON)

	return nil
}

func Get(serverAddr string, name string, page uint64, pageSize uint64) error {
	bodyReq := make(map[string]interface{})
	bodyReq["param"] = name
	bodyReq["page"] = page
	bodyReq["pageSize"] = pageSize

	jsonStr, err := stringutil.MarshalJSON(bodyReq)
	if err != nil {
		return err
	}
	resp, err := openapi.Request(openapi.RequestPOSTMethod, stringutil.StringBuilder(stringutil.WrapScheme(serverAddr, false), openapi.DBMSAPIBasePath, openapi.APIDatasourcePath), []byte(jsonStr))
	if err != nil {
		return err
	}

	var jsonData map[string]interface{}
	err = stringutil.UnmarshalJSON(resp, &jsonData)
	if err != nil {
		return fmt.Errorf("error decoding JSON: %v", err)
	}

	formattedJSON, err := stringutil.MarshalIndentJSON(stringutil.FormatJSONFields(jsonData))
	if err != nil {
		return fmt.Errorf("error encoding JSON: %v", err)
	}
	fmt.Println(formattedJSON)

	return nil
}
