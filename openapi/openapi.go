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
package openapi

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

const (
	// DebugAPIBasePath api debug base path
	DebugAPIBasePath = "/debug"
	// DBMSAPIBasePath api dbms api base path
	DBMSAPIBasePath = "/api/v1/"
)

const (
	APIDatabasePath      = "database"
	APIDatasourcePath    = "datasource"
	APITaskPath          = "task"
	APIStructMigratePath = "structMigrate"
	APIDataMigratePath   = "dataMigrate"
	APISqlMigratePath    = "sqlMigrate"
)

const (
	RequestPUTMethod    = "PUT"
	RequestPOSTMethod   = "POST"
	RequestGETMethod    = "GET"
	RequestDELETEMethod = "DELETE"
)

const (
	ResponseResultStatusSuccess = "success"
	ResponseResultStatusFailed  = "failed"
)

func Request(method, url string, body []byte) ([]byte, error) {
	client := &http.Client{}
	req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request http status [%d] not ok, please check server status or logs", resp.StatusCode)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return respBody, nil
}
