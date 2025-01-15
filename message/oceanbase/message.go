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
package oceanbase

import (
	"encoding/json"
	"strings"
)

const (
	MsgRecordTypeInsert    = "INSERT"
	MsgRecordTypeUpdate    = "UPDATE"
	MsgRecordTypeDelete    = "DELETE"
	MsgRecordTypeHeartbeat = "HEARTBEAT"
	MsgRecordTypeDDL       = "DDL"
	MsgRecordTypeROW       = "ROW"
)

// https://www.oceanbase.com/docs/enterprise-oms-doc-cn-1000000001781775
type Message struct {
	/*
		DELETE only has prevStruct, INSERT and DDL only have postStruct, UPDATE has both prevStruct and postStruct, HEARTBEAT (regular heartbeat message) does not have postStruct and postStruct.
	*/
	// the table record kv image before change
	PrevStruct map[string]interface{} `json:"prevStruct"`
	// the table record kv image after change
	PostStruct map[string]interface{} `json:"postStruct"`

	Metadata allMetadata `json:"allMetaData"`
	// INSERT/UPDATE/DELETE/HEARTBEAT/DDL/ROW
	RecordType string `json:"recordType"`
}

type allMetadata struct {
	Checkpoint         string `json:"checkpoint"`
	RecordPrimaryKey   string `json:"record_primary_key"`
	RecordPrimaryValue string `json:"record_primary_value"`
	SourceIdentity     string `json:"source_identity"`
	DbType             string `json:"dbType"`
	StoreDataSequence  uint64 `json:"storeDataSequence"`
	TableName          string `json:"table_name"`
	DbName             string `json:"db"`
	Timestamp          string `json:"timestamp"`
	UniqueID           string `json:"uniqueId"`
	TransID            string `json:"transId"`
	ClusterID          string `json:"clusterId"`
	DdlType            string `json:"ddlType"`
}

func (m *Message) String() string {
	js, _ := json.Marshal(m)
	return string(js)
}

func (m *Message) DecodeDDLChangedEvent() *DDLChangedEvent {
	return &DDLChangedEvent{
		SchemaName: m.Metadata.DbName,
		TableName:  m.Metadata.TableName,
		DdlQuery:   m.PostStruct["ddl"].(string),
		DdlType:    m.Metadata.DdlType,
		CommitTs:   m.Metadata.StoreDataSequence,
	}
}

func (m *Message) DecodeRowChangedEvent() *RowChangedEvent {
	validUniqs := make(map[string]interface{})

	pkNames := strings.Split(m.Metadata.RecordPrimaryKey, "\u0001")
	pkValues := strings.Split(m.Metadata.RecordPrimaryValue, "\u0001")

	for i, n := range pkNames {
		validUniqs[n] = pkValues[i]
	}

	return &RowChangedEvent{
		SchemaName: m.Metadata.DbName,
		TableName:  m.Metadata.TableName,
		QueryType:  m.RecordType,
		CommitTs:   m.Metadata.StoreDataSequence,

		ValidUniqColumns: validUniqs,

		NewColumnData: m.PostStruct,
		OldColumnData: m.PrevStruct,
	}
}
