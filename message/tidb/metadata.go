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
package tidb

import (
	"encoding/json"
	"sync"

	"github.com/wentaojin/dbms/utils/stringutil"
)

var metaCache *MetadataCache

func init() {
	metaCache = NewMetadataCache()
}

type MetadataCache struct {
	rwMutex  sync.RWMutex
	metadata map[string]*metadata
}

type metadata struct {
	schemaName   string
	tableName    string
	tableColumns map[string]*column
}

type column struct {
	columnName string
	columnType string
	dataLength int
	isGeneraed bool
}

func (d *metadata) setTable(schemaName, tableName string) {
	d.schemaName = schemaName
	d.tableName = tableName
}

func (d *metadata) setColumn(k string, v *column) {
	d.tableColumns[k] = v
}

func NewMetadataCache() *MetadataCache {
	return &MetadataCache{
		metadata: make(map[string]*metadata),
	}
}

func (m *MetadataCache) Build(schemaName, tableName string) string {
	return schemaName + "." + tableName
}

// Get retrieves the metadata for a given schema and table name.
func (m *MetadataCache) Get(schemaName, tableName string) (*metadata, bool) {
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	value, exists := m.metadata[m.Build(schemaName, tableName)]
	return value, exists
}

// Set sets or updates the metadata for a given schema and table name.
func (m *MetadataCache) Set(schemaName, tableName string, metadata *metadata) {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	m.metadata[m.Build(schemaName, tableName)] = metadata
}

// Delete removes the metadata for a given schema and table name.
func (m *MetadataCache) Delete(schemaName, tableName string) {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	delete(m.metadata, m.Build(schemaName, tableName))
}

func (m *MetadataCache) DeleteSchema(schemaName string) {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	for k, _ := range m.metadata {
		schema := stringutil.StringSplit(k, ".")[0]
		if schema == schemaName {
			delete(m.metadata, k)
		}
	}
}

// Size returns the number of entries in the cache.
func (m *MetadataCache) Size() int {
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	return len(m.metadata)
}

// All returns the all of entries in the cache.
func (m *MetadataCache) All() string {
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	jsBs, _ := json.Marshal(m.metadata)
	return string(jsBs)
}
