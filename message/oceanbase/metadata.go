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
	"sync"
)

var (
	upMetaCache   *MetadataCache
	downMetaCache *MetadataCache
)

func init() {
	upMetaCache = NewMetadataCache()
	downMetaCache = NewMetadataCache()
}

type MetadataCache struct {
	rwMutex  sync.RWMutex
	metadata map[string]*metadata
	timeZone string
}

type metadata struct {
	SchemaName   string
	TableName    string
	TableColumns map[string]*column
}

type column struct {
	ColumnName string
	ColumnType string
	DataLength int
	IsGeneraed bool
}

func (d *metadata) setTable(schemaName, tableName string) {
	d.SchemaName = schemaName
	d.TableName = tableName
}

func (d *metadata) setColumn(k string, v *column) {
	d.TableColumns[k] = v
}

func (d *metadata) getColumn() map[string]*column {
	return d.TableColumns
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

// Timezone returns the timezone of entries in the cache.
func (m *MetadataCache) SetTimezone(timeZone string) {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	m.timeZone = timeZone
}

func (m *MetadataCache) GetTimezone() string {
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()
	return m.timeZone
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
