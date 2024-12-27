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
	"sort"
	"strings"

	"github.com/wentaojin/dbms/message"
)

func MsgConvDDLEvent(key *MessageEventKey, value *MessageDDLEventValue) *DDLChangedEvent {
	return &DDLChangedEvent{
		CommitTs:   key.CommitTs,
		SchemaName: key.SchemaName,
		TableName:  key.TableName,
		DdlQuery:   value.DdlQuery,
		DdlType:    value.DdlType,
	}
}

func MsgConvRowChangedEvent(key *MessageEventKey, value *MessageRowEventValue) (*RowChangedEvent, error) {
	e := new(RowChangedEvent)
	e.SchemaName = key.SchemaName
	e.TableName = key.TableName
	e.CommitTs = key.CommitTs

	if len(value.Delete) != 0 {
		// Parse message row value and convert row changed column value with field name
		preCols, err := codecColumns2RowChangeColumns(value.Delete)
		if err != nil {
			return e, err
		}
		sortColumnArrays(preCols)
		e.QueryType = message.DMLDeleteQueryType
		e.ValidUniqColumns, e.Columns, e.ColumnType, e.OldColumnData = GetTableSortedColumnChangedEvent(preCols)
	} else {
		if len(value.Before) != 0 && len(value.Upsert) != 0 {
			e.QueryType = message.DMLUpdateQueryType
			preCols, err := codecColumns2RowChangeColumns(value.Before)
			if err != nil {
				return e, err
			}
			sortColumnArrays(preCols)
			_, _, _, e.OldColumnData = GetTableSortedColumnChangedEvent(preCols)
		} else {
			e.QueryType = message.DMLInsertQueryType
		}
		cols, err := codecColumns2RowChangeColumns(value.Upsert)
		if err != nil {
			return e, err
		}

		sortColumnArrays(cols)

		e.ValidUniqColumns, e.Columns, e.ColumnType, e.NewColumnData = GetTableSortedColumnChangedEvent(cols)

	}
	return e, nil
}

type ConvColumn struct {
	ColumnName  string         `json:"columnName"`
	ColumnType  string         `json:"columnType"`
	ColumnFlag  ColumnFlagType `json:"columnFlag"`
	ColumnValue interface{}    `json:"columnValue"`
}

type columnsArray []*ConvColumn

func (a columnsArray) Len() int {
	return len(a)
}

func (a columnsArray) Less(i, j int) bool {
	return a[i].ColumnName < a[j].ColumnName
}

func (a columnsArray) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// sortColumnArrays sort column arrays by name
func sortColumnArrays(arrays ...[]*ConvColumn) {
	for _, array := range arrays {
		if array != nil {
			sort.Sort(columnsArray(array))
		}
	}
}

func codecColumns2RowChangeColumns(cols map[string]Column) ([]*ConvColumn, error) {
	sinkCols := make([]*ConvColumn, 0, len(cols))
	for name, col := range cols {
		c, err := col.ConvRowChangeColumn(name)
		if err != nil {
			return nil, err
		}
		sinkCols = append(sinkCols, c)
	}
	if len(sinkCols) == 0 {
		return nil, nil
	}
	sort.Slice(sinkCols, func(i, j int) bool {
		return strings.Compare(sinkCols[i].ColumnName, sinkCols[j].ColumnName) > 0
	})
	return sinkCols, nil
}

func GetTableSortedColumnChangedEvent(cols []*ConvColumn) (map[string]interface{}, []*columnAttr, map[string]string, map[string]interface{}) {
	var columns []*columnAttr
	columnTys := make(map[string]string)
	columnCd := make(map[string]interface{})
	validUniqColumns := make(map[string]interface{})

	for _, c := range cols {
		columns = append(columns, &columnAttr{
			ColumnName:        c.ColumnName,
			ColumnType:        c.ColumnType,
			IsGeneratedColumn: c.ColumnFlag.IsGeneratedColumn(),
		})
		columnTys[c.ColumnName] = c.ColumnType
		columnCd[c.ColumnName] = c.ColumnValue
		/*
			The table synchronized by TiCDC needs to have at least one valid index. The definition of a valid index is as follows:
			1,	the primary key (PRIMARY KEY) is a valid index.
			2,	each column in the unique index (UNIQUE INDEX) is explicitly defined as NOT NULL in the table structure and there are no virtual generated columns (VIRTUAL GENERATED COLUMNS).

			Data synchronization TiCDC will select a valid index as the Handle Index. The HandleKeyFlag of the columns included in the Handle Index is set to 1.
		*/
		if c.ColumnFlag.IsHandleKey() {
			validUniqColumns[c.ColumnName] = c.ColumnValue
		}
	}
	return validUniqColumns, columns, columnTys, columnCd
}
