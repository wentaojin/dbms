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

import "encoding/json"

type ReportOverview struct {
	ReportName        string `json:"report_name"`
	ReportUser        string `json:"report_user"`
	HostName          string `json:"host_name"`
	PlatformName      string `json:"platform_name"`
	DBName            string `json:"db_name"`
	DBVersion         string `json:"db_version"`
	GlobalDBName      string `json:"global_db_name"`
	ClusterDB         string `json:"cluster_db"`
	ClusterDBInstance string `json:"cluster_db_instance"`
	InstanceName      string `json:"instance_name"`
	InstanceNumber    string `json:"instance_number"`
	ThreadNumber      string `json:"thread_number"`
	BlockSize         string `json:"block_size"`
	TotalUsedSize     string `json:"total_used_size"`
	HostCPUS          string `json:"host_cpus"`
	HostMem           string `json:"host_mem"`
	CharacterSet      string `json:"character_set"`
}

func (ro *ReportOverview) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ReportSummary struct {
	AssessType    string `json:"assess_type"`
	AssessName    string `json:"assess_name"`
	AssessTotal   int    `json:"assess_total"`
	Compatible    int    `json:"compatible"`
	Incompatible  int    `json:"incompatible"`
	Convertible   int    `json:"convertible"`
	InConvertible int    `json:"inconvertible"`
}

func (ro *ReportSummary) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ReportCheck struct {
	ListSchemaPartitionTableCountsCheck  []SchemaPartitionTableCountsCheck  `json:"list_schema_partition_table_counts_check"`
	ListSchemaTableRowLengthCheck        []SchemaTableRowLengthCheck        `json:"list_schema_table_row_length_check"`
	ListSchemaTableIndexRowLengthCheck   []SchemaTableIndexRowLengthCheck   `json:"list_schema_table_index_row_length_check"`
	ListSchemaTableColumnCountsCheck     []SchemaTableColumnCountsCheck     `json:"list_schema_table_column_counts_check"`
	ListSchemaIndexCountsCheck           []SchemaTableIndexCountsCheck      `json:"list_schema_index_counts_check"`
	ListUsernameLengthCheck              []UsernameLengthCheck              `json:"list_username_length_check"`
	ListSchemaTableNameLengthCheck       []SchemaTableNameLengthCheck       `json:"list_schema_table_name_length_check"`
	ListSchemaTableColumnNameLengthCheck []SchemaTableColumnNameLengthCheck `json:"list_schema_table_column_name_length_check"`
	ListSchemaTableIndexNameLengthCheck  []SchemaTableIndexNameLengthCheck  `json:"list_schema_table_index_name_length_check"`
	ListSchemaViewNameLengthCheck        []SchemaViewNameLengthCheck        `json:"list_schema_view_name_length_check"`
	ListSchemaSequenceNameLengthCheck    []SchemaSequenceNameLengthCheck    `json:"list_schema_sequence_name_length_check"`
}

func (rc *ReportCheck) String() string {
	jsonStr, _ := json.Marshal(rc)
	return string(jsonStr)
}

type SchemaPartitionTableCountsCheck struct {
	Schema          string `json:"schema"`
	TableName       string `json:"table_name"`
	PartitionCounts string `json:"partition_counts"`
}

func (ro *SchemaPartitionTableCountsCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableRowLengthCheck struct {
	Schema       string `json:"schema"`
	TableName    string `json:"table_name"`
	AvgRowLength string `json:"avg_row_length"`
}

func (ro *SchemaTableRowLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableIndexRowLengthCheck struct {
	Schema       string `json:"schema"`
	TableName    string `json:"table_name"`
	IndexName    string `json:"index_name"`
	ColumnLength string `json:"column_length"`
}

func (ro *SchemaTableIndexRowLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableColumnCountsCheck struct {
	Schema       string `json:"schema"`
	TableName    string `json:"table_name"`
	ColumnCounts string `json:"column_counts"`
}

func (ro *SchemaTableColumnCountsCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableIndexCountsCheck struct {
	Schema      string `json:"schema"`
	TableName   string `json:"table_name"`
	IndexCounts string `json:"index_counts"`
}

func (ro *SchemaTableIndexCountsCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type UsernameLengthCheck struct {
	Schema        string `json:"schema"`
	AccountStatus string `json:"account_status"`
	Created       string `json:"created"`
	Length        string `json:"length"`
}

func (ro *UsernameLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableNameLengthCheck struct {
	Schema    string `json:"schema"`
	TableName string `json:"table_name"`
	Length    string `json:"length"`
}

func (ro *SchemaTableNameLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableColumnNameLengthCheck struct {
	Schema     string `json:"schema"`
	TableName  string `json:"table_name"`
	ColumnName string `json:"column_name"`
	Length     string `json:"length"`
}

func (ro *SchemaTableColumnNameLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableIndexNameLengthCheck struct {
	Schema    string `json:"schema"`
	TableName string `json:"table_name"`
	IndexName string `json:"index_name"`
	Length    string `json:"length"`
}

func (ro *SchemaTableIndexNameLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaViewNameLengthCheck struct {
	Schema   string `json:"schema"`
	ViewName string `json:"view_name"`
	ReadOnly string `json:"read_only"`
	Length   string `json:"length"`
}

func (ro *SchemaViewNameLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaSequenceNameLengthCheck struct {
	Schema       string `json:"schema"`
	SequenceName string `json:"sequence_name"`
	OrderFlag    string `json:"order_flag"`
	Length       string `json:"length"`
}

func (ro *SchemaSequenceNameLengthCheck) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type ReportCompatible struct {
	ListSchemaTableTypeCompatibles          []SchemaTableTypeCompatibles          `json:"list_schema_table_type_compatibles"`
	ListSchemaColumnTypeCompatibles         []SchemaColumnTypeCompatibles         `json:"list_schema_column_type_compatibles"`
	ListSchemaConstraintTypeCompatibles     []SchemaConstraintTypeCompatibles     `json:"list_schema_constraint_type_compatibles"`
	ListSchemaIndexTypeCompatibles          []SchemaIndexTypeCompatibles          `json:"list_schema_index_type_compatibles"`
	ListSchemaDefaultValueCompatibles       []SchemaDefaultValueCompatibles       `json:"list_schema_default_value_compatibles"`
	ListSchemaViewTypeCompatibles           []SchemaViewTypeCompatibles           `json:"list_schema_view_type_compatibles"`
	ListSchemaObjectTypeCompatibles         []SchemaObjectTypeCompatibles         `json:"list_schema_object_type_compatibles"`
	ListSchemaPartitionTypeCompatibles      []SchemaPartitionTypeCompatibles      `json:"list_schema_partition_type_compatibles"`
	ListSchemaSubPartitionTypeCompatibles   []SchemaSubPartitionTypeCompatibles   `json:"list_schema_sub_partition_type_compatibles"`
	ListSchemaTemporaryTableTypeCompatibles []SchemaTemporaryTableTypeCompatibles `json:"list_schema_temporary_table_type_compatibles"`
}

func (sc *ReportCompatible) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaTableTypeCompatibles struct {
	Schema        string `json:"schema"`
	TableType     string `json:"table_type"`
	ObjectCounts  string `json:"object_counts"`
	ObjectSize    string `json:"object_size"`
	IsCompatible  string `json:"is_compatible"`
	IsConvertible string `json:"is_convertible"`
}

func (sc *SchemaTableTypeCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaColumnTypeCompatibles struct {
	Schema        string `json:"schema"`
	ColumnType    string `json:"column_type"`
	ObjectCounts  string `json:"object_counts"`
	MaxDataLength string `json:"max_data_length"`
	ColumnTypeMap string `json:"column_type_map"`
	IsEquivalent  string `json:"is_equivalent"`
}

func (sc *SchemaColumnTypeCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaConstraintTypeCompatibles struct {
	Schema         string `json:"schema"`
	ConstraintType string `json:"constraint_type"`
	ObjectCounts   string `json:"object_counts"`
	IsCompatible   string `json:"is_compatible"`
	IsConvertible  string `json:"is_convertible"`
}

func (sc *SchemaConstraintTypeCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaIndexTypeCompatibles struct {
	Schema        string `json:"schema"`
	IndexType     string `json:"index_type"`
	ObjectCounts  string `json:"object_counts"`
	IsCompatible  string `json:"is_compatible"`
	IsConvertible string `json:"is_convertible"`
}

func (sc *SchemaIndexTypeCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaDefaultValueCompatibles struct {
	Schema             string `json:"schema"`
	ColumnDefaultValue string `json:"column_default_value"`
	ObjectCounts       string `json:"object_counts"`
	DefaultValueMap    string `json:"default_value_map"`
	IsCompatible       string `json:"is_compatible"`
	IsConvertible      string `json:"is_convertible"`
}

func (sc *SchemaDefaultValueCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaViewTypeCompatibles struct {
	Schema        string `json:"schema"`
	ViewType      string `json:"view_type"`
	ViewTypeOwner string `json:"view_type_owner"`
	ObjectCounts  string `json:"object_counts"`
	IsCompatible  string `json:"is_compatible"`
	IsConvertible string `json:"is_convertible"`
}

func (sc *SchemaViewTypeCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaObjectTypeCompatibles struct {
	Schema        string `json:"schema"`
	ObjectType    string `json:"object_type"`
	ObjectCounts  string `json:"object_counts"`
	IsCompatible  string `json:"is_compatible"`
	IsConvertible string `json:"is_convertible"`
}

func (sc *SchemaObjectTypeCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaPartitionTypeCompatibles struct {
	Schema        string `json:"schema"`
	PartitionType string `json:"partition_type"`
	ObjectCounts  string `json:"object_counts"`
	IsCompatible  string `json:"is_compatible"`
	IsConvertible string `json:"is_convertible"`
}

func (sc *SchemaPartitionTypeCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaSubPartitionTypeCompatibles struct {
	Schema           string `json:"schema"`
	SubPartitionType string `json:"sub_partition_type"`
	ObjectCounts     string `json:"object_counts"`
	IsCompatible     string `json:"is_compatible"`
	IsConvertible    string `json:"is_convertible"`
}

func (sc *SchemaSubPartitionTypeCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type SchemaTemporaryTableTypeCompatibles struct {
	Schema             string `json:"schema"`
	TemporaryTableType string `json:"temporary_table_type"`
	ObjectCounts       string `json:"object_counts"`
	IsCompatible       string `json:"is_compatible"`
	IsConvertible      string `json:"is_convertible"`
}

func (sc *SchemaTemporaryTableTypeCompatibles) String() string {
	jsonStr, _ := json.Marshal(sc)
	return string(jsonStr)
}

type ReportRelated struct {
	ListSchemaActiveSession            []SchemaActiveSession            `json:"list_schema_active_session"`
	ListSchemaTableSizeData            []SchemaTableSizeData            `json:"list_schema_table_size_data"`
	ListSchemaTableRowsTOP             []SchemaTableRowsTOP             `json:"list_schema_table_rows_top"`
	ListSchemaCodeObject               []SchemaCodeObject               `json:"list_schema_code_object"`
	ListSchemaSynonymObject            []SchemaSynonymObject            `json:"list_schema_synonym_object"`
	ListSchemaMaterializedViewObject   []SchemaMaterializedViewObject   `json:"list_schema_materialized_view_object"`
	ListSchemaTableAvgRowLengthTOP     []SchemaTableAvgRowLengthTOP     `json:"list_schema_table_avg_row_length_top"`
	ListSchemaTableNumberTypeEqualZero []SchemaTableNumberTypeEqualZero `json:"list_schema_table_number_type_equal_zero"`
}

func (rr *ReportRelated) String() string {
	jsonStr, _ := json.Marshal(rr)
	return string(jsonStr)
}

type SchemaActiveSession struct {
	Rownum         string `json:"rownum"`
	DBID           string `json:"dbid"`
	InstanceNumber string `json:"instance_number"`
	SampleID       string `json:"sample_id"`
	SampleTime     string `json:"sample_time"`
	SessionCounts  string `json:"session_counts"`
}

func (ro *SchemaActiveSession) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableSizeData struct {
	Schema        string `json:"schema"`
	TableSize     string `json:"table_size"`
	IndexSize     string `json:"index_size"`
	LobTableSize  string `json:"lob_table_size"`
	LobIndexSize  string `json:"lob_index_size"`
	AllTablesRows string `json:"all_tables_rows"`
}

func (ro *SchemaTableSizeData) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableRowsTOP struct {
	Schema    string `json:"schema"`
	TableName string `json:"table_name"`
	TableType string `json:"table_type"`
	TableSize string `json:"table_size"`
}

func (ro *SchemaTableRowsTOP) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaCodeObject struct {
	Schema     string `json:"schema"`
	ObjectName string `json:"object_name"`
	ObjectType string `json:"object_type"`
	Lines      string `json:"lines"`
}

func (ro *SchemaCodeObject) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaSynonymObject struct {
	Schema      string `json:"schema"`
	SynonymName string `json:"synonym_name"`
	TableOwner  string `json:"table_owner"`
	TableName   string `json:"table_name"`
}

func (ro *SchemaSynonymObject) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaMaterializedViewObject struct {
	Schema            string `json:"schema"`
	MviewName         string `json:"mview_name"`
	RewriteCapability string `json:"rewrite_capability"`
	RefreshMode       string `json:"refresh_mode"`
	RefreshMethod     string `json:"refresh_method"`
	FastRefreshable   string `json:"fast_refreshable"`
}

func (ro *SchemaMaterializedViewObject) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableAvgRowLengthTOP struct {
	Schema       string `json:"schema"`
	TableName    string `json:"table_name"`
	AvgRowLength string `json:"avg_row_length"`
}

func (ro *SchemaTableAvgRowLengthTOP) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}

type SchemaTableNumberTypeEqualZero struct {
	Schema        string `json:"schema"`
	TableName     string `json:"table_name"`
	ColumnName    string `json:"column_name"`
	DataPrecision string `json:"data_precision"`
	DataScale     string `json:"data_scale"`
}

func (ro *SchemaTableNumberTypeEqualZero) String() string {
	jsonStr, _ := json.Marshal(ro)
	return string(jsonStr)
}
