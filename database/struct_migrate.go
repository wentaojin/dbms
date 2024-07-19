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
package database

// IDatabaseStructMigrate used for database table struct migrate
type IDatabaseStructMigrate interface {
	GetDatabaseSchema() ([]string, error)
	GetDatabaseTable(schemaName string) ([]string, error)
	GetDatabaseCharset() (string, error)
	GetDatabaseCollation() (string, error)
	GetDatabaseVersion() (string, error)
	GetDatabasePartitionTable(schemaName string) ([]string, error)
	GetDatabaseTemporaryTable(schemaName string) ([]string, error)
	GetDatabaseClusteredTable(schemaName string) ([]string, error)
	GetDatabaseMaterializedView(schemaName string) ([]string, error)
	GetDatabaseNormalView(schemaName string) ([]string, error)
	GetDatabaseCompositeTypeTable(schemaName string) ([]string, error)
	GetDatabaseExternalTable(schemaName string) ([]string, error)
	GetDatabaseTableType(schemaName string) (map[string]string, error)
	GetDatabaseTableColumnInfo(schemaName string, tableName string) ([]map[string]string, error)
	GetDatabaseTablePrimaryKey(schemaName string, tableName string) ([]map[string]string, error)
	GetDatabaseTableUniqueKey(schemaName string, tableName string) ([]map[string]string, error)
	GetDatabaseTableForeignKey(schemaName string, tableName string) ([]map[string]string, error)
	GetDatabaseTableCheckKey(schemaName string, tableName string) ([]map[string]string, error)
	GetDatabaseTableNormalIndex(schemaName string, tableName string) ([]map[string]string, error)
	GetDatabaseTableUniqueIndex(schemaName string, tableName string) ([]map[string]string, error)
	GetDatabaseTableComment(schemaName string, tableName string) ([]map[string]string, error)
	GetDatabaseTableColumnComment(schemaName string, tableName string) ([]map[string]string, error)
	GetDatabaseTableCharset(schemaName string, tableName string) (string, error)
	GetDatabaseTableCollation(schemaName, tableName string) (string, error)
	GetDatabaseSchemaCollation(schemaName string) (string, error)
	GetDatabaseTableOriginStruct(schemaName, tableName, tableType string) (string, error)
}

type IDatabaseSequenceMigrate interface {
	GetDatabaseSequence(schemaName string) ([]map[string]string, error)
}

// IStructMigrateAttributesReader used for database table attributes
type IStructMigrateAttributesReader interface {
	GetTablePrimaryKey() ([]map[string]string, error)
	GetTableUniqueKey() ([]map[string]string, error)
	GetTableForeignKey() ([]map[string]string, error)
	GetTableCheckKey() ([]map[string]string, error)
	GetTableUniqueIndex() ([]map[string]string, error)
	GetTableNormalIndex() ([]map[string]string, error)
	GetTableComment() ([]map[string]string, error)
	GetTableColumns() ([]map[string]string, error)
	GetTableColumnComment() ([]map[string]string, error)
	GetTableCharsetCollation() (string, string, error)
	GetTableOriginStruct() (string, error)
}

type StructMigrateAttributes struct {
	TableCharset   string              `json:"tableCharset"`
	TableCollation string              `json:"tableCollation"`
	PrimaryKey     []map[string]string `json:"primaryKey"`
	UniqueKey      []map[string]string `json:"uniqueKey"`
	ForeignKey     []map[string]string `json:"foreignKey"`
	CheckKey       []map[string]string `json:"checkKey"`
	UniqueIndex    []map[string]string `json:"uniqueIndex"`
	NormalIndex    []map[string]string `json:"normalIndex"`
	TableComment   []map[string]string `json:"tableComment"`
	TableColumns   []map[string]string `json:"tableColumns"`
	ColumnComment  []map[string]string `json:"columnComment"`
	OriginStruct   string              `json:"originStruct"`
}

func IStructMigrateAttributes(t IStructMigrateAttributesReader) (*StructMigrateAttributes, error) {
	charset, collation, err := t.GetTableCharsetCollation()
	if err != nil {
		return nil, err
	}
	primaryKey, err := t.GetTablePrimaryKey()
	if err != nil {
		return &StructMigrateAttributes{}, err
	}
	uniqueKey, err := t.GetTableUniqueKey()
	if err != nil {
		return &StructMigrateAttributes{}, err
	}
	foreignKey, err := t.GetTableForeignKey()
	if err != nil {
		return &StructMigrateAttributes{}, err
	}
	checkKey, err := t.GetTableCheckKey()
	if err != nil {
		return &StructMigrateAttributes{}, err
	}
	uniqueIndex, err := t.GetTableUniqueIndex()
	if err != nil {
		return &StructMigrateAttributes{}, err
	}
	normalIndex, err := t.GetTableNormalIndex()
	if err != nil {
		return &StructMigrateAttributes{}, err
	}
	tableComment, err := t.GetTableComment()
	if err != nil {
		return &StructMigrateAttributes{}, err
	}
	tableColumns, err := t.GetTableColumns()
	if err != nil {
		return &StructMigrateAttributes{}, err
	}
	columnComment, err := t.GetTableColumnComment()
	if err != nil {
		return &StructMigrateAttributes{}, err
	}

	return &StructMigrateAttributes{
		TableCharset:   charset,
		TableCollation: collation,
		PrimaryKey:     primaryKey,
		UniqueKey:      uniqueKey,
		ForeignKey:     foreignKey,
		CheckKey:       checkKey,
		UniqueIndex:    uniqueIndex,
		NormalIndex:    normalIndex,
		TableComment:   tableComment,
		TableColumns:   tableColumns,
		ColumnComment:  columnComment,
	}, nil
}

type IStructMigrateAttributesRuleReader interface {
	GetCreatePrefixRule() string
	GetCaseFieldRule() string
	GetSchemaNameRule() (map[string]string, error)
	GetTableNameRule() (map[string]string, error)
	GetTableColumnRule() (map[string]string, map[string]string, map[string]string, error)
	GetTableAttributesRule() (string, error)
	GetTableCommentRule() (string, error)
	GetTableColumnCollationRule() (map[string]string, error)
	GetTableColumnCommentRule() (map[string]string, error)
}

type StructMigrateAttributesRule struct {
	CreatePrefixRule       string            `json:"createPrefixRule"`
	CaseFieldRuleT         string            `json:"caseFieldRule"`
	SchemaNameRule         map[string]string `json:"schemaNameRule"`
	TableNameRule          map[string]string `json:"tableNameRule"`
	ColumnNameRule         map[string]string `json:"columnNameRule"`
	ColumnDatatypeRule     map[string]string `json:"columnDatatypeRule"`
	ColumnDefaultValueRule map[string]string `json:"columnDefaultValueRule"`
	ColumnCollationRule    map[string]string `json:"columnCollationRule"`
	ColumnCommentRule      map[string]string `json:"columnCommentRule"`
	TableAttrRule          string            `json:"tableAttrRule"`
	TableCommentRule       string            `json:"tableCommentRule"`
}

func IStructMigrateAttributesRule(t IStructMigrateAttributesRuleReader) (*StructMigrateAttributesRule, error) {
	schemaNameRule, err := t.GetSchemaNameRule()
	if err != nil {
		return &StructMigrateAttributesRule{}, err
	}
	tableNameRule, err := t.GetTableNameRule()
	if err != nil {
		return &StructMigrateAttributesRule{}, err
	}
	columnNameRule, datatypeRule, defaultValRule, err := t.GetTableColumnRule()
	if err != nil {
		return &StructMigrateAttributesRule{}, err
	}
	rule, err := t.GetTableCommentRule()
	if err != nil {
		return &StructMigrateAttributesRule{}, err
	}
	attr, err := t.GetTableAttributesRule()
	if err != nil {
		return &StructMigrateAttributesRule{}, err
	}
	collationRule, err := t.GetTableColumnCollationRule()
	if err != nil {
		return &StructMigrateAttributesRule{}, err
	}
	commentRule, err := t.GetTableColumnCommentRule()
	if err != nil {
		return &StructMigrateAttributesRule{}, err
	}
	return &StructMigrateAttributesRule{
			CreatePrefixRule:       t.GetCreatePrefixRule(),
			CaseFieldRuleT:         t.GetCaseFieldRule(),
			SchemaNameRule:         schemaNameRule,
			TableNameRule:          tableNameRule,
			ColumnNameRule:         columnNameRule,
			ColumnDatatypeRule:     datatypeRule,
			ColumnDefaultValueRule: defaultValRule,
			ColumnCollationRule:    collationRule,
			ColumnCommentRule:      commentRule,
			TableAttrRule:          attr,
			TableCommentRule:       rule,
		},
		nil
}

type IStructMigrateAttributesProcessor interface {
	GenSchemaNameS() string
	GenTableNameS() string
	GenTableTypeS() string
	GenTableOriginDDlS() string
	GenSchemaNameT() (string, error)
	GenTableNameT() (string, error)
	GenTableCreatePrefixT() string
	GenTableSuffix() (string, error)
	GenTablePrimaryKey() (string, error)
	GenTableUniqueKey() ([]string, error)
	GenTableForeignKey() ([]string, error)
	GenTableCheckKey() ([]string, error)
	GenTableUniqueIndex() ([]string, []string, error)
	GenTableNormalIndex() ([]string, []string, error)
	GenTableComment() (string, error)
	GenTableColumns() ([]string, error)
	GenTableColumnComment() ([]string, error)
}

type TableStruct struct {
	SchemaNameS          string   `json:"schemaNameS"`
	TableNameS           string   `json:"tableNameS"`
	TableTypeS           string   `json:"tableTypeS"`
	OriginDdlS           string   `json:"originDdlS"`
	SchemaNameT          string   `json:"schemaNameT"`
	TableNameT           string   `json:"tableNameT"`
	TableCreatePrefixT   string   `json:"tableCreatePrefixT"`
	TablePrimaryKey      string   `json:"tablePrimaryKey"`
	TableColumns         []string `json:"tableColumns"`
	TableUniqueKeys      []string `json:"tableUniqueKeys"`
	TableNormalIndexes   []string `json:"tableNormalIndexes"`
	TableUniqueIndexes   []string `json:"tableUniqueIndexes"`
	TableSuffix          string   `json:"tableSuffix"`
	TableComment         string   `json:"tableComment"`
	TableColumnComment   []string `json:"tableColumnComment"`
	TableCheckKeys       []string `json:"tableCheckKeys"`
	TableForeignKeys     []string `json:"tableForeignKeys"`
	TableIncompatibleDDL []string `json:"tableIncompatibleDDL"`
}

func IStructMigrateTableStructure(p IStructMigrateAttributesProcessor) (*TableStruct, error) {
	var incompatibleSqls []string
	schemaNameT, err := p.GenSchemaNameT()
	if err != nil {
		return &TableStruct{}, err
	}
	tableNameT, err := p.GenTableNameT()
	if err != nil {
		return &TableStruct{}, err
	}
	columns, err := p.GenTableColumns()
	if err != nil {
		return &TableStruct{}, err
	}
	primaryKey, err := p.GenTablePrimaryKey()
	if err != nil {
		return &TableStruct{}, err
	}
	uniqueKeys, err := p.GenTableUniqueKey()
	if err != nil {
		return &TableStruct{}, err
	}
	normalIndexes, normalIndexCompSQL, err := p.GenTableNormalIndex()
	if err != nil {
		return &TableStruct{}, err
	}
	uniqueIndexes, uniqueIndexCompSQL, err := p.GenTableUniqueIndex()
	if err != nil {
		return &TableStruct{}, err
	}
	tableSuffix, err := p.GenTableSuffix()
	if err != nil {
		return &TableStruct{}, err
	}
	tableComment, err := p.GenTableComment()
	if err != nil {
		return &TableStruct{}, err
	}
	checkKeys, err := p.GenTableCheckKey()
	if err != nil {
		return &TableStruct{}, err
	}
	foreignKeys, err := p.GenTableForeignKey()
	if err != nil {
		return &TableStruct{}, err
	}
	columnComments, err := p.GenTableColumnComment()
	if err != nil {
		return &TableStruct{}, err
	}

	if len(normalIndexCompSQL) > 0 {
		incompatibleSqls = append(incompatibleSqls, normalIndexCompSQL...)
	}
	if len(uniqueIndexCompSQL) > 0 {
		incompatibleSqls = append(incompatibleSqls, uniqueIndexCompSQL...)
	}

	return &TableStruct{
		SchemaNameS:          p.GenSchemaNameS(),
		TableNameS:           p.GenTableNameS(),
		TableTypeS:           p.GenTableTypeS(),
		OriginDdlS:           p.GenTableOriginDDlS(),
		SchemaNameT:          schemaNameT,
		TableNameT:           tableNameT,
		TableCreatePrefixT:   p.GenTableCreatePrefixT(),
		TablePrimaryKey:      primaryKey,
		TableColumns:         columns,
		TableUniqueKeys:      uniqueKeys,
		TableNormalIndexes:   normalIndexes,
		TableUniqueIndexes:   uniqueIndexes,
		TableSuffix:          tableSuffix,
		TableComment:         tableComment,
		TableColumnComment:   columnComments,
		TableCheckKeys:       checkKeys,
		TableForeignKeys:     foreignKeys,
		TableIncompatibleDDL: incompatibleSqls,
	}, nil
}

type IStructMigrateDatabaseWriter interface {
	WriteStructDatabase() error
	SyncStructDatabase() error
}

type ISequenceMigrateDatabaseWriter interface {
	WriteSequenceDatabase() error
	SyncSequenceDatabase() error
}

type IStructMigrateFileWriter interface {
	InitOutputFile() error
	SyncStructFile() error
	SyncSequenceFile() error
	Close() error
}
