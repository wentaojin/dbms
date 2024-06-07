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
package processor

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/utils/structure"
	"go.uber.org/zap"
)

// Table structure comparison
// Based on the upstream Oracle table structure information, compare the downstream MySQL table structure
// 1. If the upstream exists and the downstream does not exist, the record will be output. If the upstream does not exist and the downstream exists, the record will not be output by default.
// 2. Ignore the comparison of different index names and constraint names between the upstream and downstream, and only compare whether the same fields exist under the same constraints downstream.
// 3. Partitions only compare partition types, partition keys, partition expressions, etc., and do not compare the specific conditions of each partition.
type Table struct {
	TaskName string
	TaskFlow string
	Source   *structure.Table
	Target   *structure.Table
}

func (t *Table) ComparePartitionTableType() string {
	logger.Info("compare partition table type",
		zap.String("task_name", t.TaskName),
		zap.String("task_flow", t.TaskFlow),
		zap.String("schema_name_s", t.Source.SchemaName),
		zap.String("table_name_s", t.Source.TableName),
		zap.String("table_partition_s", t.Source.String(constant.StructComparePartitionStructureJSONFormat)),
		zap.String("schema_name_t", t.Target.SchemaName),
		zap.String("table_name_t", t.Target.TableName),
		zap.String("table_partition_t", t.Target.String(constant.StructComparePartitionStructureJSONFormat)))
	var (
		b     strings.Builder
		partS string
		partT string
	)

	if len(t.Source.Partitions) > 0 && len(t.Target.Partitions) == 0 {
		partS = "YES"
		partT = "NO"
	}

	if len(t.Source.Partitions) == 0 && len(t.Target.Partitions) > 0 {
		partS = "YES"
		partT = "NO"
	}

	if !strings.EqualFold(partS, partT) {
		b.WriteString("/*\n")
		b.WriteString(fmt.Sprintf("the task [%s] task_flow [%s] database table type is different\n", t.TaskName, t.TaskFlow))
		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"CATEGORY", "SOURCE", "TARGET", "PARTITION_S", "PARTITION_T", "SUGGEST"})
		tw.AppendRow(table.Row{"PARTITION", fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName), fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName), partS, partT, "Manual Create Partition Table"})
		b.WriteString(fmt.Sprintf("%s\n", tw.Render()))
		b.WriteString("*/\n")
		logger.Warn("compare partition table type isn't equal",
			zap.String("task_name", t.TaskName),
			zap.String("task_flow", t.TaskFlow),
			zap.String("schema_name_s", t.Source.SchemaName),
			zap.String("table_name_s", t.Source.TableName),
			zap.String("table_partition_s", t.Source.String(constant.StructComparePartitionStructureJSONFormat)),
			zap.String("schema_name_t", t.Target.SchemaName),
			zap.String("table_name_t", t.Target.TableName),
			zap.String("table_partition_t", t.Target.String(constant.StructComparePartitionStructureJSONFormat)))
	}
	return b.String()
}

func (t *Table) CompareTableComment() string {
	logger.Info("compare table comment",
		zap.String("task_name", t.TaskName),
		zap.String("task_flow", t.TaskFlow),
		zap.String("schema_name_s", t.Source.SchemaName),
		zap.String("table_name_s", t.Source.TableName),
		zap.String("table_comment_s", t.Source.TableComment),
		zap.String("schema_name_t", t.Target.SchemaName),
		zap.String("table_name_t", t.Target.TableName),
		zap.String("table_comment_t", t.Target.TableComment))

	var b strings.Builder
	if !strings.EqualFold(t.Source.TableComment, t.Target.TableComment) {
		b.WriteString("/*\n")
		b.WriteString(fmt.Sprintf("the task [%s] task_flow [%s] database table comment is different\n", t.TaskName, t.TaskFlow))

		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"CATEGORY", "SOURCE", "TARGET", "SUGGEST"})
		tw.AppendRow(table.Row{"COMMENT", t.Source.TableComment, t.Target.TableComment, "Manual Create Table Comment"})

		b.WriteString(fmt.Sprintf("%v\n", tw.Render()))
		b.WriteString("*/\n")

		switch {
		case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL):
			b.WriteString(fmt.Sprintf("ALTER TABLE %s.%s COMMENT '%s';\n", t.Target.SchemaName, t.Target.TableName, t.Source.TableComment))
		}

		logger.Warn("compare table comment isn't equal",
			zap.String("task_name", t.TaskName),
			zap.String("task_flow", t.TaskFlow),
			zap.String("schema_name_s", t.Source.SchemaName),
			zap.String("table_name_s", t.Source.TableName),
			zap.String("table_comment_s", t.Source.TableComment),
			zap.String("schema_name_t", t.Target.SchemaName),
			zap.String("table_name_t", t.Target.TableName),
			zap.String("table_comment_t", t.Target.TableComment))
	}
	return b.String()
}

func (t *Table) CompareTableCharsetCollation() string {
	logger.Info("compare table charset collation",
		zap.String("task_name", t.TaskName),
		zap.String("task_flow", t.TaskFlow),
		zap.String("schema_name_s", t.Source.SchemaName),
		zap.String("table_name_s", t.Source.TableName),
		zap.String("table_charset_s", t.Source.TableCharset),
		zap.String("table_collation_s", t.Source.TableCollation),
		zap.String("schema_name_t", t.Target.SchemaName),
		zap.String("table_name_t", t.Target.TableName),
		zap.String("table_charset_t", t.Target.TableCharset),
		zap.String("table_collation_t", t.Target.TableCollation))
	dbCharsetT := constant.MigrateTableStructureDatabaseCharsetMap[t.TaskFlow][t.Source.TableCharset]
	dbCollationT := constant.MigrateTableStructureDatabaseCollationMap[t.TaskFlow][t.Source.TableCollation][dbCharsetT]

	var b strings.Builder

	if !strings.EqualFold(t.Target.TableCharset, dbCharsetT) || !strings.EqualFold(t.Target.TableCollation, dbCollationT) {
		b.WriteString("/*\n")
		b.WriteString(fmt.Sprintf("the task [%s] task_flow [%s] database table charset or collation is different\n", t.TaskName, t.TaskFlow))

		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"CATEGORY", "SOURCE", "TARGET", "SUGGEST"})
		tw.AppendRow(table.Row{"TABLE CHARSET COLLATION",
			fmt.Sprintf("CHARSET [%s] COLLATION [%s]", t.Source.TableCharset, t.Source.TableCollation),
			fmt.Sprintf("CHARSET [%s] COLLATION [%s]", t.Target.TableCharset, t.Target.TableCollation),
			"Manual Create Table Charset Collation"})

		b.WriteString(fmt.Sprintf("%v\n", tw.Render()))
		b.WriteString("*/\n")

		switch {
		case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL):
			b.WriteString(fmt.Sprintf("ALTER TABLE %s.%s CHARACTER SET %s COLLATE %s;\n\n", t.Target.SchemaName, t.Target.TableName, dbCharsetT, dbCollationT))
		}

		logger.Warn("compare table charset collation isn't equal",
			zap.String("task_name", t.TaskName),
			zap.String("task_flow", t.TaskFlow),
			zap.String("schema_name_s", t.Source.SchemaName),
			zap.String("table_name_s", t.Source.TableName),
			zap.String("table_charset_s", t.Source.TableCharset),
			zap.String("table_collation_s", t.Source.TableCollation),
			zap.String("schema_name_t", t.Target.SchemaName),
			zap.String("table_name_t", t.Target.TableName),
			zap.String("table_charset_t", t.Target.TableCharset),
			zap.String("table_collation_t", t.Target.TableCollation))
	}

	return b.String()
}

func (t *Table) CompareTableColumnCharsetCollation() string {
	logger.Info("compare table column charset collation",
		zap.String("task_name", t.TaskName),
		zap.String("task_flow", t.TaskFlow),
		zap.String("schema_name_s", t.Source.SchemaName),
		zap.String("table_name_s", t.Source.TableName),
		zap.String("table_column_s", t.Source.String(constant.StructCompareColumnsStructureJSONFormat)),
		zap.String("schema_name_t", t.Target.SchemaName),
		zap.String("table_name_t", t.Target.TableName),
		zap.String("table_column_t", t.Target.String(constant.StructCompareColumnsStructureJSONFormat)))

	var b strings.Builder

	modifyColumnMap := make(map[string]structure.NewColumn)

	for colName, colInfo := range t.Target.NewColumns {
		if colInfoS, ok := t.Source.NewColumns[stringutil.StringUpper(colName)]; ok {
			if !strings.EqualFold(colInfo.Charset, "UNKNOWN") || !strings.EqualFold(colInfo.Collation, "UNKNOWN") {
				if !strings.EqualFold(colInfo.Charset, colInfoS.Charset) || !strings.EqualFold(colInfo.Collation, colInfoS.Collation) {
					modifyColumnMap[colName] = colInfo
				}
			}
		}
	}

	if len(modifyColumnMap) > 0 {
		b.WriteString("/*\n")
		b.WriteString(fmt.Sprintf("the task [%s] task_flow [%s] database table column charset or collation is different\n", t.TaskName, t.TaskFlow))

		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"CATEGORY", "SOURCE", "TARGET", "COLUMN_ORIGIN_T", "SUGGEST"})

		var sqlStrs []string
		for colName, colInfo := range modifyColumnMap {
			tw.AppendRow(table.Row{"COLUMN CHARSET COLLATION",
				fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName),
				fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName),
				fmt.Sprintf("%s %s", colName, colInfo.Datatype),
				"Manual Modify Table Column Charset Collation"})

			switch {
			case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB):
				sqlStrs = append(sqlStrs, fmt.Sprintf("ALTER TABLE %s.%s MODIFY %s %s CHARACTER SET %s COLLATE %s;", t.Target.SchemaName, t.Target.TableName, colName, colInfo.Datatype, strings.ToLower(colInfo.Charset), strings.ToLower(colInfo.Collation)))
			}
		}

		b.WriteString(fmt.Sprintf("%v\n", tw.Render()))
		b.WriteString("*/\n")
		b.WriteString(strings.Join(sqlStrs, "\n") + "\n\n")

		logger.Warn("compare table column charset collation isn't equal",
			zap.String("task_name", t.TaskName),
			zap.String("task_flow", t.TaskFlow),
			zap.String("schema_name_s", t.Source.SchemaName),
			zap.String("table_name_s", t.Source.TableName),
			zap.String("table_column_s", t.Source.String(constant.StructCompareColumnsStructureJSONFormat)),
			zap.String("schema_name_t", t.Target.SchemaName),
			zap.String("table_name_t", t.Target.TableName),
			zap.String("table_column_t", t.Target.String(constant.StructCompareColumnsStructureJSONFormat)))
	}
	return b.String()
}

func (t *Table) CompareTableColumnCounts() string {
	logger.Info("compare table column counts",
		zap.String("task_name", t.TaskName),
		zap.String("task_flow", t.TaskFlow),
		zap.String("schema_name_s", t.Source.SchemaName),
		zap.String("table_name_s", t.Source.TableName),
		zap.String("table_column_s", t.Source.String(constant.StructCompareColumnsStructureJSONFormat)),
		zap.String("schema_name_t", t.Target.SchemaName),
		zap.String("table_name_t", t.Target.TableName),
		zap.String("table_column_t", t.Target.String(constant.StructCompareColumnsStructureJSONFormat)))

	var b strings.Builder

	addColumnMap := make(map[string]structure.NewColumn)
	delColumnMap := make(map[string]structure.NewColumn)

	for colName, colInfo := range t.Target.NewColumns {
		if _, ok := t.Source.NewColumns[colName]; !ok {
			delColumnMap[colName] = colInfo
		}
	}

	if len(delColumnMap) > 0 {
		b.WriteString("/*\n")
		b.WriteString(fmt.Sprintf("the task [%s] task_flow [%s] database table column counts source lack and target drop column\n", t.TaskName, t.TaskFlow))

		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"CATEGORY", "SOURCE", "TARGET", "COLUMN_CURRENT_T", "SUGGEST"})

		var sqlStrs []string
		for colName, colInfo := range delColumnMap {
			tw.AppendRow(table.Row{"COLUMN COUNTS",
				fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName),
				fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName),
				fmt.Sprintf("%s %s", colName, colInfo.Datatype),
				"Manual Drop Table Column"})

			switch {
			case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB):
				sqlStrs = append(sqlStrs, fmt.Sprintf("ALTER TABLE %s.%s DROP COLUMN %s;", t.Target.SchemaName, t.Target.TableName, colName))
			}
		}

		b.WriteString(fmt.Sprintf("%v\n", tw.Render()))
		b.WriteString("*/\n")
		b.WriteString(strings.Join(sqlStrs, "\n") + "\n\n")

		logger.Warn("compare table column counts isn't equal",
			zap.String("task_name", t.TaskName),
			zap.String("task_flow", t.TaskFlow),
			zap.String("schema_name_s", t.Source.SchemaName),
			zap.String("table_name_s", t.Source.TableName),
			zap.String("table_column_s", t.Source.String(constant.StructCompareColumnsStructureJSONFormat)),
			zap.String("schema_name_t", t.Target.SchemaName),
			zap.String("table_name_t", t.Target.TableName),
			zap.String("table_column_t", t.Target.String(constant.StructCompareColumnsStructureJSONFormat)),
			zap.String("action", "drop"))
	}

	for colName, colInfo := range t.Source.NewColumns {
		if _, ok := t.Target.NewColumns[colName]; !ok {
			addColumnMap[colName] = colInfo
		}
	}

	if len(addColumnMap) > 0 {
		b.WriteString("/*\n")
		b.WriteString(fmt.Sprintf("the task [%s] task_flow [%s] database table column counts target lack and target add column\n", t.TaskName, t.TaskFlow))

		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"CATEGORY", "SOURCE", "TARGET", "COLUMN_EXPECT_T", "SUGGEST"})

		var sqlStrs []string
		for colName, colInfo := range addColumnMap {
			tw.AppendRow(table.Row{"COLUMN COUNTS",
				fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName),
				fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName),
				fmt.Sprintf("%s %s", colName, colInfo.Datatype),
				"Manual Add Table Column"})

			switch {
			case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB):
				if !strings.EqualFold(colInfo.Charset, "UNKNOWN") || !strings.EqualFold(colInfo.Collation, "UNKNOWN") {
					sqlStrs = append(sqlStrs, fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s %s CHARACTER SET %s COLLATE %s;", t.Target.SchemaName, t.Target.TableName, colName, colInfo.Datatype, colInfo.Charset, colInfo.Collation))
				} else {
					sqlStrs = append(sqlStrs, fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s %s;", t.Target.SchemaName, t.Target.TableName, colName, colInfo.Datatype))
				}
			}
		}

		b.WriteString(fmt.Sprintf("%v\n", tw.Render()))
		b.WriteString("*/\n")
		b.WriteString(strings.Join(sqlStrs, "\n") + "\n\n")

		logger.Warn("compare table column counts isn't equal",
			zap.String("task_name", t.TaskName),
			zap.String("task_flow", t.TaskFlow),
			zap.String("schema_name_s", t.Source.SchemaName),
			zap.String("table_name_s", t.Source.TableName),
			zap.String("table_column_s", t.Source.String(constant.StructCompareColumnsStructureJSONFormat)),
			zap.String("schema_name_t", t.Target.SchemaName),
			zap.String("table_name_t", t.Target.TableName),
			zap.String("table_column_t", t.Target.String(constant.StructCompareColumnsStructureJSONFormat)),
			zap.String("action", "modify"))
	}

	return b.String()
}

func (t *Table) CompareTablePrimaryConstraint() (string, error) {
	logger.Info("compare table primary constraint",
		zap.String("task_name", t.TaskName),
		zap.String("task_flow", t.TaskFlow),
		zap.String("schema_name_s", t.Source.SchemaName),
		zap.String("table_name_s", t.Source.TableName),
		zap.String("primary_key_s", t.Source.String(constant.StructComparePrimaryStructureJSONFormat)),
		zap.String("schema_name_t", t.Target.SchemaName),
		zap.String("table_name_t", t.Target.TableName),
		zap.String("primary_key_t", t.Target.String(constant.StructComparePrimaryStructureJSONFormat)))

	var (
		b       strings.Builder
		sqlStrs []string
	)

	addTCons, delTCons, _ := stringutil.CompareMapInter(t.Source.PrimaryConstraints, t.Target.PrimaryConstraints)
	if len(addTCons) != 0 || len(delTCons) != 0 {
		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"CATEGORY", "SOURCE", "TARGET", "CONSTRAINT", "SUGGEST"})

		b.WriteString("/*\n")
		b.WriteString(fmt.Sprintf("the task [%s] task_flow [%s] database table primary constraint isn't different\n", t.TaskName, t.TaskFlow))

		for consName, consCol := range delTCons {
			if val, ok := consCol.(structure.ConstraintPrimary); ok {
				tw.AppendRow(table.Row{"PRIMARY CONSTRAINT",
					fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName),
					fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName),
					fmt.Sprintf("%s[%s]", consName, val.ConstraintColumn),
					"Manual Drop Primary Constraint",
				})

				switch {
				case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB):
					sqlStrs = append(sqlStrs, fmt.Sprintf("ALTER TABLE %s.%s DROP PRIMARY KEY;\n", t.Target.SchemaName, t.Target.TableName))
				}
				continue
			}
			return "", fmt.Errorf("the oracle schema [%s] table [%s] primary constraint [%v] assert ConstraintPrimary failed, type: [%v]", t.Source.SchemaName, t.Source.TableName, consCol, reflect.TypeOf(consCol))
		}

		for consName, consCol := range addTCons {
			if val, ok := consCol.(structure.ConstraintPrimary); ok {
				tw.AppendRow(table.Row{"PRIMARY CONSTRAINT",
					fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName),
					fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName),
					fmt.Sprintf("%s[%s]", consName, val.ConstraintColumn),
					"Manual Add Primary Constraint",
				})

				switch {
				case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB):
					sqlStrs = append(sqlStrs, fmt.Sprintf("ALTER TABLE %s.%s ADD PRIMARY KEY (%s);\n", t.Target.SchemaName, t.Target.TableName, val.ConstraintColumn))
				}
				continue
			}
			return "", fmt.Errorf("the oracle schema [%s] table [%s] primary constraint [%v] assert ConstraintPrimary failed, type: [%v]", t.Source.SchemaName, t.Source.TableName, consCol, reflect.TypeOf(consCol))
		}

		b.WriteString(fmt.Sprintf("%v\n", tw.Render()))
		b.WriteString("*/\n")
		b.WriteString(strings.Join(sqlStrs, "\n") + "\n\n")

		logger.Info("compare table primary constraint isn't equal",
			zap.String("task_name", t.TaskName),
			zap.String("task_flow", t.TaskFlow),
			zap.String("schema_name_s", t.Source.SchemaName),
			zap.String("table_name_s", t.Source.TableName),
			zap.String("primary_key_s", t.Source.String(constant.StructComparePrimaryStructureJSONFormat)),
			zap.String("schema_name_t", t.Target.SchemaName),
			zap.String("table_name_t", t.Target.TableName),
			zap.String("primary_key_t", t.Target.String(constant.StructComparePrimaryStructureJSONFormat)),
			zap.String("action", "drop or add"))
	}
	return b.String(), nil
}

func (t *Table) CompareTableUniqueConstraint() (string, error) {
	logger.Info("compare table unique constraint",
		zap.String("task_name", t.TaskName),
		zap.String("task_flow", t.TaskFlow),
		zap.String("schema_name_s", t.Source.SchemaName),
		zap.String("table_name_s", t.Source.TableName),
		zap.String("unique_key_s", t.Source.String(constant.StructCompareUniqueStructureJSONFormat)),
		zap.String("schema_name_t", t.Target.SchemaName),
		zap.String("table_name_t", t.Target.TableName),
		zap.String("unique_key_t", t.Target.String(constant.StructCompareUniqueStructureJSONFormat)))

	var (
		b       strings.Builder
		sqlStrs []string
	)

	addTCons, delTCons, _ := stringutil.CompareMapInter(t.Source.UniqueConstraints, t.Target.UniqueConstraints)
	if len(addTCons) != 0 || len(delTCons) != 0 {
		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"CATEGORY", "SOURCE", "TARGET", "CONSTRAINT", "SUGGEST"})

		b.WriteString("/*\n")
		b.WriteString(fmt.Sprintf("the task [%s] task_flow [%s] database table unique constraint isn't different\n", t.TaskName, t.TaskFlow))

		for consName, consCol := range delTCons {
			if val, ok := consCol.(structure.ConstraintUnique); ok {
				tw.AppendRow(table.Row{"UNIQUE CONSTRAINT",
					fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName),
					fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName),
					fmt.Sprintf("%s[%s]", consName, val.ConstraintColumn),
					"Manual Drop Unique Constraint",
				})

				switch {
				case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB):
					sqlStrs = append(sqlStrs, fmt.Sprintf("ALTER TABLE %s.%s DROP KEY %s;\n", t.Target.SchemaName, t.Target.TableName, consName))
				}
				continue
			}
			return "", fmt.Errorf("the oracle schema [%s] table [%s] unique constraint [%v] assert ConstraintUnique failed, type: [%v]", t.Source.SchemaName, t.Source.TableName, consCol, reflect.TypeOf(consCol))
		}

		for consName, consCol := range addTCons {
			if val, ok := consCol.(structure.ConstraintUnique); ok {
				tw.AppendRow(table.Row{"UNIQUE CONSTRAINT",
					fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName),
					fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName),
					fmt.Sprintf("%s[%s]", consName, val.ConstraintColumn),
					"Manual Add Unique Constraint",
				})

				switch {
				case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB):
					sqlStrs = append(sqlStrs, fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s UNIQUE (%s);\n", t.Target.SchemaName, t.Target.TableName, consName, val.ConstraintColumn))
				}
				continue
			}
			return "", fmt.Errorf("the oracle schema [%s] table [%s] unique constraint [%v] assert ConstraintUnique failed, type: [%v]", t.Source.SchemaName, t.Source.TableName, consCol, reflect.TypeOf(consCol))
		}

		b.WriteString(fmt.Sprintf("%v\n", tw.Render()))
		b.WriteString("*/\n")
		b.WriteString(strings.Join(sqlStrs, "\n") + "\n\n")

		logger.Info("compare table unique constraint isn't equal",
			zap.String("task_name", t.TaskName),
			zap.String("task_flow", t.TaskFlow),
			zap.String("schema_name_s", t.Source.SchemaName),
			zap.String("table_name_s", t.Source.TableName),
			zap.String("unique_key_s", t.Source.String(constant.StructCompareUniqueStructureJSONFormat)),
			zap.String("schema_name_t", t.Target.SchemaName),
			zap.String("table_name_t", t.Target.TableName),
			zap.String("unique_key_t", t.Target.String(constant.StructCompareUniqueStructureJSONFormat)),
			zap.String("action", "drop or add"))
	}
	return b.String(), nil
}

func (t *Table) CompareTableForeignConstraint() (string, error) {
	logger.Info("compare table foreign constraint",
		zap.String("task_name", t.TaskName),
		zap.String("task_flow", t.TaskFlow),
		zap.String("schema_name_s", t.Source.SchemaName),
		zap.String("table_name_s", t.Source.TableName),
		zap.String("foreign_key_s", t.Source.String(constant.StructCompareForeignStructureJSONFormat)),
		zap.String("schema_name_t", t.Target.SchemaName),
		zap.String("table_name_t", t.Target.TableName),
		zap.String("foreign_key_t", t.Target.String(constant.StructCompareUniqueStructureJSONFormat)))

	var (
		b       strings.Builder
		sqlStrs []string
	)

	addTCons, delTCons, _ := stringutil.CompareMapInter(t.Source.ForeignConstraints, t.Target.ForeignConstraints)
	if len(addTCons) != 0 || len(delTCons) != 0 {
		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"CATEGORY", "SOURCE", "TARGET", "CONSTRAINT", "SUGGEST"})

		b.WriteString("/*\n")
		b.WriteString(fmt.Sprintf("the task [%s] task_flow [%s] database table foreign constraint isn't different\n", t.TaskName, t.TaskFlow))

		for consName, consCol := range delTCons {
			if val, ok := consCol.(structure.ConstraintForeign); ok {
				tw.AppendRow(table.Row{"FOREIGN CONSTRAINT",
					fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName),
					fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName),
					fmt.Sprintf("%s[%s]", consName, val.ConstraintColumn),
					"Manual Drop Foreign Constraint",
				})

				switch {
				case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB):
					sqlStrs = append(sqlStrs, fmt.Sprintf("ALTER TABLE %s.%s DROP FOREIGN KEY %s;\n", t.Target.SchemaName, t.Target.TableName, consName))
				}
				continue
			}
			return "", fmt.Errorf("the oracle schema [%s] table [%s] foreign constraint [%v] assert ConstraintForeign failed, type: [%v]", t.Source.SchemaName, t.Source.TableName, consCol, reflect.TypeOf(consCol))
		}

		for consName, consCol := range addTCons {
			if val, ok := consCol.(structure.ConstraintForeign); ok {
				tw.AppendRow(table.Row{"FOREIGN CONSTRAINT",
					fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName),
					fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName),
					fmt.Sprintf("%s[%s]", consName, val.ConstraintColumn),
					"Manual Add Foreign Constraint",
				})

				switch {
				case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB):
					if !strings.EqualFold(val.DeleteRule, "") {
						sqlStrs = append(sqlStrs, fmt.Sprintf("ALTER TABLE %s.%s ADD FOREIGN KEY (%s) REFERENCES %s.%s(%s) ON DELETE %s;\n", t.Target.SchemaName, t.Target.TableName, consName, val.ReferencedTableSchema, val.ReferencedTableName, val.ReferencedColumnName, val.DeleteRule))
					}
					if !strings.EqualFold(val.UpdateRule, "") {
						sqlStrs = append(sqlStrs, fmt.Sprintf("ALTER TABLE %s.%s ADD FOREIGN KEY (%s) REFERENCES %s.%s(%s) ON UPDATE %s;\n", t.Target.SchemaName, t.Target.TableName, consName, val.ReferencedTableSchema, val.ReferencedTableName, val.ReferencedColumnName, val.UpdateRule))
					}
					if strings.EqualFold(val.DeleteRule, "") && strings.EqualFold(val.UpdateRule, "") {
						sqlStrs = append(sqlStrs, fmt.Sprintf("ALTER TABLE %s.%s ADD FOREIGN KEY (%s) REFERENCES %s.%s(%s);\n", t.Target.SchemaName, t.Target.TableName, consName, val.ReferencedTableSchema, val.ReferencedTableName, val.ReferencedColumnName))
					}
				}
				continue
			}
			return "", fmt.Errorf("the oracle schema [%s] table [%s] foreign constraint [%v] assert ConstraintForeign failed, type: [%v]", t.Source.SchemaName, t.Source.TableName, consCol, reflect.TypeOf(consCol))
		}

		b.WriteString(fmt.Sprintf("%v\n", tw.Render()))
		b.WriteString("*/\n")
		b.WriteString(strings.Join(sqlStrs, "\n") + "\n\n")

		logger.Info("compare table foreign constraint isn't equal",
			zap.String("task_name", t.TaskName),
			zap.String("task_flow", t.TaskFlow),
			zap.String("schema_name_s", t.Source.SchemaName),
			zap.String("table_name_s", t.Source.TableName),
			zap.String("foreign_key_s", t.Source.String(constant.StructCompareForeignStructureJSONFormat)),
			zap.String("schema_name_t", t.Target.SchemaName),
			zap.String("table_name_t", t.Target.TableName),
			zap.String("foreign_key_t", t.Target.String(constant.StructCompareForeignStructureJSONFormat)),
			zap.String("action", "drop or add"))
	}
	return b.String(), nil
}

func (t *Table) CompareTableCheckConstraint() (string, error) {
	logger.Info("compare table check constraint",
		zap.String("task_name", t.TaskName),
		zap.String("task_flow", t.TaskFlow),
		zap.String("schema_name_s", t.Source.SchemaName),
		zap.String("table_name_s", t.Source.TableName),
		zap.String("check_key_s", t.Source.String(constant.StructCompareCheckStructureJSONFormat)),
		zap.String("schema_name_t", t.Target.SchemaName),
		zap.String("table_name_t", t.Target.TableName),
		zap.String("check_key_t", t.Target.String(constant.StructCompareCheckStructureJSONFormat)))

	var (
		b       strings.Builder
		sqlStrs []string
	)

	addTCons, delTCons, _ := stringutil.CompareMapInter(t.Source.CheckConstraints, t.Target.CheckConstraints)
	if len(addTCons) != 0 || len(delTCons) != 0 {
		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"CATEGORY", "SOURCE", "TARGET", "CONSTRAINT", "SUGGEST"})

		b.WriteString("/*\n")
		b.WriteString(fmt.Sprintf("the task [%s] task_flow [%s] database table check constraint isn't different\n", t.TaskName, t.TaskFlow))

		for consName, consCol := range delTCons {
			if val, ok := consCol.(structure.ConstraintCheck); ok {
				tw.AppendRow(table.Row{"CHECK CONSTRAINT",
					fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName),
					fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName),
					fmt.Sprintf("%s[%s]", consName, val.ConstraintExpression),
					"Manual Drop Check Constraint",
				})

				switch {
				case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB):
					sqlStrs = append(sqlStrs, fmt.Sprintf("ALTER TABLE %s.%s DROP CHECK KEY %s;\n", t.Target.SchemaName, t.Target.TableName, consName))
				}
				continue
			}
			return "", fmt.Errorf("the oracle schema [%s] table [%s] check constraint [%v] assert ConstraintCheck failed, type: [%v]", t.Source.SchemaName, t.Source.TableName, consCol, reflect.TypeOf(consCol))
		}

		for consName, consCol := range addTCons {
			if val, ok := consCol.(structure.ConstraintCheck); ok {
				tw.AppendRow(table.Row{"CHECK CONSTRAINT",
					fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName),
					fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName),
					fmt.Sprintf("%s[%s]", consName, val.ConstraintExpression),
					"Manual Add Check Constraint",
				})

				switch {
				case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB):
					sqlStrs = append(sqlStrs, fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s CHECK %s;\n", t.Target.SchemaName, t.Target.TableName, consName, val.ConstraintExpression))
				}
				continue
			}
			return "", fmt.Errorf("the oracle schema [%s] table [%s] check constraint [%v] assert ConstraintCheck failed, type: [%v]", t.Source.SchemaName, t.Source.TableName, consCol, reflect.TypeOf(consCol))
		}

		b.WriteString(fmt.Sprintf("%v\n", tw.Render()))
		b.WriteString("*/\n")
		b.WriteString(strings.Join(sqlStrs, "\n") + "\n\n")

		logger.Info("compare table check constraint isn't equal",
			zap.String("task_name", t.TaskName),
			zap.String("task_flow", t.TaskFlow),
			zap.String("schema_name_s", t.Source.SchemaName),
			zap.String("table_name_s", t.Source.TableName),
			zap.String("check_key_s", t.Source.String(constant.StructCompareCheckStructureJSONFormat)),
			zap.String("schema_name_t", t.Target.SchemaName),
			zap.String("table_name_t", t.Target.TableName),
			zap.String("check_key_t", t.Target.String(constant.StructCompareCheckStructureJSONFormat)),
			zap.String("action", "drop or add"))
	}
	return b.String(), nil
}

func (t *Table) CompareTableIndexDetail() (string, error) {
	logger.Info("compare table index detail",
		zap.String("task_name", t.TaskName),
		zap.String("task_flow", t.TaskFlow),
		zap.String("schema_name_s", t.Source.SchemaName),
		zap.String("table_name_s", t.Source.TableName),
		zap.String("index_detail_s", t.Source.String(constant.StructCompareIndexStructureJSONFormat)),
		zap.String("schema_name_t", t.Target.SchemaName),
		zap.String("table_name_t", t.Target.TableName),
		zap.String("index_detail_t", t.Target.String(constant.StructCompareIndexStructureJSONFormat)))

	var (
		b       strings.Builder
		sqlStrs []string
	)

	addTCons, delTCons, _ := stringutil.CompareMapInter(t.Source.Indexes, t.Target.Indexes)
	if len(addTCons) != 0 || len(delTCons) != 0 {
		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"CATEGORY", "SOURCE", "TARGET", "UNIQUENESS", "INDEX TYPE", "INDEX DETAIL", "SUGGEST"})

		b.WriteString("/*\n")
		b.WriteString(fmt.Sprintf("the task [%s] task_flow [%s] database table index detail isn't different\n", t.TaskName, t.TaskFlow))

		for consName, consCol := range delTCons {
			if val, ok := consCol.(structure.Index); ok {
				tw.AppendRow(table.Row{"TABLE INDEX",
					fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName),
					fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName),
					val.Uniqueness,
					val.IndexType,
					fmt.Sprintf("%s[%s]", consName, val.IndexColumn),
					"Manual Drop Table Index",
				})

				switch {
				case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB):
					sqlStrs = append(sqlStrs, fmt.Sprintf("ALTER TABLE %s.%s DROP INDEX %s;", t.Target.SchemaName, t.Target.TableName, consName))
				}
				continue
			}
			return "", fmt.Errorf("the oracle schema [%s] table [%s] index detail [%v] assert Index failed, type: [%v]", t.Source.SchemaName, t.Source.TableName, consCol, reflect.TypeOf(consCol))
		}

		for consName, consCol := range addTCons {
			if val, ok := consCol.(structure.Index); ok {
				tw.AppendRow(table.Row{"TABLE INDEX",
					fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName),
					fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName),
					val.Uniqueness,
					val.IndexType,
					fmt.Sprintf("%s[%s]", consName, val.IndexColumn),
					"Manual Add Table Index",
				})

				switch {
				case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB):
					if val.Uniqueness == "UNIQUE" && val.IndexType == "NORMAL" {
						sqlStrs = append(sqlStrs, fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s.%s (%s);\n", consName, t.Target.SchemaName, t.Target.TableName, val.IndexColumn))
						continue
					}
					if val.Uniqueness == "UNIQUE" && val.IndexType == "FUNCTION-BASED NORMAL" {
						sqlStrs = append(sqlStrs, fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s.%s (%s);\n", consName, t.Target.SchemaName, t.Target.TableName, val.IndexColumn))
						continue
					}
					if val.Uniqueness == "NONUNIQUE" && val.IndexType == "NORMAL" {
						sqlStrs = append(sqlStrs, fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s);\n", consName, t.Target.SchemaName, t.Target.TableName, val.IndexColumn))
						continue
					}
					if val.Uniqueness == "NONUNIQUE" && val.IndexType == "BITMAP" {
						sqlStrs = append(sqlStrs, fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);\n", consName, t.Target.SchemaName, t.Target.TableName, val.IndexColumn))
						continue
					}
					if val.Uniqueness == "NONUNIQUE" && val.IndexType == "FUNCTION-BASED NORMAL" {
						sqlStrs = append(sqlStrs, fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s);\n", consName, t.Target.SchemaName, t.Target.TableName, val.IndexColumn))
						continue
					}
					if val.Uniqueness == "NONUNIQUE" && val.IndexType == "FUNCTION-BASED BITMAP" {
						sqlStrs = append(sqlStrs, fmt.Sprintf("CREATE BITMAP INDEX %s ON %s.%s (%s);\n", consName, t.Target.SchemaName, t.Target.TableName, val.IndexColumn))
						continue
					}
					if val.Uniqueness == "NONUNIQUE" && val.IndexType == "DOMAIN" {
						sqlStrs = append(sqlStrs, fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s) INDEXTYPE IS %s.%s PARAMETERS ('%s');\n", consName, t.Target.SchemaName, t.Target.TableName, val.IndexColumn, val.DomainIndexOwner, val.DomainIndexName, val.DomainParameters))
						continue
					}
					return b.String(), fmt.Errorf("the oracle schema [%s] table [%s] compare failed, not support index: [%v]", t.Target.SchemaName, t.Target.TableName, t.Source.String(constant.StructCompareIndexStructureJSONFormat))
				}
				continue
			}
			return "", fmt.Errorf("the oracle schema [%s] table [%s] check constraint [%v] assert ConstraintCheck failed, type: [%v]", t.Source.SchemaName, t.Source.TableName, consCol, reflect.TypeOf(consCol))
		}

		b.WriteString(fmt.Sprintf("%v\n", tw.Render()))
		b.WriteString("*/\n")
		b.WriteString(strings.Join(sqlStrs, "\n") + "\n\n")

		logger.Info("compare table index detail isn't equal",
			zap.String("task_name", t.TaskName),
			zap.String("task_flow", t.TaskFlow),
			zap.String("schema_name_s", t.Source.SchemaName),
			zap.String("table_name_s", t.Source.TableName),
			zap.String("index_detail_s", t.Source.String(constant.StructCompareIndexStructureJSONFormat)),
			zap.String("schema_name_t", t.Target.SchemaName),
			zap.String("table_name_t", t.Target.TableName),
			zap.String("index_detail_t", t.Target.String(constant.StructCompareIndexStructureJSONFormat)),
			zap.String("action", "drop or add"))
	}
	return b.String(), nil
}

func (t *Table) CompareTableColumnDetail() (string, error) {
	logger.Info("compare table column detail",
		zap.String("task_name", t.TaskName),
		zap.String("task_flow", t.TaskFlow),
		zap.String("schema_name_s", t.Source.SchemaName),
		zap.String("table_name_s", t.Source.TableName),
		zap.String("column_detail_s", t.Source.String(constant.StructCompareColumnsStructureJSONFormat)),
		zap.String("schema_name_t", t.Target.SchemaName),
		zap.String("table_name_t", t.Target.TableName),
		zap.String("column_detail_t", t.Target.String(constant.StructCompareColumnsStructureJSONFormat)))

	var (
		b       strings.Builder
		sqlStrs []string
	)

	addTCons, delTCons, modTCons := stringutil.CompareMapInter(t.Source.NewColumns, t.Target.NewColumns)
	if len(addTCons) != 0 || len(delTCons) != 0 || len(modTCons) != 0 {
		oldColumns := make(map[string]string)
		for _, olds := range t.Source.OldColumns {
			for k, v := range olds {
				oldColumns[k] = v.DatatypeName
			}
		}

		tw := table.NewWriter()
		tw.SetStyle(table.StyleLight)
		tw.AppendHeader(table.Row{"CATEGORY", "SOURCE", "TARGET", "COLUMN_NAME_S", "COLUMN_NAME_T", "SUGGEST"})

		b.WriteString("/*\n")
		b.WriteString(fmt.Sprintf("the task [%s] task_flow [%s] database table column detail isn't different\n", t.TaskName, t.TaskFlow))

		for colName, colInfo := range delTCons {
			if val, ok := colInfo.(structure.NewColumn); ok {
				tw.AppendRow(table.Row{"TABLE COLUMN",
					fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName),
					fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName),
					fmt.Sprintf("%s[%s]", colName, oldColumns[colName]),
					fmt.Sprintf("%s[%s]", colName, val.Datatype),
					"Manual Drop Table Column",
				})

				switch {
				case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB):
					sqlStrs = append(sqlStrs, fmt.Sprintf("ALTER TABLE %s.%s DROP COLUMN %s;", t.Target.SchemaName, t.Target.TableName, colName))
				}
				continue
			}
			return "", fmt.Errorf("the oracle schema [%s] table [%s] column detail [%v] drop assert NewColumn failed, type: [%v]", t.Source.SchemaName, t.Source.TableName, colInfo, reflect.TypeOf(colInfo))
		}

		for colName, colInfo := range addTCons {
			if val, ok := colInfo.(structure.NewColumn); ok {
				tw.AppendRow(table.Row{"TABLE COLUMN",
					fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName),
					fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName),
					fmt.Sprintf("%s[%s]", colName, oldColumns[colName]),
					fmt.Sprintf("%s[%s]", colName, t.Target.NewColumns[colName].Datatype),
					"Manual Add Table Column",
				})

				switch {
				case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB):
					sqlStrs = append(sqlStrs, fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s %s;", t.Target.SchemaName, t.Target.TableName, colName, t.genAlterTableColumnDetail(val)))
				}
				continue
			}
			return "", fmt.Errorf("the oracle schema [%s] table [%s] column detail [%v] add assert ConstraintCheck failed, type: [%v]", t.Source.SchemaName, t.Source.TableName, colName, reflect.TypeOf(colInfo))
		}

		for colName, colInfo := range modTCons {
			if val, ok := colInfo.(structure.NewColumn); ok {
				tw.AppendRow(table.Row{"TABLE COLUMN",
					fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName),
					fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName),
					fmt.Sprintf("%s[%s]", colName, oldColumns[colName]),
					fmt.Sprintf("%s[%s]", colName, t.Target.NewColumns[colName].Datatype),
					"Manual Modify Table Column",
				})

				switch {
				case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB):
					sqlStrs = append(sqlStrs, fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s;", t.Target.SchemaName, t.Target.TableName, colName, t.genAlterTableColumnDetail(val)))
				}
				continue
			}
			return "", fmt.Errorf("the oracle schema [%s] table [%s] column detail [%v] modify assert ConstraintCheck failed, type: [%v]", t.Source.SchemaName, t.Source.TableName, colName, reflect.TypeOf(colInfo))
		}

		b.WriteString(fmt.Sprintf("%v\n", tw.Render()))
		b.WriteString("*/\n")
		b.WriteString(strings.Join(sqlStrs, "\n") + "\n\n")

		logger.Info("compare table column detail isn't equal",
			zap.String("task_name", t.TaskName),
			zap.String("task_flow", t.TaskFlow),
			zap.String("schema_name_s", t.Source.SchemaName),
			zap.String("table_name_s", t.Source.TableName),
			zap.String("column_detail_s", t.Source.String(constant.StructCompareColumnsStructureJSONFormat)),
			zap.String("schema_name_t", t.Target.SchemaName),
			zap.String("table_name_t", t.Target.TableName),
			zap.String("column_detail_t", t.Target.String(constant.StructCompareColumnsStructureJSONFormat)),
			zap.String("action", "drop or add or modify"))
	}
	return b.String(), nil
}

func (t *Table) CompareTablePartitionDetail() (string, error) {
	logger.Info("compare partition table",
		zap.String("task_name", t.TaskName),
		zap.String("task_flow", t.TaskFlow),
		zap.String("schema_name_s", t.Source.SchemaName),
		zap.String("table_name_s", t.Source.TableName),
		zap.String("table_partition_s", t.Source.String(constant.StructComparePartitionStructureJSONFormat)),
		zap.String("schema_name_t", t.Target.SchemaName),
		zap.String("table_name_t", t.Target.TableName),
		zap.String("table_partition_t", t.Target.String(constant.StructComparePartitionStructureJSONFormat)))

	var b strings.Builder

	if len(t.Source.Partitions) > 0 && len(t.Target.Partitions) > 0 {
		addParts, delParts := stringutil.CompareInter(t.Source.Partitions, t.Target.Partitions)
		if len(addParts) != 0 && len(delParts) != 0 {
			tw := table.NewWriter()
			tw.SetStyle(table.StyleLight)
			tw.AppendHeader(table.Row{"CATEGORY", "SOURCE", "TARGET", "SUGGEST"})

			b.WriteString("/*\n")
			b.WriteString(fmt.Sprintf("the task [%s] task_flow [%s] database table partition detail isn't different\n", t.TaskName, t.TaskFlow))

			if len(delParts) > 0 {
				b.WriteString("DELETE PARTITION DETAIL:\n")
			}
			for _, p := range delParts {
				if _, ok := p.(structure.Partition); ok {
					tw.AppendRow(table.Row{"TABLE PARTITION",
						fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName),
						fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName),
						"Manual Drop Table Partition",
					})
					b.WriteString(t.Source.String(constant.StructComparePartitionStructureJSONFormat) + "\n")
					b.WriteString(t.Target.String(constant.StructComparePartitionStructureJSONFormat) + "\n\n")
					continue
				}
				return "", fmt.Errorf("the oracle schema [%s] table [%s] paritions [%v] assert Partition failed, type: [%v]", t.Source.SchemaName, t.Source.TableName, p, reflect.TypeOf(p))
			}

			if len(delParts) > 0 {
				b.WriteString("ADD PARTITION DETAIL:\n")
			}
			for _, p := range addParts {
				if _, ok := p.(structure.Partition); ok {
					tw.AppendRow(table.Row{"TABLE PARTITION",
						fmt.Sprintf("%s.%s", t.Source.SchemaName, t.Source.TableName),
						fmt.Sprintf("%s.%s", t.Target.SchemaName, t.Target.TableName),
						"Manual Add Table Partition",
					})
					b.WriteString(t.Source.String(constant.StructComparePartitionStructureJSONFormat) + "\n")
					b.WriteString(t.Target.String(constant.StructComparePartitionStructureJSONFormat) + "\n\n")
					continue
				}
				return "", fmt.Errorf("the oracle schema [%s] table [%s] paritions [%v] assert Partition failed, type: [%v]", t.Source.SchemaName, t.Source.TableName, p, reflect.TypeOf(p))
			}
			b.WriteString(fmt.Sprintf("%v\n", tw.Render()))
			b.WriteString("*/\n")

			logger.Info("compare partition table isn't equal",
				zap.String("task_name", t.TaskName),
				zap.String("task_flow", t.TaskFlow),
				zap.String("schema_name_s", t.Source.SchemaName),
				zap.String("table_name_s", t.Source.TableName),
				zap.String("table_partition_s", t.Source.String(constant.StructComparePartitionStructureJSONFormat)),
				zap.String("schema_name_t", t.Target.SchemaName),
				zap.String("table_name_t", t.Target.TableName),
				zap.String("table_partition_t", t.Target.String(constant.StructComparePartitionStructureJSONFormat)),
				zap.String("action", "drop or add"))
		}
	}
	return b.String(), nil
}

func (t *Table) genAlterTableColumnDetail(newColumn structure.NewColumn) string {
	var sqlSuffix string
	switch {
	case strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(t.TaskFlow, constant.TaskFlowOracleToTiDB):
		if strings.EqualFold(newColumn.Charset, "UNKNOWN") && strings.EqualFold(newColumn.Collation, "UNKNOWN") {
			sqlSuffix = fmt.Sprintf("%s %s", newColumn.Datatype, t.genTableColumnDefaultCommentMeta(newColumn.NULLABLE, newColumn.Comment, newColumn.DataDefault))
		} else {
			sqlSuffix = fmt.Sprintf("%s CHARACTER SET %s COLLATE %s %s", newColumn.Datatype, newColumn.Charset, newColumn.Collation, t.genTableColumnDefaultCommentMeta(newColumn.NULLABLE, newColumn.Comment, newColumn.DataDefault))
		}
	}
	return sqlSuffix
}

func (t *Table) genTableColumnDefaultCommentMeta(nullable, comment, defaultVal string) string {
	var colMeta string

	// NULLABLE
	if strings.EqualFold(nullable, "Y") {
		nullable = "NULL"
	} else {
		nullable = "NOT NULL"
	}

	if nullable == "NULL" {
		switch {
		case comment != "" && defaultVal != "":
			colMeta = fmt.Sprintf("DEFAILT %s COMMENT '%s'", defaultVal, comment)
		case comment != "" && defaultVal == "":
			colMeta = fmt.Sprintf("DEFAULT NULL COMMENT '%s'", comment)
		case comment == "" && defaultVal != "":
			colMeta = fmt.Sprintf("DEFAULT %s", defaultVal)
		case comment == "" && defaultVal == "":
			colMeta = "DEFAULT NULL"
		}
	} else {
		switch {
		case comment != "" && defaultVal != "":
			colMeta = fmt.Sprintf("%s DEFAULT %s COMMENT '%s'", nullable, defaultVal, comment)
		case comment != "" && defaultVal == "":
			colMeta = fmt.Sprintf("%s COMMENT '%s'", nullable, comment)
		case comment == "" && defaultVal != "":
			colMeta = fmt.Sprintf("%s DEFAULT %s", nullable, defaultVal)
		case comment == "" && defaultVal == "":
			colMeta = fmt.Sprintf("%s", nullable)
		}
	}
	return colMeta
}
