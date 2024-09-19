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
	"context"
	"fmt"
	"github.com/wentaojin/dbms/database"
	"strings"
)

type ImportTiDBDatabase struct {
	CharsetSet          string `json:"charsetSet"`
	FieldsTerminatedBy  string `json:"fieldsTerminatedBy"`
	FieldsEnclosedBy    string `json:"fieldsEnclosedBy"`
	FieldsEscapedBy     string `json:"fieldsEscapedBy"`
	FieldsDefinedNullBy string `json:"fieldsDefinedNullBy"`
	LinesTerminatedBy   string `json:"linesTerminatedBy"`
	SkipRows            int    `json:"skipRows"`
	ImportTiDBParams
}

type ImportTiDBParams struct {
	DiskQuota             string `json:"diskQuota"`
	Thread                int    `json:"thread"`
	MaxWriteSpeed         string `json:"maxWriteSpeed"`
	ChecksumTable         string `json:"checksumTable"`
	CloudStorageUri       string `json:"cloudStorageUri"`
	Detached              bool   `json:"detached"`
	DisableTikvImportMode bool   `json:"disableTikvImportMode"`
	SplitFile             bool   `json:"splitFile"` // strict-format
}

func (p *ImportTiDBParams) Builder() string {
	var bs []string
	if !strings.EqualFold(p.DiskQuota, "") {
		bs = append(bs, fmt.Sprintf("DISK_QUOTA='%s'", p.DiskQuota))
	}
	if p.Thread != 0 {
		bs = append(bs, fmt.Sprintf("THREAD=%v", p.Thread))
	}
	if !strings.EqualFold(p.MaxWriteSpeed, "") {
		bs = append(bs, fmt.Sprintf("MAX_WRITE_SPEED='%s'", p.MaxWriteSpeed))
	}
	if !strings.EqualFold(p.ChecksumTable, "") {
		bs = append(bs, fmt.Sprintf("CHECKSUM_TABLE = '%s'", p.ChecksumTable))
	}
	if !p.SplitFile {
		bs = append(bs, "SPLIT_FILE")
	}
	if !p.Detached {
		bs = append(bs, "DETACHED")
	}
	if !p.DisableTikvImportMode {
		bs = append(bs, "DISABLE_TIKV_IMPORT_MODE")
	}

	if len(bs) > 0 {
		return strings.Join(bs, ",")
	}
	return ""
}

func (i *ImportTiDBDatabase) Import(ctx context.Context, db database.IDatabase, schemaNameT, tableNameT, columnNameT, outputCsvDir string) error {
	var sqlStr string
	if strings.EqualFold(i.Builder(), "") {
		sqlStr = fmt.Sprintf("IMPORT INTO `%s`.`%s` (%s) FROM '%s/*.csv' WITH CHARACTER_SET='%s',FIELDS_TERMINATED_BY='%s',FIELDS_ENCLOSED_BY='%s',FIELDS_ESCAPED_BY='%s',FIELDS_DEFINED_NULL_BY='%s',LINES_TERMINATED_BY='%s',SKIP_ROWS=%d",
			schemaNameT, tableNameT, columnNameT, outputCsvDir, i.CharsetSet, i.FieldsTerminatedBy, i.FieldsEnclosedBy, i.FieldsEscapedBy, i.FieldsDefinedNullBy, i.LinesTerminatedBy, i.SkipRows)
	} else {
		sqlStr = fmt.Sprintf("IMPORT INTO `%s`.`%s` (%s) FROM '%s/*.csv' WITH CHARACTER_SET='%s',FIELDS_TERMINATED_BY='%s',FIELDS_ENCLOSED_BY='%s',FIELDS_ESCAPED_BY='%s',FIELDS_DEFINED_NULL_BY='%s',LINES_TERMINATED_BY='%s',SKIP_ROWS=%d,%v",
			schemaNameT, tableNameT, columnNameT, outputCsvDir, i.CharsetSet, i.FieldsTerminatedBy, i.FieldsEnclosedBy, i.FieldsEscapedBy, i.FieldsDefinedNullBy, i.LinesTerminatedBy, i.SkipRows, i.Builder())
	}
	_, err := db.ExecContext(ctx, sqlStr)
	if err != nil {
		return err
	}
	return nil
}
