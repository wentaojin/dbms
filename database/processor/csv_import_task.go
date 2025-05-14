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
	"strconv"
	"strings"

	"github.com/wentaojin/dbms/database"
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
	Thread                string `json:"thread"`
	MaxWriteSpeed         string `json:"maxWriteSpeed"`
	ChecksumTable         string `json:"checksumTable"`
	CloudStorageUri       string `json:"cloudStorageUri"`
	Detached              string `json:"detached"`
	DisableTikvImportMode string `json:"disableTikvImportMode"`
	SplitFile             string `json:"splitFile"` // strict-format
}

func (p *ImportTiDBParams) Builder() (string, error) {
	var bs []string
	if !strings.EqualFold(p.DiskQuota, "") {
		bs = append(bs, fmt.Sprintf("DISK_QUOTA='%s'", p.DiskQuota))
	}
	if !strings.EqualFold(p.Thread, "") {
		bs = append(bs, fmt.Sprintf("THREAD=%v", p.Thread))
	}
	if !strings.EqualFold(p.MaxWriteSpeed, "") {
		bs = append(bs, fmt.Sprintf("MAX_WRITE_SPEED='%s'", p.MaxWriteSpeed))
	}
	if !strings.EqualFold(p.ChecksumTable, "") {
		bs = append(bs, fmt.Sprintf("CHECKSUM_TABLE = '%s'", p.ChecksumTable))
	}
	if !strings.EqualFold(p.SplitFile, "") {
		parseBool, err := strconv.ParseBool(p.SplitFile)
		if err != nil {
			return "", err
		}
		if parseBool {
			bs = append(bs, "SPLIT_FILE")
		}
	}
	if !strings.EqualFold(p.Detached, "") {
		parseBool, err := strconv.ParseBool(p.Detached)
		if err != nil {
			return "", err
		}
		if parseBool {
			bs = append(bs, "DETACHED")
		}
	}
	if !strings.EqualFold(p.DisableTikvImportMode, "") {
		parseBool, err := strconv.ParseBool(p.DisableTikvImportMode)
		if err != nil {
			return "", err
		}
		if parseBool {
			bs = append(bs, "DISABLE_TIKV_IMPORT_MODE")
		}
	}

	if len(bs) > 0 {
		return strings.Join(bs, ","), nil
	}
	return "", nil
}

func (i *ImportTiDBDatabase) Import(ctx context.Context, db database.IDatabase, schemaNameT, tableNameT, columnNameT, outputCsvDir string) error {
	var sqlStr string
	bs, err := i.Builder()
	if err != nil {
		return err
	}
	if strings.EqualFold(bs, "") {
		sqlStr = fmt.Sprintf("IMPORT INTO `%s`.`%s` (%s) FROM '%s/*.csv' WITH CHARACTER_SET='%s',FIELDS_TERMINATED_BY='%s',FIELDS_ENCLOSED_BY='%s',FIELDS_ESCAPED_BY='%s',FIELDS_DEFINED_NULL_BY='%s',LINES_TERMINATED_BY='%s',SKIP_ROWS=%d",
			schemaNameT, tableNameT, columnNameT, outputCsvDir, i.CharsetSet, i.FieldsTerminatedBy, i.FieldsEnclosedBy, i.FieldsEscapedBy, i.FieldsDefinedNullBy, i.LinesTerminatedBy, i.SkipRows)
	} else {
		sqlStr = fmt.Sprintf("IMPORT INTO `%s`.`%s` (%s) FROM '%s/*.csv' WITH CHARACTER_SET='%s',FIELDS_TERMINATED_BY='%s',FIELDS_ENCLOSED_BY='%s',FIELDS_ESCAPED_BY='%s',FIELDS_DEFINED_NULL_BY='%s',LINES_TERMINATED_BY='%s',SKIP_ROWS=%d,%v",
			schemaNameT, tableNameT, columnNameT, outputCsvDir, i.CharsetSet, i.FieldsTerminatedBy, i.FieldsEnclosedBy, i.FieldsEscapedBy, i.FieldsDefinedNullBy, i.LinesTerminatedBy, i.SkipRows, bs)
	}
	_, err = db.ExecContext(ctx, sqlStr)
	if err != nil {
		return err
	}
	return nil
}
