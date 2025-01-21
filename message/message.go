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
package message

const (
	DMLDeleteQueryType = "DELETE"
	DMLInsertQueryType = "INSERT"
	DMLUpdateQueryType = "UPDATE"
)

var (
	MYSQLCompatibleMsgColumnStringCharacterDatatype = []string{
		"TINYTEXT",
		"MEDIUMTEXT",
		"TEXT",
		"LONGTEXT",
		"VARCHAR",
		"JSON",
		"CHAR",
	}
	MYSQLCompatibleMsgColumnDatetimeCharacterDatatype = []string{
		"DATE",
		"DATETIME",
		"TIMESTAMP",
	}
	MYSQLCompatibleMsgColumnYearCharacterDatatype = []string{
		"YEAR",
	}
	MYSQLCompatibleMsgColumnTimeCharacterDatatype = []string{
		"TIME",
	}
	// all downstream databases except MySQL and TiDB use the unified adaptive field length to automatically fill spaces in DELETE statements of char type to avoid data processing errors due to missing spaces (for example, Oracle database queries or DML operations do not automatically fill spaces), while INSERT statements use the original value of the upstream TiDB char data type to consume synchronously.
	MYSQLCompatibleMsgColumnCharCharacterDatatype = []string{
		"CHAR",
	}

	// Byte array, displayed in BASE64 encoding by default.
	// Note: For BIT fixed-length types, the high-order 0s will be removed after the byte array is received incrementally, but not for the full amount, so the BASE64 encoding you see may be inconsistent. However, the actual results are consistent, and the results after decoding are consistent.
	OceanbaseMsgColumnBytesDatatype = []string{
		"TINYBLOB",
		"BLOB",
		"MEDIUMBLOB",
		"LONGBLOB",
		"BINARY",
		"VARBINARY",
		"BIT",
	}
)
