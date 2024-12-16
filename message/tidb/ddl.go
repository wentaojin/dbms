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

// DDLType is the type for DDL DDL.
type DDLType int

// List DDL actions.
const (
	DDLNone                          DDLType = 0
	DDLCreateSchema                  DDLType = 1
	DDLDropSchema                    DDLType = 2
	DDLCreateTable                   DDLType = 3
	DDLDropTable                     DDLType = 4
	DDLAddColumn                     DDLType = 5
	DDLDropColumn                    DDLType = 6
	DDLAddIndex                      DDLType = 7
	DDLDropIndex                     DDLType = 8
	DDLAddForeignKey                 DDLType = 9
	DDLDropForeignKey                DDLType = 10
	DDLTruncateTable                 DDLType = 11
	DDLModifyColumn                  DDLType = 12
	DDLRebaseAutoID                  DDLType = 13
	DDLRenameTable                   DDLType = 14
	DDLSetDefaultValue               DDLType = 15
	DDLShardRowID                    DDLType = 16
	DDLModifyTableComment            DDLType = 17
	DDLRenameIndex                   DDLType = 18
	DDLAddTablePartition             DDLType = 19
	DDLDropTablePartition            DDLType = 20
	DDLCreateView                    DDLType = 21
	DDLModifyTableCharsetAndCollate  DDLType = 22
	DDLTruncateTablePartition        DDLType = 23
	DDLDropView                      DDLType = 24
	DDLRecoverTable                  DDLType = 25
	DDLModifySchemaCharsetAndCollate DDLType = 26
	DDLLockTable                     DDLType = 27
	DDLUnlockTable                   DDLType = 28
	DDLRepairTable                   DDLType = 29
	DDLSetTiFlashReplica             DDLType = 30
	DDLUpdateTiFlashReplicaStatus    DDLType = 31
	DDLAddPrimaryKey                 DDLType = 32
	DDLDropPrimaryKey                DDLType = 33
	DDLCreateSequence                DDLType = 34
	DDLAlterSequence                 DDLType = 35
	DDLDropSequence                  DDLType = 36
	DDLAddColumns                    DDLType = 37 // Deprecated, we use DDLMultiSchemaChange instead.
	DDLDropColumns                   DDLType = 38 // Deprecated, we use DDLMultiSchemaChange instead.
	DDLModifyTableAutoIDCache        DDLType = 39
	DDLRebaseAutoRandomBase          DDLType = 40
	DDLAlterIndexVisibility          DDLType = 41
	DDLExchangeTablePartition        DDLType = 42
	DDLAddCheckConstraint            DDLType = 43
	DDLDropCheckConstraint           DDLType = 44
	DDLAlterCheckConstraint          DDLType = 45

	// `DDLAlterTableAlterPartition` is removed and will never be used.
	// Just left a tombstone here for compatibility.
	_DEPRECATEDDDLAlterTableAlterPartition DDLType = 46

	DDLRenameTables                  DDLType = 47
	_DEPRECATEDDDLDropIndexes        DDLType = 48 // Deprecated, we use DDLMultiSchemaChange instead.
	DDLAlterTableAttributes          DDLType = 49
	DDLAlterTablePartitionAttributes DDLType = 50
	DDLCreatePlacementPolicy         DDLType = 51
	DDLAlterPlacementPolicy          DDLType = 52
	DDLDropPlacementPolicy           DDLType = 53
	DDLAlterTablePartitionPlacement  DDLType = 54
	DDLModifySchemaDefaultPlacement  DDLType = 55
	DDLAlterTablePlacement           DDLType = 56
	DDLAlterCacheTable               DDLType = 57
	// not used
	DDLAlterTableStatsOptions DDLType = 58
	DDLAlterNoCacheTable      DDLType = 59
	DDLCreateTables           DDLType = 60
	DDLMultiSchemaChange      DDLType = 61
	DDLFlashbackCluster       DDLType = 62
	DDLRecoverSchema          DDLType = 63
	DDLReorganizePartition    DDLType = 64
	DDLAlterTTLInfo           DDLType = 65
	DDLAlterTTLRemove         DDLType = 67
	DDLCreateResourceGroup    DDLType = 68
	DDLAlterResourceGroup     DDLType = 69
	DDLDropResourceGroup      DDLType = 70
	DDLAlterTablePartitioning DDLType = 71
	DDLRemovePartitioning     DDLType = 72
	DDLAddVectorIndex         DDLType = 73
)

// DDLTypeMap is the map of DDL DDLType to string.
var DDLTypeMap = map[DDLType]string{
	DDLCreateSchema:                  "create schema",
	DDLDropSchema:                    "drop schema",
	DDLCreateTable:                   "create table",
	DDLCreateTables:                  "create tables",
	DDLDropTable:                     "drop table",
	DDLAddColumn:                     "add column",
	DDLDropColumn:                    "drop column",
	DDLAddIndex:                      "add index",
	DDLDropIndex:                     "drop index",
	DDLAddForeignKey:                 "add foreign key",
	DDLDropForeignKey:                "drop foreign key",
	DDLTruncateTable:                 "truncate table",
	DDLModifyColumn:                  "modify column",
	DDLRebaseAutoID:                  "rebase auto_increment ID",
	DDLRenameTable:                   "rename table",
	DDLRenameTables:                  "rename tables",
	DDLSetDefaultValue:               "set default value",
	DDLShardRowID:                    "shard row ID",
	DDLModifyTableComment:            "modify table comment",
	DDLRenameIndex:                   "rename index",
	DDLAddTablePartition:             "add partition",
	DDLDropTablePartition:            "drop partition",
	DDLCreateView:                    "create view",
	DDLModifyTableCharsetAndCollate:  "modify table charset and collate",
	DDLTruncateTablePartition:        "truncate partition",
	DDLDropView:                      "drop view",
	DDLRecoverTable:                  "recover table",
	DDLModifySchemaCharsetAndCollate: "modify schema charset and collate",
	DDLLockTable:                     "lock table",
	DDLUnlockTable:                   "unlock table",
	DDLRepairTable:                   "repair table",
	DDLSetTiFlashReplica:             "set tiflash replica",
	DDLUpdateTiFlashReplicaStatus:    "update tiflash replica status",
	DDLAddPrimaryKey:                 "add primary key",
	DDLDropPrimaryKey:                "drop primary key",
	DDLCreateSequence:                "create sequence",
	DDLAlterSequence:                 "alter sequence",
	DDLDropSequence:                  "drop sequence",
	DDLModifyTableAutoIDCache:        "modify auto id cache",
	DDLRebaseAutoRandomBase:          "rebase auto_random ID",
	DDLAlterIndexVisibility:          "alter index visibility",
	DDLExchangeTablePartition:        "exchange partition",
	DDLAddCheckConstraint:            "add check constraint",
	DDLDropCheckConstraint:           "drop check constraint",
	DDLAlterCheckConstraint:          "alter check constraint",
	DDLAlterTableAttributes:          "alter table attributes",
	DDLAlterTablePartitionPlacement:  "alter table partition placement",
	DDLAlterTablePartitionAttributes: "alter table partition attributes",
	DDLCreatePlacementPolicy:         "create placement policy",
	DDLAlterPlacementPolicy:          "alter placement policy",
	DDLDropPlacementPolicy:           "drop placement policy",
	DDLModifySchemaDefaultPlacement:  "modify schema default placement",
	DDLAlterTablePlacement:           "alter table placement",
	DDLAlterCacheTable:               "alter table cache",
	DDLAlterNoCacheTable:             "alter table nocache",
	DDLAlterTableStatsOptions:        "alter table statistics options",
	DDLMultiSchemaChange:             "alter table multi-schema change",
	DDLFlashbackCluster:              "flashback cluster",
	DDLRecoverSchema:                 "flashback schema",
	DDLReorganizePartition:           "alter table reorganize partition",
	DDLAlterTTLInfo:                  "alter table ttl",
	DDLAlterTTLRemove:                "alter table no_ttl",
	DDLCreateResourceGroup:           "create resource group",
	DDLAlterResourceGroup:            "alter resource group",
	DDLDropResourceGroup:             "drop resource group",
	DDLAlterTablePartitioning:        "alter table partition by",
	DDLRemovePartitioning:            "alter table remove partitioning",
	DDLAddVectorIndex:                "add vector index",

	// `DDLAlterTableAlterPartition` is removed and will never be used.
	// Just left a tombstone here for compatibility.
	_DEPRECATEDDDLAlterTableAlterPartition: "alter partition",
}

// String return current ddl DDL in string
func (d DDLType) String() string {
	if v, ok := DDLTypeMap[d]; ok {
		return v
	}
	return "none"
}
