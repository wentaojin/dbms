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
package buildin

import (
	"github.com/wentaojin/dbms/utils/constant"
)

func InitO2MBuildinCompatibleRule() []*BuildinCompatibleRule {
	var buildinObjComps []*BuildinCompatibleRule
	/*
		O2M Build-IN Compatible Rule
	*/
	// oracle character set
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCharsetAL32UTF8,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCharsetZHS16GBK,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	// oracle table type
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleTableTypeHeap,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleTableTypeClustered,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleTableTypeTemporary,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleTableTypePartition,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	// oracle constraint type
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleConstraintTypePrimary,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleConstraintTypeUnique,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleConstraintTypeCheck,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleConstraintTypeForeign,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	// oracle index type
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleIndexTypeNormal,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleIndexTypeFunctionBasedNormal,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleIndexTypeBitmap,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleIndexTypeFunctionBasedBitmap,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleIndexTypeDomain,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	// oracle view type
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleViewTypeView,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	// oracle code type
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeMaterializedView,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeCluster,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeConsumerGroup,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeContext,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeDestination,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeDirectory,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeEdition,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeEvaluationContext,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeFunction,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeIndexPartition,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeIndexType,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeJavaClass,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeJavaData,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeJavaResource,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeJavaSource,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeJob,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeJobClass,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeLibrary,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeLob,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeLobPartition,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeLockdownProfile,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeOperator,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypePackage,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypePackageBody,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeProcedure,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeProgram,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeQueue,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeResourcePlan,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeRule,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeRuleSet,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeSchedule,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeSchedulerGroup,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeSequence,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeTrigger,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeType,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeTypeBody,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeUndefined,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeUnifiedAuditPolicy,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeWindow,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeXMLSchema,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeDatabaseLink,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleCodeTypeSynonym,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})

	// oracle partitions/subpartition type
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOraclePartitionTypeRange,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOraclePartitionTypeList,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOraclePartitionTypeHash,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOraclePartitionTypeSystem,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOraclePartitionTypeReference,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOraclePartitionTypeReference,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOraclePartitionTypeComposite,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOraclePartitionTypeInterval,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOraclePartitionTypeRangeHash,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOraclePartitionTypeRangeList,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOraclePartitionTypeRangeRange,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOraclePartitionTypeListHash,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOraclePartitionTypeListHash,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOraclePartitionTypeListList,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOraclePartitionTypeListRange,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})

	// oracle temporary type
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleTemporaryTypeSession,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeMySQL,
		ObjectNameS:   constant.BuildInOracleTemporaryTypeTransaction,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})

	return buildinObjComps
}

func InitO2TBuildinCompatibleRule() []*BuildinCompatibleRule {
	var buildinObjComps []*BuildinCompatibleRule
	/*
		O2T Build-IN Compatible Rule
	*/
	// oracle character set
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCharsetAL32UTF8,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCharsetZHS16GBK,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	// oracle table type
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleTableTypeHeap,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleTableTypeClustered,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleTableTypeTemporary,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleTableTypePartition,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	// oracle constraint type
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleConstraintTypePrimary,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleConstraintTypeUnique,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleConstraintTypeCheck,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleConstraintTypeForeign,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	// oracle index type
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleIndexTypeNormal,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleIndexTypeFunctionBasedNormal,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleIndexTypeBitmap,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleIndexTypeFunctionBasedBitmap,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleIndexTypeDomain,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	// oracle view type
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleViewTypeView,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessYesConvertible,
	})
	// oracle code type
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeMaterializedView,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeCluster,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeConsumerGroup,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeContext,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeDestination,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeDirectory,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeEdition,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeEvaluationContext,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeFunction,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeIndexPartition,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeIndexType,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeJavaClass,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeJavaData,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeJavaResource,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeJavaSource,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeJob,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeJobClass,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeLibrary,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeLob,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeLobPartition,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeLockdownProfile,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeOperator,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypePackage,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypePackageBody,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeProcedure,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeProgram,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeQueue,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeResourcePlan,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeRule,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeRuleSet,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeSchedule,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeSchedulerGroup,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeSequence,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeTrigger,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeType,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeTypeBody,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeUndefined,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeUnifiedAuditPolicy,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeWindow,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeXMLSchema,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeDatabaseLink,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleCodeTypeSynonym,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})

	// oracle partitions/subpartition type
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOraclePartitionTypeRange,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOraclePartitionTypeList,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOraclePartitionTypeHash,
		IsCompatible:  constant.AssessYesCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOraclePartitionTypeSystem,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOraclePartitionTypeReference,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOraclePartitionTypeReference,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOraclePartitionTypeComposite,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOraclePartitionTypeInterval,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOraclePartitionTypeRangeHash,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOraclePartitionTypeRangeList,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOraclePartitionTypeRangeRange,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOraclePartitionTypeListHash,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOraclePartitionTypeListHash,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOraclePartitionTypeListList,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOraclePartitionTypeListRange,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})

	// oracle temporary type
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleTemporaryTypeSession,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})
	buildinObjComps = append(buildinObjComps, &BuildinCompatibleRule{
		DBTypeS:       constant.DatabaseTypeOracle,
		DBTypeT:       constant.DatabaseTypeTiDB,
		ObjectNameS:   constant.BuildInOracleTemporaryTypeTransaction,
		IsCompatible:  constant.AssessNoCompatible,
		IsConvertible: constant.AssessNoConvertible,
	})

	return buildinObjComps
}

// CompatibleRuleSliceSplit used for the according to splitCounts, split slice
func CompatibleRuleSliceSplit(items []*BuildinCompatibleRule, splitCounts int) [][]*BuildinCompatibleRule {
	subArraySize := len(items) / splitCounts

	result := make([][]*BuildinCompatibleRule, 0)

	for i := 0; i < splitCounts; i++ {
		start := i * subArraySize

		end := start + subArraySize

		if i == splitCounts-1 {
			end = len(items)
		}

		subArray := items[start:end]
		if len(subArray) > 0 {
			result = append(result, subArray)
		}
	}

	return result
}
