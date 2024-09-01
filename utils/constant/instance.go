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
package constant

import "time"

// Master
const (
	// DefaultMasterDatabaseDBMSKey is used for saving dbms meta database infos
	DefaultMasterDatabaseDBMSKey         = "/dbms-master/database"
	DefaultMasterCrontabExpressPrefixKey = "/dbms-master/crontab/express/"
)

// Worker
const (
	DefaultWorkerServerDialTimeout     = 5 * time.Second
	DefaultWorkerServerBackoffMaxDelay = 5 * time.Second
)

// Instance
const (
	DefaultInstanceTaskReferencesPrefixKey  = "/dbms-instance/reference/"
	DefaultInstanceServiceRegisterPrefixKey = "/dbms-instance/register/"
	DefaultInstanceBoundState               = "BOUND"
	DefaultInstanceFreeState                = "FREE"
	DefaultInstanceStoppedState             = "STOPPED"
	DefaultInstanceFailedState              = "FAILED"
)

// Instance
const (
	DefaultInstanceRoleMaster = "MASTER"
	DefaultInstanceRoleWorker = "WORKER"

	DefaultInstanceServiceRetryCounts   = 1440
	DefaultInstanceServiceRetryInterval = 5 * time.Second
)
