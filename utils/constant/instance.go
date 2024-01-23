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
	DefaultMasterCrontabExpressPrefixKey = "/dbms-master/crontab/express"
	DefaultMasterCrontabEntryPrefixKey   = "/dbms-master/crontab/entry"
)

// Worker
const (
	DefaultWorkerRegisterPrefixKey     = "/dbms-worker/register/"
	DefaultWorkerStatePrefixKey        = "/dbms-worker/state/"
	DefaultWorkerBoundState            = "BOUND"
	DefaultWorkerFreeState             = "FREE"
	DefaultWorkerStoppedState          = "STOPPED"
	DefaultWorkerServerDialTimeout     = 5 * time.Second
	DefaultWorkerServerBackoffMaxDelay = 5 * time.Second
)

// Instance
const (
	DefaultInstanceRoleMaster = "MASTER"
	DefaultInstanceRoleWorker = "WORKER"

	DefaultInstanceServiceRetryCounts   = 5
	DefaultInstanceServiceRetryInterval = 5 * time.Second
)
