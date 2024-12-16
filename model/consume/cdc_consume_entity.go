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
package consume

import "github.com/wentaojin/dbms/model/common"

type MsgTopicPartition struct {
	ID         uint64 `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName   string `gorm:"type:varchar(30);not null;uniqueIndex:uniq_task_topic_complex;comment:task name" json:"taskName"`
	Topic      string `gorm:"type:varchar(100);not null;uniqueIndex:uniq_task_topic_complex;comment:subscribe topic name" json:"topic"`
	Partitions int    `gorm:"type:bigint;not null;uniqueIndex:uniq_task_topic_complex;comment:subscribe topic partiton number" json:"partitions"`
	Replicas   int    `gorm:"type:int;not null;comment:subscribe topic partiton replicas" json:"replicas"`
	Checkpoint uint64 `gorm:"type:bigint;not null;default 0;comment:subscribe topic partiton consume checkpoint" json:"checkpoint"`
	Offset     int64  `gorm:"type:bigint;not null;default 0;comment:subscribe topic partiton consume message offset" json:"offset"`
	*common.Entity
}

type MsgDdlRewrite struct {
	ID             uint64 `gorm:"primary_key;autoIncrement;comment:id" json:"id"`
	TaskName       string `gorm:"type:varchar(30);not null;uniqueIndex:uniq_task_topic_complex;comment:task name" json:"taskName"`
	Topic          string `gorm:"type:varchar(100);not null;uniqueIndex:uniq_task_topic_complex;comment:subscribe topic name" json:"topic"`
	DdlDigest      string `gorm:"type:varchar(300);not null;uniqueIndex:uniq_task_topic_complex;comment:subscribe topic ddl digest" json:"ddlDigest"`
	OriginDdlText  string `gorm:"type:longtext;not null;comment:subscribe topic origin ddl text" json:"originDdlText"`
	RewriteDdlText string `gorm:"type:longtext;not null;comment:subscribe topic rewrite ddl text" json:"rewriteDdlText"`
	*common.Entity
}
