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

import "context"

type IMsgTopicPartition interface {
	CreateMsgTopicPartition(ctx context.Context, data *MsgTopicPartition) (*MsgTopicPartition, error)
	ListMsgTopicPartition(ctx context.Context, page uint64, pageSize uint64) ([]*MsgTopicPartition, error)
	GetMsgTopicPartition(ctx context.Context, data *MsgTopicPartition) (*MsgTopicPartition, error)
	CountMsgTopicPartition(ctx context.Context, data *MsgTopicPartition) (int, error)
	DeleteMsgTopicPartition(ctx context.Context, taskName []string) error
	UpdateMsgTopicPartition(ctx context.Context, data *MsgTopicPartition, updates map[string]interface{}) error
}

type IMsgDdlRewrite interface {
	CreateMsgDdlRewrite(ctx context.Context, data *MsgDdlRewrite) (*MsgDdlRewrite, error)
	ListMsgDdlRewrite(ctx context.Context, page uint64, pageSize uint64) ([]*MsgDdlRewrite, error)
	QueryMsgDdlRewrite(ctx context.Context, data *MsgDdlRewrite) ([]*MsgDdlRewrite, error)
	GetMsgDdlRewrite(ctx context.Context, data *MsgDdlRewrite) (*MsgDdlRewrite, error)
	DeleteMsgDdlRewrite(ctx context.Context, taskName []string) error
}
