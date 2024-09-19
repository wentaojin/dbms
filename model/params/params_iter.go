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
package params

import "context"

type IParams interface {
	CreateTaskDefaultParam(ctx context.Context, data *TaskDefaultParam) (*TaskDefaultParam, error)
	UpdateTaskDefaultParam(ctx context.Context, data *TaskDefaultParam) (*TaskDefaultParam, error)
	ListTaskDefaultParam(ctx context.Context, page uint64, pageSize uint64) ([]*TaskDefaultParam, error)
	QueryTaskDefaultParam(ctx context.Context, data *TaskDefaultParam) ([]*TaskDefaultParam, error)
	DeleteTaskDefaultParam(ctx context.Context, ids []uint64) error

	CreateTaskCustomParam(ctx context.Context, data *TaskCustomParam) (*TaskCustomParam, error)
	UpdateTaskCustomParam(ctx context.Context, data *TaskCustomParam) (*TaskCustomParam, error)
	ListTaskCustomParam(ctx context.Context, page uint64, pageSize uint64) ([]*TaskCustomParam, error)
	QueryTaskCustomParam(ctx context.Context, data *TaskCustomParam) ([]*TaskCustomParam, error)
	GetTaskCustomParam(ctx context.Context, data *TaskCustomParam) (*TaskCustomParam, error)
	DeleteTaskCustomParam(ctx context.Context, taskName []string) error
}
