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
package datasource

import "context"

type IDatasource interface {
	CreateDatasource(ctx context.Context, datasource *Datasource) (*Datasource, error)
	UpdateDatasource(ctx context.Context, datasource *Datasource) (*Datasource, error)
	ListDatasource(ctx context.Context, page uint64, pageSize uint64) ([]*Datasource, error)
	GetDatasource(ctx context.Context, datasourceName string) (*Datasource, error)
	DeleteDatasource(ctx context.Context, datasourceNames []string) error
}
