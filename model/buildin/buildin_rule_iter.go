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

import "context"

type IBuildInRuleRecord interface {
	CreateBuildInRuleRecord(ctx context.Context, rec *BuildinRuleRecord) (*BuildinRuleRecord, error)
	GetBuildInRuleRecord(ctx context.Context, ruleName string) (*BuildinRuleRecord, error)
}

type IBuildInDatatypeRule interface {
	CreateBuildInDatatypeRule(ctx context.Context, rec []*BuildinDatatypeRule) ([]*BuildinDatatypeRule, error)
	QueryBuildInDatatypeRule(ctx context.Context, dbTypeS, dbTypeT string) ([]*BuildinDatatypeRule, error)
}

type IBuildInDefaultValueRule interface {
	CreateBuildInDefaultValueRule(ctx context.Context, rec []*BuildinDefaultvalRule) ([]*BuildinDefaultvalRule, error)
	QueryBuildInDefaultValueRule(ctx context.Context, dbTypeS, dbTypeT string) ([]*BuildinDefaultvalRule, error)
}

type IBuildInCompatibleRule interface {
	CreateBuildInCompatibleRule(ctx context.Context, rec []*BuildinCompatibleRule) ([]*BuildinCompatibleRule, error)
	QueryBuildInCompatibleRule(ctx context.Context, dbTypeS, dbTypeT string) ([]*BuildinCompatibleRule, error)
}
