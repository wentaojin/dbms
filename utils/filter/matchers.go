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
package filter

import (
	"fmt"
	"regexp"
)

// table matcher rule
// filter match success is positive
// filter match failed is negative
type tableRule struct {
	table matcher
}

// matcher table filter interface
type matcher interface {
	matchString(name string) bool
}

type stringMatcher string

func (m stringMatcher) matchString(name string) bool {
	// case field rule
	return string(m) == name
}

// trueMatcher match all the `*` pattern
type trueMatcher struct{}

func (trueMatcher) matchString(string) bool {
	return true
}

// regexpMatcher base regexp expression matcher
type regexpMatcher struct {
	pattern *regexp.Regexp
}

func newRegexpMatcher(pat string) (matcher, error) {
	if pat == "(?i)(^|([\\s\\t\\n]+))(.*$)" {
		// special case for '*'
		return trueMatcher{}, nil
	}

	pattern, err := regexp.Compile(pat)
	if err != nil {
		return nil, fmt.Errorf("newRegexpMatcher regexp compile failed: %v", err)
	}

	return regexpMatcher{pattern: pattern}, nil
}

func (m regexpMatcher) matchString(name string) bool {
	return m.pattern.MatchString(name)
}
