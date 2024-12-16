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

import (
	"fmt"
	"testing"
)

func TestResolve(t *testing.T) {
	tests := []struct {
		name      string
		events    []*RowChangedEvent
		resolveTs uint64
		expected  []*RowChangedEvent
	}{
		{
			name:      "empty slice",
			events:    []*RowChangedEvent{},
			resolveTs: 10,
			expected:  []*RowChangedEvent{},
		},
		{
			name: "all events have CommitTs less than or equal to Ts",
			events: []*RowChangedEvent{
				{CommitTs: 5},
				{CommitTs: 8},
				{CommitTs: 10},
			},
			resolveTs: 10,
			expected: []*RowChangedEvent{
				{CommitTs: 5},
				{CommitTs: 8},
				{CommitTs: 10},
			},
		},
		{
			name: "some events have CommitTs less than or equal to Ts",
			events: []*RowChangedEvent{
				{CommitTs: 5},
				{CommitTs: 15},
				{CommitTs: 10},
				{CommitTs: 20},
			},
			resolveTs: 10,
			expected: []*RowChangedEvent{
				{CommitTs: 5},
				{CommitTs: 10},
			},
		},
		{
			name: "unordered CommitTs",
			events: []*RowChangedEvent{
				{CommitTs: 10},
				{CommitTs: 5},
				{CommitTs: 15},
				{CommitTs: 7},
				{CommitTs: 20},
			},
			resolveTs: 10,
			expected: []*RowChangedEvent{
				{CommitTs: 5},
				{CommitTs: 7},
				{CommitTs: 10},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			group := NewEventGroup()
			for _, e := range tt.events {
				group.Append(e)
			}

			// origin events
			fmt.Println(len(group.events))

			result := group.ResolvedTs(tt.resolveTs)
			if len(result) != len(tt.expected) {
				t.Errorf("after ResolveLE with Ts=%d, expected %d events, but got %d", tt.resolveTs, len(tt.expected), len(result))
			}

			// Check if all elements in result have CommitTs <= resolveTs
			for _, e := range result {
				if e.CommitTs > tt.resolveTs {
					t.Errorf("event with CommitTs=%d should not be included", e.CommitTs)
				}
			}

			// Optionally check if remaining events have CommitTs > resolveTs
			for _, e := range group.events {
				if e.CommitTs <= tt.resolveTs {
					t.Errorf("event with CommitTs=%d should have been resolved", e.CommitTs)
				}
			}
			// remaining events
			fmt.Println(len(group.events))
		})
	}
}
