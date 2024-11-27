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
package thread

import (
	"fmt"
	"time"
)

const (
	DefaultThreadErrorMaxRetries = 3
	DefaultThreadErrorRereyDelay = 300 * time.Millisecond
)

type RetryConfig struct {
	MaxRetries int           // Maximum number of retries
	Delay      time.Duration // The delay between each retry
}

// ShouldRetry determines whether a retry is needed
/*
	shouldRetry := func(err error) bool {
		return errors.Is(err, ErrTemporaryFailure)
	}
*/
type ShouldRetry func(error) bool

func Retry(config *RetryConfig, shouldRetry ShouldRetry, fn func() error) (err error) {
	var attempts int
	for {
		err = fn()
		if err == nil || !shouldRetry(err) || attempts >= config.MaxRetries {
			break
		}
		attempts++
		fmt.Printf("Attempt %d failed, retrying in %v...\n", attempts, config.Delay)
		time.Sleep(config.Delay)
	}

	return

}
