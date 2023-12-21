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
package main

import "fmt"

func cleanup() error {
	return fmt.Errorf("error from cleanup")
}
func doAnotherThing() error {
	return fmt.Errorf("error from another thing")
}

func getMessage() (msg string, errs []error) {
	defer func() {
		if tempErr := cleanup(); tempErr != nil {
			errs = append(errs, tempErr)
		}
	}()

	if tempErr := doAnotherThing(); tempErr != nil {
		errs = append(errs, tempErr)
		return "", errs
	}

	return "hello world", errs
}

func main() {
	message, errs := getMessage()
	if errs != nil {
		fmt.Printf("There are %d error(s)\n", len(errs))
		for _, err := range errs {
			fmt.Printf("Error: %v\n", err)
		}
	} else {
		fmt.Printf("Success. Message: '%s'\n", message)
	}
}
