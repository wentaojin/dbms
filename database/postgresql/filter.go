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
package postgresql

import "github.com/wentaojin/dbms/utils/structure"

func (d *Database) FilterDatabaseTable(sourceSchema string, includeTableS, excludeTableS []string) (*structure.TableObjects, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) FilterDatabaseSequence(sourceSchema string, includeSequenceS, excludeSequenceS []string) (*structure.SequenceObjects, error) {
	//TODO implement me
	panic("implement me")
}
