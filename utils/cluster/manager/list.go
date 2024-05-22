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
package manager

import "github.com/wentaojin/dbms/utils/cluster"

// Cluster represents a clsuter
type Cluster struct {
	Name       string `json:"name"`
	User       string `json:"user"`
	Version    string `json:"version"`
	Path       string `json:"path"`
	PrivateKey string `json:"private_key"`
}

// GetClusterList get the clusters list.
func (c *Controller) GetClusterList() ([]Cluster, error) {
	names, err := c.ListClusterNameAll()
	if err != nil {
		return nil, err
	}

	var clusters []Cluster

	for _, name := range names {
		metadata, err := cluster.ParseMetadataYaml(c.GetMetaFilePath(name))
		if err != nil {
			return nil, err
		}

		clusters = append(clusters, Cluster{
			Name:       name,
			User:       metadata.GetUser(),
			Version:    metadata.GetVersion(),
			Path:       c.Path(name),
			PrivateKey: c.Path(name, "ssh", "id_rsa"),
		})
	}

	return clusters, nil
}
