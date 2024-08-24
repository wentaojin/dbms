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
package stringutil

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
)

// UnwrapScheme removes http or https scheme from input.
func UnwrapScheme(s string) string {
	if strings.HasPrefix(s, "http://") {
		return s[len("http://"):]
	} else if strings.HasPrefix(s, "https://") {
		return s[len("https://"):]
	}
	return s
}

// WrapSchemesForInitialCluster acts like WrapSchemes, except input is "name=URL,...".
func WrapSchemesForInitialCluster(s string, prefix string, https bool) string {
	items := strings.Split(s, ",")
	output := make([]string, 0, len(items))
	for _, item := range items {
		kv := strings.Split(item, ":")
		output = append(output, fmt.Sprintf("%s_%s_%s=%s", prefix, strings.ReplaceAll(kv[0], ".", "-"), kv[1], WrapScheme(item, https)))
	}
	return strings.Join(output, ",")
}

// HostWhiteListForInitialCluster remove duplicate addr, returns addr.
func HostWhiteListForInitialCluster(s string) map[string]struct{} {
	items := strings.Split(s, ",")
	list := make(map[string]struct{})

	for _, item := range items {
		kv := strings.Split(item, "=")
		lis := strings.Split(UnwrapScheme(kv[1]), ":")
		if len(lis) >= 1 {
			list[lis[0]] = struct{}{}
		}
	}
	return list
}

// WrapSchemes adds http or https scheme to input if missing. input could be a comma-separated list.
func WrapSchemes(str string, https bool) []string {
	items := strings.Split(str, ",")
	output := make([]string, 0, len(items))
	for _, s := range items {
		output = append(output, WrapScheme(s, https))
	}
	return output
}

func WrapScheme(s string, https bool) string {
	if s == "" {
		return s
	}
	s = UnwrapScheme(s)
	if https {
		return "https://" + s
	}
	return "http://" + s
}

// WrapPrefixIPName returns the name with ip and prefix
func WrapPrefixIPName(localHost string, prefix string, peerAddr string) string {
	items := strings.Split(peerAddr, ",")
	for _, item := range items {
		kv := strings.Split(item, ":")
		if strings.EqualFold(kv[0], localHost) {
			return fmt.Sprintf("%s-%s", prefix, kv[1])
		}
	}
	return ""
}

// IsDirExist returns whether the directory is exists.
func IsDirExist(d string) bool {
	if stat, err := os.Stat(d); err == nil && stat.IsDir() {
		return true
	}
	return false
}

// WithHostPort returns addr with host port
func WithHostPort(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		// do nothing
		return addr
	}
	if len(host) == 0 {
		return fmt.Sprintf("127.0.0.1:%s", port)
	}
	return addr
}

// MarshalJSON returns marshal object json
func MarshalJSON(v any) (string, error) {
	jsonStr, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return BytesToString(jsonStr), nil
}

// MarshalIndentJSON returns marshal indent object json
func MarshalIndentJSON(v any) (string, error) {
	jsonStr, err := json.MarshalIndent(v, "", "    ")
	if err != nil {
		return "", err
	}
	return BytesToString(jsonStr), nil
}

// UnmarshalJSON returns marshal object json
func UnmarshalJSON(data []byte, v any) error {
	err := json.Unmarshal(data, v)
	if err != nil {
		return err
	}
	return nil
}

// FormatJSONFields returns format json string
func FormatJSONFields(data interface{}) interface{} {
	switch value := data.(type) {
	case map[string]interface{}:
		formattedMap := make(map[string]interface{})
		for key, val := range value {
			formattedMap[key] = FormatJSONFields(val)
		}
		return formattedMap
	case []map[string]interface{}:
		formattedMapArray := make([]map[string]interface{}, len(value))

		for i, item := range value {
			formattedMap := make(map[string]interface{})
			for key, val := range item {
				formattedMap[key] = FormatJSONFields(val)
			}
			formattedMapArray[i] = formattedMap
		}
	case []interface{}:
		formattedArray := make([]interface{}, len(value))
		for i, item := range value {
			formattedArray[i] = FormatJSONFields(item)
		}
		return formattedArray
	case float64:
		return value
	case string:
		var nestedData interface{}
		err := json.Unmarshal([]byte(value), &nestedData)
		if err == nil {
			return FormatJSONFields(nestedData)
		} else {
			var nestedArrayData []map[string]interface{}
			err = json.Unmarshal([]byte(value), &nestedArrayData)
			if err == nil {
				return FormatJSONFields(nestedData)
			}
		}
	}
	return data
}
