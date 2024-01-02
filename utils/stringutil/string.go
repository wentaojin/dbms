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
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strings"
	"unicode"

	"github.com/wentaojin/dbms/utils/constant"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
	"golang.org/x/text/transform"

	"github.com/scylladb/go-set"
	"github.com/scylladb/go-set/strset"
	"github.com/thinkeridea/go-extend/exstrings"
)

// StringBuilder used for string builder, and returns string
func StringBuilder(str ...string) string {
	var b strings.Builder
	for _, p := range str {
		b.WriteString(p)
	}
	return b.String() // no copying
}

// StringSplit used for string split, and returns array string
func StringSplit(str string, sep string) []string {
	return strings.Split(str, sep)
}

// StringJoin used for string join, and returns array string
func StringJoin(strs []string, sep string) string {
	return exstrings.Join(strs, sep)
}

// StringUpper used for string upper, and returns upper string
func StringUpper(str string) string {
	return strings.ToUpper(str)
}

// StringItemsFilterDifference used for filter difference items, and returns new array string
func StringItemsFilterDifference(originItems, excludeItems []string) []string {
	s1 := set.NewStringSet()
	for _, t := range originItems {
		s1.Add(strings.ToUpper(t))
	}
	s2 := set.NewStringSet()
	for _, t := range excludeItems {
		s2.Add(strings.ToUpper(t))
	}
	return strset.Difference(s1, s2).List()
}

// StringItemsFilterIntersection used for filter intersection items in the two items, and return new array string
func StringItemsFilterIntersection(originItems, newItems []string) []string {
	s1 := set.NewStringSet()
	for _, t := range originItems {
		s1.Add(strings.ToUpper(t))
	}
	s2 := set.NewStringSet()
	for _, t := range newItems {
		s2.Add(strings.ToUpper(t))
	}
	return strset.Intersection(s1, s2).List()
}

// IsContainedString used for judge items whether is contained the item, and if it's contained, return true
func IsContainedString(items []string, item string) bool {
	for _, eachItem := range items {
		if eachItem == item {
			return true
		}
	}
	return false
}

// StringSplitSlice used for the according to splitCounts, split slice
func StringSplitSlice(items []string, splitCounts int) [][]string {
	subArraySize := len(items) / splitCounts

	result := make([][]string, 0)

	for i := 0; i < splitCounts; i++ {
		start := i * subArraySize

		end := start + subArraySize

		if i == splitCounts-1 {
			end = len(items)
		}

		subArray := items[start:end]
		result = append(result, subArray)
	}

	return result
}

// GetJSONTagFieldValue used for get field value with json tag
func GetJSONTagFieldValue(obj interface{}) map[string]string {
	objValue := reflect.ValueOf(obj)

	fieldInfo := make(map[string]string)

	if objValue.Kind() == reflect.Ptr {
		objValue = objValue.Elem() // Dereference the pointer
	}

	objType := objValue.Type()

	for i := 0; i < objValue.NumField(); i++ {
		field := objValue.Field(i)
		fieldType := objType.Field(i)
		jsonTag := fieldType.Tag.Get("json")

		if jsonTag != "" {
			fieldValue := valueToString(field)
			fieldInfo[removeOmitempty(jsonTag)] = fieldValue
		}
	}
	return fieldInfo
}

func removeOmitempty(tag string) string {
	tagParts := strings.Split(tag, ",")
	for i, part := range tagParts {
		if part == "omitempty" {
			tagParts = append(tagParts[:i], tagParts[i+1:]...)
			break
		}
	}
	return strings.Join(tagParts, ",")
}

func valueToString(value reflect.Value) string {
	switch value.Kind() {
	case reflect.String:
		return value.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fmt.Sprintf("%d", value.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return fmt.Sprintf("%d", value.Uint())
	case reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%f", value.Float())
	case reflect.Bool:
		return fmt.Sprintf("%t", value.Bool())
	default:
		panic("Unsupported Type")
	}
}

// VersionOrdinal used for the database version comparison
func VersionOrdinal(version string) string {
	// ISO/IEC 14651:2011
	const maxByte = 1<<8 - 1
	vo := make([]byte, 0, len(version)+8)
	j := -1
	for i := 0; i < len(version); i++ {
		b := version[i]
		if '0' > b || b > '9' {
			vo = append(vo, b)
			j = -1
			continue
		}
		if j == -1 {
			vo = append(vo, 0x00)
			j = len(vo) - 1
		}
		if vo[j] == 1 && vo[j+1] == '0' {
			vo[j+1] = b
			continue
		}
		if vo[j]+1 > maxByte {
			panic("VersionOrdinal: invalid version")
		}
		vo = append(vo, b)
		vo[j]++
	}
	return string(vo)
}

// ExchangeStringDict used for exchange string dict
func ExchangeStringDict(highPriority, lowPriority map[string]string) map[string]string {
	result := make(map[string]string)

	for k, v := range lowPriority {
		result[k] = v
	}

	for k, v := range highPriority {
		result[k] = v
	}

	return result
}

// CharsetConvert used for string data charset convert
func CharsetConvert(data []byte, fromCharset, toCharset string) ([]byte, error) {
	switch {
	case strings.EqualFold(fromCharset, constant.CharsetUTF8MB4) && strings.EqualFold(toCharset, constant.CharsetGBK):
		reader := transform.NewReader(bytes.NewReader(data), encoding.ReplaceUnsupported(simplifiedchinese.GBK.NewEncoder()))
		gbkBytes, err := io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		return gbkBytes, nil

	case strings.EqualFold(fromCharset, constant.CharsetUTF8MB4) && strings.EqualFold(toCharset, constant.CharsetGB18030):
		reader := transform.NewReader(bytes.NewReader(data), encoding.ReplaceUnsupported(simplifiedchinese.GB18030.NewEncoder()))
		gbk18030Bytes, err := io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		return gbk18030Bytes, nil

	case strings.EqualFold(fromCharset, constant.CharsetUTF8MB4) && strings.EqualFold(toCharset, constant.CharsetBIG5):
		reader := transform.NewReader(bytes.NewReader(data), encoding.ReplaceUnsupported(traditionalchinese.Big5.NewEncoder()))
		bigBytes, err := io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		return bigBytes, nil

	case strings.EqualFold(fromCharset, constant.CharsetUTF8MB4) && strings.EqualFold(toCharset, constant.CharsetUTF8MB4):
		return data, nil

	case strings.EqualFold(fromCharset, constant.CharsetGBK) && strings.EqualFold(toCharset, constant.CharsetUTF8MB4):
		decoder := simplifiedchinese.GBK.NewDecoder()
		utf8Data, err := decoder.Bytes(data)
		if err != nil {
			return nil, err
		}

		return utf8Data, nil

	case strings.EqualFold(fromCharset, constant.CharsetGB18030) && strings.EqualFold(toCharset, constant.CharsetUTF8MB4):
		decoder := simplifiedchinese.GB18030.NewDecoder()
		utf8Data, err := decoder.Bytes(data)
		if err != nil {
			return nil, err
		}
		return utf8Data, nil

	case strings.EqualFold(fromCharset, constant.CharsetBIG5) && strings.EqualFold(toCharset, constant.CharsetUTF8MB4):
		decoder := traditionalchinese.Big5.NewDecoder()
		utf8Data, err := decoder.Bytes(data)
		if err != nil {
			return nil, err
		}

		return utf8Data, nil

	default:
		return nil, fmt.Errorf("string data current from charset [%v] to charset [%v] convert isn't support", fromCharset, toCharset)
	}
}

// 如果存在特殊字符，直接在特殊字符前添加 \
/**
判断是否为字母： unicode.IsLetter(v)
判断是否为十进制数字： unicode.IsDigit(v)
判断是否为数字： unicode.IsNumber(v)
判断是否为空白符号： unicode.IsSpace(v)
判断是否为特殊符号：unicode.IsSymbol(v)
判断是否为Unicode标点字符 :unicode.IsPunct(v)
判断是否为中文：unicode.Han(v)
*/
func SpecialLettersMySQLCompatibleDatabase(bs []byte) string {

	var (
		b     strings.Builder
		chars []rune
	)
	for _, r := range bytes.Runes(bs) {
		if unicode.IsPunct(r) || unicode.IsSymbol(r) || unicode.IsSpace(r) {
			// mysql/tidb % 字符, /% 代表 /%，% 代表 % ,无需转义
			// mysql/tidb _ 字符, /_ 代表 /_，_ 代表 _ ,无需转义
			if r == '%' || r == '_' {
				chars = append(chars, r)
			} else {
				chars = append(chars, '\\', r)
			}
		} else {
			chars = append(chars, r)
		}
	}

	b.WriteString(string(chars))

	return b.String()
}

func SpecialLettersOracleCompatibleDatabase(bs []byte) string {

	var (
		b     strings.Builder
		chars []rune
	)
	for _, r := range bytes.Runes(bs) {
		if r == '\'' {
			chars = append(chars, '\'', r)
		} else {
			chars = append(chars, r)
		}
	}

	b.WriteString(string(chars))

	return b.String()
}
