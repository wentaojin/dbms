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
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"
	"unsafe"

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

// StringLower used for string lower, and returns lower string
func StringLower(str string) string {
	return strings.ToLower(str)
}

// StringUpperSlice used for string slice upper, and returns upper string
func StringUpperSlice(strs []string) []string {
	var newStrs []string
	for _, s := range strs {
		newStrs = append(newStrs, StringUpper(s))
	}
	return newStrs
}

// StringSliceRemoveElement used for string slice remove element
func StringSliceRemoveElement(slice []string, elem string) []string {
	var result []string
	for _, v := range slice {
		if v != elem {
			result = append(result, v)
		}
	}
	return result
}

// StringLowerSlice used for string slice lower, and returns lower string
func StringLowerSlice(strs []string) []string {
	var newStrs []string
	for _, s := range strs {
		newStrs = append(newStrs, StringLower(s))
	}
	return newStrs
}

func StrconvIntBitSize(s string, bitSize int) (int64, error) {
	i, err := strconv.ParseInt(s, 10, bitSize)
	if err != nil {
		return i, err
	}
	return i, nil
}

func StrconvUintBitSize(s string, bitSize int) (uint64, error) {
	i, err := strconv.ParseUint(s, 10, bitSize)
	if err != nil {
		return i, err
	}
	return i, nil
}

func StrconvFloatBitSize(s string, bitSize int) (float64, error) {
	i, err := strconv.ParseFloat(s, bitSize)
	if err != nil {
		return i, err
	}
	return i, nil
}

func StrconvRune(s string) (int32, error) {
	r, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return rune(r), err
	}
	return rune(r), nil
}

func StringPairKey(res map[string]string) string {
	var newStr []string
	for k, _ := range res {
		newStr = append(newStr, k)
	}
	return StringJoin(newStr, ",")
}

func StringTrim(originStr string, trimStr string) string {
	return strings.Trim(originStr, trimStr)
}

func CompareInter(structA, structB interface{}) ([]interface{}, []interface{}) {
	var (
		addDiffs    []interface{}
		removeDiffs []interface{}
	)
	aVal := reflect.ValueOf(structA)
	bVal := reflect.ValueOf(structB)

	if !aVal.IsValid() && !bVal.IsValid() {
		return addDiffs, removeDiffs
	}

	if aVal.Kind() == reflect.Struct && bVal.Kind() == reflect.Struct {
		if reflect.DeepEqual(aVal, reflect.Zero(reflect.TypeOf(structA))) {
			removeDiffs = append(removeDiffs, structB)
		} else {
			if !reflect.DeepEqual(structA, structB) {
				addDiffs = append(addDiffs, structA)
				removeDiffs = append(removeDiffs, structB)
			}
		}
		return addDiffs, removeDiffs
	}

	if aVal.IsNil() && bVal.IsNil() {
		return addDiffs, removeDiffs
	}

	if aVal.IsNil() && !bVal.IsNil() {
		if bVal.Kind() == reflect.Slice || bVal.Kind() == reflect.Array {
			for bi := 0; bi < aVal.Len(); bi++ {
				removeDiffs = append(removeDiffs, bVal.Index(bi).Interface())
			}
		}
		return addDiffs, removeDiffs
	}

	if !aVal.IsNil() && bVal.IsNil() {
		if aVal.Kind() == reflect.Slice || aVal.Kind() == reflect.Array {
			for ai := 0; ai < aVal.Len(); ai++ {
				addDiffs = append(addDiffs, aVal.Index(ai).Interface())
			}
		}
		return addDiffs, removeDiffs
	}

	if !aVal.IsNil() && !bVal.IsNil() {
		if (aVal.Kind() == reflect.Slice && bVal.Kind() == reflect.Slice) || (aVal.Kind() == reflect.Array && bVal.Kind() == reflect.Array) {
			dictB := make(map[interface{}]bool)
			for bi := 0; bi < bVal.Len(); bi++ {
				dictB[bVal.Index(bi).Interface()] = true
			}

			dictA := make(map[interface{}]bool)
			for ai := 0; ai < aVal.Len(); ai++ {
				dictA[aVal.Index(ai).Interface()] = true
				if _, ok := dictB[aVal.Index(ai).Interface()]; !ok {
					addDiffs = append(addDiffs, aVal.Index(ai).Interface())
				}
			}

			for bi := 0; bi < bVal.Len(); bi++ {
				if _, ok := dictA[bVal.Index(bi).Interface()]; !ok {
					removeDiffs = append(removeDiffs, bVal.Index(bi).Interface())
				}
			}
		}
	}
	if len(addDiffs) == 0 && len(removeDiffs) == 0 {
		return addDiffs, removeDiffs
	}
	return addDiffs, removeDiffs
}

func StringReplacer(str, toReplace, replaceWith string) string {
	return regexp.MustCompile(toReplace).ReplaceAllString(str, replaceWith)
}

func StringMatcher(str, toMatch string) bool {
	return regexp.MustCompile(toMatch).MatchString(str)
}

func StringExtractorWithinQuotationMarks(text string, patterns ...string) []string {
	var matches []string
	for _, pattern := range patterns {
		re := regexp.MustCompile("\"" + pattern + "\"")
		founds := re.FindAllString(text, -1)
		for _, f := range founds {
			matches = append(matches, RemovePrefixSuffixOnce(f, "\"", "\""))
		}
	}
	return matches
}

func StringExtractorWithoutQuotationMarks(text string, patterns ...string) []string {
	var matches []string
	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		founds := re.FindAllString(text, -1)
		matches = append(matches, founds...)
	}
	return matches
}

func StringExtractorWithinBrackets(text string) []string {
	re := regexp.MustCompile(`\((.*?)\)`)
	matches := re.FindAllStringSubmatch(text, -1)
	var results []string
	for _, match := range matches {
		results = append(results, match[1])
	}
	return results
}

func RemovePrefixSuffixOnce(str string, prefix, suffix string) string {
	return RemovePrefixOnce(RemoveSuffixOnce(str, suffix), prefix)
}

func RemoveSuffixOnce(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		return s[:len(s)-len(suffix)]
	}
	return s
}

func RemovePrefixOnce(s, prefix string) string {
	if strings.HasPrefix(s, prefix) {
		return s[len(prefix):]
	}
	return s
}

// Key-Value
func CompareMapInter(mapSA interface{}, mapTB interface{}) (map[string]interface{}, map[string]interface{}, map[string]interface{}) {
	mapA := make(map[string]interface{})
	mapB := make(map[string]interface{})

	addMap := make(map[string]interface{})
	delMap := make(map[string]interface{})
	modifyMap := make(map[string]interface{})

	aVal := reflect.ValueOf(mapSA)
	bVal := reflect.ValueOf(mapTB)

	iterMA := aVal.MapRange()
	for iterMA.Next() {
		mapA[iterMA.Key().String()] = iterMA.Value().Interface()
	}

	iterMB := bVal.MapRange()
	for iterMB.Next() {
		mapB[iterMB.Key().String()] = iterMB.Value().Interface()
	}

	for kA, iterA := range mapA {
		if iterB, ok := mapB[kA]; !ok {
			var tmpIters []interface{}
			for _, valB := range mapB {
				// the different key but the value is the same, skip
				addInterB, delInterB := CompareInter(iterA, valB)
				if len(addInterB) == 0 && len(delInterB) == 0 {
					tmpIters = append(tmpIters, valB)
				}
			}
			// it isn't existed the same value
			if len(tmpIters) == 0 {
				addMap[kA] = iterA
			}
		} else {
			addInterB, delInterB := CompareInter(iterA, iterB)
			if len(addInterB) == 0 && len(delInterB) == 0 {
				continue
			}
			addMap[kA] = addInterB[0]
			delMap[kA] = delInterB[0]
		}
	}

	for kB, iterB := range mapB {
		if _, ok := mapA[kB]; !ok {
			var tmpIters []interface{}
			for _, valA := range mapA {
				// the different key but the value is the same, skip
				addInterA, delInterA := CompareInter(iterB, valA)
				if len(addInterA) == 0 && len(delInterA) == 0 {
					tmpIters = append(tmpIters, valA)
				}
			}
			// it isn't existed the same value
			if len(tmpIters) == 0 {
				delMap[kB] = iterB
			}
		}
	}

	if len(addMap) != 0 && len(delMap) != 0 {
		for k, _ := range delMap {
			if val, ok := addMap[k]; ok {
				delete(delMap, k)
				modifyMap[k] = val
				delete(addMap, k)
			}
		}
	}
	return addMap, delMap, modifyMap
}

func StringSliceAlignLen(slices [][]string) ([][]string, int) {
	minL := 0
	for i, slice := range slices {
		if i == 0 || len(slice) < minL {
			minL = len(slice)
		}
	}

	// align
	alignSlices := make([][]string, len(slices))
	for i, slice := range slices {
		alignSlices[i] = slice[:minL]
	}
	return alignSlices, minL
}

// StringItemsFilterDifference used for filter difference items, and returns new array string
func StringItemsFilterDifference(originItems, excludeItems []string) []string {
	s1 := set.NewStringSet()
	for _, t := range originItems {
		s1.Add(t)
	}
	s2 := set.NewStringSet()
	for _, t := range excludeItems {
		s2.Add(t)
	}
	return strset.Difference(s1, s2).List()
}

// StringItemsFilterIntersection used for filter intersection items in the two items, and return new array string
func StringItemsFilterIntersection(originItems, newItems []string) []string {
	s1 := set.NewStringSet()
	for _, t := range originItems {
		s1.Add(t)
	}
	s2 := set.NewStringSet()
	for _, t := range newItems {
		s2.Add(t)
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

// IsContainedStringIgnoreCase used for judge items whether is contained the item, and if it's contained, return true
func IsContainedStringIgnoreCase(items []string, item string) bool {
	for _, eachItem := range items {
		if strings.EqualFold(eachItem, item) {
			return true
		}
	}
	return false
}

// StringSliceSplit used for the according to splitCounts, split slice
func StringSliceSplit(items []string, splitCounts int) [][]string {
	subArraySize := len(items) / splitCounts

	result := make([][]string, 0)

	for i := 0; i < splitCounts; i++ {
		start := i * subArraySize

		end := start + subArraySize

		if i == splitCounts-1 {
			end = len(items)
		}

		subArray := items[start:end]
		if len(subArray) > 0 {
			result = append(result, subArray)
		}
	}

	return result
}

// AnySliceSplit used for the according to splitCounts, split slice
func AnySliceSplit(value interface{}, batchSize int) []interface{} {
	var result []interface{}

	reflectValue := reflect.Indirect(reflect.ValueOf(value))

	switch reflectValue.Kind() {
	case reflect.Slice, reflect.Array:
		// the reflection length judgment of the optimized value
		reflectLen := reflectValue.Len()
		for i := 0; i < reflectLen; i += batchSize {
			ends := i + batchSize
			if ends > reflectLen {
				ends = reflectLen
			}
			result = append(result, reflectValue.Slice(i, ends).Interface())
		}
	default:
		result = append(result, value)
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
	case reflect.Slice, reflect.Array:
		var strSlice []string
		for i := 0; i < value.Len(); i++ {
			strSlice = append(strSlice, valueToString(value.Index(i)))
		}
		return StringJoin(strSlice, constant.StringSeparatorComma)
	default:
		panic("Unsupported Type")
	}
}

// GetOutBoundIP used for the host ipaddr
func GetOutBoundIP() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:53")
	if err != nil {
		return "", fmt.Errorf("get host ip err: %v", err)
	}
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	ip := strings.Split(localAddr.String(), ":")[0]
	return ip, nil
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
	return BytesToString(vo)
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

// GetRandomElem used for get random element
func GetRandomElem(slice []string) (string, error) {
	if len(slice) == 0 {
		return "", fmt.Errorf("empty slice")
	}

	randomIndex, err := rand.Int(rand.Reader, big.NewInt(int64(len(slice))))
	if err != nil {
		return "", err
	}

	return slice[randomIndex.Int64()], nil
}

// CurrentTimeFormatString used for format time string
func CurrentTimeFormatString() string {
	return time.Now().Format("2006-01-02 15:04:05.000000")
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

func IsValueNil(i interface{}) bool {
	v := reflect.ValueOf(i)
	if v.Kind() == reflect.Ptr {
		return v.IsNil()
	} else if v.Kind() == reflect.Invalid {
		// null value nil
		return true
	}
	return false
}

func Min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func PaddingString(padNums int, padStr string, lastPadStr string) string {
	var str strings.Builder
	for i := 0; i < padNums-1; i++ {
		str.WriteString(padStr)
	}
	str.WriteString(lastPadStr)
	return str.String()
}

// BytesToString used for bytes to string, reduce memory
// https://segmentfault.com/a/1190000037679588
func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// PathNotExistOrCreate used for the filepath is whether exist, if not exist, then create
func PathNotExistOrCreate(path string) error {
	_, err := os.Stat(path)
	if err == nil {
		return nil
	}
	if os.IsNotExist(err) {
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return fmt.Errorf("file dir MkdirAll failed: %v", err)
		}
	}
	return err
}

// GetDiskUsage used for get the current dir disk usage size
func GetDiskUsage(path string) (*syscall.Statfs_t, error) {
	statfs := &syscall.Statfs_t{}
	if err := syscall.Statfs(path, statfs); err != nil {
		return statfs, fmt.Errorf("get dir [%v] disk usage size failed: %v", path, err)
	}
	return statfs, nil
}

// EscapeBinaryCSV used for bytes to string
func EscapeBinaryCSV(s []byte, escapeBackslash bool, delimiterCsv, separatorCsv string) string {
	switch {
	case escapeBackslash:
		return escapeBackslashCSV(s, delimiterCsv, separatorCsv)
	case len(delimiterCsv) > 0:
		delimiter := []byte(delimiterCsv)
		return string(bytes.ReplaceAll(s, delimiter, append(delimiter, delimiter...)))
	default:
		return string(s)
	}
}

func escapeBackslashCSV(s []byte, delimiterCsv, separatorCsv string) string {
	bf := bytes.Buffer{}

	var (
		escape  byte
		last         = 0
		specCmt byte = 0
	)

	delimiter := []byte(delimiterCsv)
	separator := []byte(separatorCsv)

	if len(delimiter) > 0 {
		specCmt = delimiter[0] // if csv has a delimiter, we should use backslash to comment the delimiter in field value
	} else if len(separator) > 0 {
		specCmt = separator[0] // if csv's delimiter is "", we should escape the separator to avoid error
	}

	for i := 0; i < len(s); i++ {
		escape = 0

		switch s[i] {
		case 0: /* Must be escaped for 'mysql' */
			escape = '0'
		case '\r':
			escape = 'r'
		case '\n': /* escaped for line terminators */
			escape = 'n'
		case '\\':
			escape = '\\'
		case specCmt:
			escape = specCmt
		}

		if escape != 0 {
			bf.Write(s[last:i])
			bf.WriteByte('\\')
			bf.WriteByte(escape)
			last = i + 1
		}
	}
	bf.Write(s[last:])
	return bf.String()
}

// If there are special characters, add \ directly before the special characters.
/**
Determine whether it is a letter: unicode.IsLetter(v)
Determine whether it is a decimal number: unicode.IsDigit(v)
Determine whether it is a number: unicode.IsNumber(v)
Determine whether it is a white space symbol: unicode.IsSpace(v)
Determine whether it is a special symbol: unicode.IsSymbol(v)
Determine whether it is a Unicode punctuation character: unicode.IsPunct(v)
Determine whether it is Chinese: unicode.Han(v)
*/
func SpecialLettersMySQLCompatibleDatabase(bs []byte) string {
	var b strings.Builder

	for _, r := range bytes.Runes(bs) {
		if unicode.IsPunct(r) || unicode.IsSymbol(r) || unicode.IsSpace(r) {
			// mysql/tidb % character, /% represents /%, % represents %, no need to escape
			// mysql/tidb _ character, /_ represents /_, _ represents _, no need to escape
			if r == '%' || r == '_' {
				b.WriteRune(r)
			} else {
				b.WriteRune('\\')
				b.WriteRune(r)
			}
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func SpecialLettersOracleCompatibleDatabase(bs []byte) string {
	var b strings.Builder

	for _, r := range bytes.Runes(bs) {
		if r == '\'' {
			b.WriteRune('\\')
			b.WriteRune(r)
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}
