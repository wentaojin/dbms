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
package message

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
)

const (
	// "" no compression
	Null string = ""

	// None no compression
	None string = "none"

	// Snappy compression
	Snappy string = "snappy"

	// LZ4 compression
	LZ4 string = "lz4"
)

var (
	lz4ReaderPool = sync.Pool{
		New: func() interface{} {
			return lz4.NewReader(nil)
		},
	}

	bufferPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
)

// Encode the given data by the given compression codec.
func Compress(cc string, data []byte) ([]byte, error) {
	switch cc {
	case None:
		return data, nil
	case Snappy:
		return snappy.Encode(nil, data), nil
	case LZ4:
		var buf bytes.Buffer
		writer := lz4.NewWriter(&buf)
		if _, err := writer.Write(data); err != nil {
			return nil, fmt.Errorf("compression failed: %v", err)
		}
		if err := writer.Close(); err != nil {
			return nil, fmt.Errorf("compression failed: %v", err)
		}
		return buf.Bytes(), nil
	default:
	}

	return nil, fmt.Errorf("unsupported compression %s", cc)
}

// Decode the given data by the given compression codec.
func Decompress(cc string, data []byte) ([]byte, error) {
	switch cc {
	case None, Null:
		return data, nil
	case Snappy:
		return snappy.Decode(nil, data)
	case LZ4:
		reader, ok := lz4ReaderPool.Get().(*lz4.Reader)
		if !ok {
			reader = lz4.NewReader(bytes.NewReader(data))
		} else {
			reader.Reset(bytes.NewReader(data))
		}
		buffer := bufferPool.Get().(*bytes.Buffer)
		_, err := buffer.ReadFrom(reader)
		// copy the buffer to a new slice with the correct length
		// reuse lz4Reader and buffer
		lz4ReaderPool.Put(reader)
		res := make([]byte, buffer.Len())
		copy(res, buffer.Bytes())
		buffer.Reset()
		bufferPool.Put(buffer)
		return res, err
	default:
		return nil, fmt.Errorf("unsupported compression %s", cc)
	}
}
