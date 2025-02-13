package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"strings"

	"golang.org/x/sync/errgroup"
)

// CRC32Differ is a struct that encapsulates the logic for computing and comparing CRC32 checksums.
type CRC32Differ struct {
	upstream   []string
	downstream []string
	results    chan [2]string
	errC       chan error
}

// NewCRC32Differ creates a new CRC32Differ.
func NewCRC32Differ(upstream, downstream []string) *CRC32Differ {
	return &CRC32Differ{
		upstream:   upstream,
		downstream: downstream,
		results:    make(chan [2]string, len(upstream)),
		errC:       make(chan error, 1),
	}
}

// computeCRC32 computes the CRC32 checksum of a string.
func (d *CRC32Differ) computeCRC32(s string) string {
	crc32q := crc32.MakeTable(0xD5828281)
	checksum := crc32.Checksum([]byte(s), crc32q)
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, checksum)
	return hex.EncodeToString(bytes)
}

// findDifferences recursively finds differences in CRC32 checksums between two string slices.
func (d *CRC32Differ) findDifferences(ctx context.Context, upstream, downstream []string, startIndex int) error {
	if len(upstream) == 0 && len(downstream) == 0 {
		return nil
	} else if len(upstream) != len(downstream) {
		return fmt.Errorf("length mismatch between upstream [%d] and downstream [%d] at index %d", len(upstream), len(downstream), startIndex)
	}

	upstreamCRC := d.computeCRC32(strings.Join(upstream, ","))
	downstreamCRC := d.computeCRC32(strings.Join(downstream, ","))
	fmt.Println(upstream, upstreamCRC, downstream, downstreamCRC)

	if upstreamCRC != downstreamCRC {
		if len(upstream) == 1 && len(downstream) == 1 {
			d.results <- [2]string{upstream[0], downstream[0]}
			return nil
		}

		mid := len(upstream) / 2

		errG, errCtx := errgroup.WithContext(ctx)
		errG.SetLimit(2)
		errG.Go(func() error {
			if err := d.findDifferences(errCtx, upstream[:mid], downstream[:mid], startIndex); err != nil {
				return err
			}
			return nil
		})

		errG.Go(func() error {
			if err := d.findDifferences(errCtx, upstream[mid:], downstream[mid:], startIndex+mid); err != nil {
				return err
			}
			return nil
		})

		return errG.Wait()
	}
	return nil
}

// CollectDifferences collects the CRC32 differences between the upstream and downstream slices.
func (d *CRC32Differ) CollectDifferences(ctx context.Context) ([][2]string, error) {
	var results [][2]string

	errG, errCtx := errgroup.WithContext(ctx)
	errG.Go(func() error {
		defer close(d.results) // Ensure the results channel is closed when the function returns
		if err := d.findDifferences(errCtx, d.upstream, d.downstream, 0); err != nil {
			return err
		}
		return nil
	})

	errG.Go(func() error {
		for diff := range d.results {
			results = append(results, diff)
		}
		return nil
	})

	if err := errG.Wait(); err != nil {
		return results, err
	}

	return results, nil
}

func main() {
	upstream := []string{"hello", "foo", "world", "sd", "jk"}
	downstream := []string{"hello", "foo", "world", "fgg", "kl"}

	differ := NewCRC32Differ(upstream, downstream)

	differences, err := differ.CollectDifferences(context.TODO())
	if err != nil {
		fmt.Printf("Error collecting differences: %v\n", err)
		return
	}

	if len(differences) > 0 {
		fmt.Println("Found differences:")
		for _, diff := range differences {
			fmt.Printf("Upstream: %s, Downstream: %s\n", diff[0], diff[1])
		}
	} else {
		fmt.Println("No differences found")
	}
}
