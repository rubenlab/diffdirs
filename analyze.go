package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func loopMismatchFiles(diffResult string, config *Config, walker func(bucket string, path string, absolutePath string) error) error {
	file, err := os.Open(diffResult)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	for scanner.Scan() {
		text := scanner.Text()
		arr := strings.Split(text, ",")
		bucket := arr[0]
		path := arr[1]
		bucketPath := config.Dirs[bucket]
		absolutePath := filepath.Join(bucketPath, path)
		err := walker(bucket, path, absolutePath)
		if err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func CountMismatchSize(diffResult string, config *Config) {
	var totalSize int64
	err := loopMismatchFiles(diffResult, config, func(bucket string, path string, absolutePath string) error {
		fi, err := os.Stat("/path/to/file")
		if err != nil {
			return err
		}
		// skip simlink files
		if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
			return nil
		}
		size := fi.Size()
		totalSize += size
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	totalGB := totalSize / gb
	fmt.Printf("total size is: %dGB", totalGB)
}
