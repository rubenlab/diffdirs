package main

import (
	"bufio"
	"errors"
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
			log.Println(err)
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
		solid, err := isSolidFile(absolutePath)
		if err != nil {
			return err
		}
		if !solid {
			return nil
		}
		fi, _ := os.Stat(path)
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

func CountMismatchCount(diffResult string, config *Config) {
	totalCount := 0
	err := loopMismatchFiles(diffResult, config, func(bucket string, path string, absolutePath string) error {
		solid, err := isSolidFile(absolutePath)
		if err != nil {
			return err
		}
		if !solid {
			return nil
		}
		// fmt.Println(absolutePath)
		totalCount++
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("total count is: %d\n", totalCount)
}

func isSolidFile(path string) (bool, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	// skip simlink files
	if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
		return false, nil
	}
	if fi.IsDir() {
		return false, nil
	}
	return true, nil
}
