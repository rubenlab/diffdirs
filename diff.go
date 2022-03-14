package main

import (
	"bufio"
	"fmt"
	"log"
	"os"

	bolt "go.etcd.io/bbolt"
)

func diff(config *Config, sourceDb string) {
	sdb, err := bolt.Open(sourceDb, 0600, nil)
	if err != nil {
		log.Printf("can't open db %s with error: %v", sourceDb, err)
		return
	}
	defer sdb.Close()

	db, err := bolt.Open(config.Db, 0600, nil)
	if err != nil {
		log.Printf("can't open db %s with error: %v", config.Db, err)
		return
	}
	defer db.Close()

	defer closeDiffLogWriter()
	sdb.View(func(tx *bolt.Tx) error {
		db.View(func(tx2 *bolt.Tx) error {
			for bucket := range config.Dirs {
				sbucket := tx.Bucket([]byte(bucket))
				if sbucket == nil {
					continue
				}
				tbucket := tx2.Bucket([]byte(bucket))
				if tbucket == nil {
					fmt.Printf("bucket %s bucket is missing\n", bucket)
				}
				sbucket.ForEach(func(k, v []byte) error {
					vt := tbucket.Get(k)
					if vt == nil {
						writeDiffLog(bucket, string(k), "missing")
					} else if string(vt) != string(v) {
						writeDiffLog(bucket, string(k), "checksum wrong")
					}
					return nil
				})
			}
			return nil
		})
		return nil
	})
}

var diffLogFile *os.File
var diffLogWriter *bufio.Writer

func getDiffLogWriter() *bufio.Writer {
	if diffLogWriter != nil {
		return diffLogWriter
	} else {
		f, err := os.Create("diffresult.csv")
		if err != nil {
			panic(err)
		}
		diffLogFile = f
		diffLogWriter = bufio.NewWriter(f)
		return diffLogWriter
	}
}

func closeDiffLogWriter() {
	if diffLogWriter != nil {
		diffLogWriter.Flush()
		diffLogFile.Close()
	}
}

func writeDiffLog(bucket string, path string, diff string) {
	line := fmt.Sprintf("%s,%s,%s\n", bucket, path, diff)
	writer := getDiffLogWriter()
	writer.WriteString(line)
	writer.Flush()
}
