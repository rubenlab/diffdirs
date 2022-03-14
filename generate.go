package main

import (
	"fmt"
	"io/fs"
	"log"
	"path/filepath"

	"github.com/codingsince1985/checksum"
	"github.com/gammazero/workerpool"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

var writeCacheSize = 100

func generate(config *Config) {
	db, err := bolt.Open(config.Db, 0600, nil)
	if err != nil {
		log.Printf("can't open db %s with error: %v", config.Db, err)
		return
	}
	defer db.Close()

	createBuckets(db, config)
	checksum := config.Checksum
	if checksum {
		checkAndSave(db, config)
	} else {
		saveSize(db, config)
	}
}

func createBuckets(db *bolt.DB, config *Config) error {
	return db.Update(func(tx *bolt.Tx) error {
		for k := range config.Dirs {
			_, err := tx.CreateBucketIfNotExists([]byte(k))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func checkAndSave(db *bolt.DB, config *Config) error {
	workers := config.Workers

	checksumChanIn := make(chan Record, workers)
	checksumChanOut := make(chan Record)
	checksumChanFinish := make(chan int)

	calculateChecksum(&checksumChanIn, workers, &checksumChanOut, &checksumChanFinish)

	writeChanFinish := make(chan int)
	writeRecord(db, &checksumChanOut, &writeChanFinish)

	for bucket, rootDir := range config.Dirs {
		filepath.Walk(rootDir, func(path string, info fs.FileInfo, err error) error {
			if err != nil {
				log.Printf("loop dir error: %v", err)
				return nil
			}
			if !info.IsDir() {
				relpath, err := filepath.Rel(rootDir, path)
				if err != nil {
					log.Printf("calculate relpath error: %v", err)
					return nil
				}
				if !config.FirstRun {
					if isRecordExists(db, bucket, relpath) {
						return nil
					}
				}
				record := Record{
					Bucket:  bucket,
					Path:    relpath,
					AbsPath: path,
				}
				checksumChanIn <- record
			}
			return nil
		})
	}
	close(checksumChanIn)
	<-checksumChanFinish
	close(checksumChanOut)
	<-writeChanFinish
	return nil
}

func saveSize(db *bolt.DB, config *Config) error {
	cache := make([]Record, 0, writeCacheSize)
	writeR := func(record Record) {
		if len(cache) < writeCacheSize {
			cache = append(cache, record)
		}
		if len(cache) == writeCacheSize {
			err := batchInsert(db, cache)
			if err != nil {
				log.Printf("error when insert records, error is %v", err)
			}
			cache = make([]Record, 0, writeCacheSize)
		}
	}

	for bucket, rootDir := range config.Dirs {
		filepath.Walk(rootDir, func(path string, info fs.FileInfo, err error) error {
			if err != nil {
				log.Printf("loop dir error: %v", err)
				return nil
			}
			if !info.IsDir() {
				relpath, err := filepath.Rel(rootDir, path)
				if err != nil {
					log.Printf("calculate relpath error: %v", err)
					return nil
				}
				if !config.FirstRun {
					if isRecordExists(db, bucket, relpath) {
						return nil
					}
				}
				size := info.Size()
				record := Record{
					Bucket:   bucket,
					Path:     relpath,
					AbsPath:  path,
					Checksum: fmt.Sprintf("%d", size),
				}
				writeR(record)
			}
			return nil
		})
	}

	if len(cache) > 0 {
		err := batchInsert(db, cache)
		if err != nil {
			log.Printf("error when insert records, error is %v", err)
		}
	}
	return nil
}

func isRecordExists(db *bolt.DB, bucket string, path string) bool {
	exists := false
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return nil
		}
		data := b.Get([]byte(path))
		if data != nil {
			exists = true
		}
		return nil
	})
	return exists
}

func calculateChecksum(chanIn *chan Record, workers int, chanOut *chan Record, chanFinish *chan int) {
	go func() {
		wp := workerpool.New(workers)
		for r := range *chanIn {
			record := r
			wp.Submit(func() {
				abspath := record.AbsPath
				md5, err := checksum.MD5sum(abspath)
				if err != nil {
					log.Printf("error in calculation checksum: %v", err)
					return
				}
				record.Checksum = md5
				*chanOut <- record
			})
		}
		wp.StopWait()
		*chanFinish <- 0
	}()
}

func writeRecord(db *bolt.DB, chanIn *chan Record, chanFinish *chan int) {
	go func() {
		cache := make([]Record, 0, writeCacheSize)
		for r := range *chanIn {
			if len(cache) < writeCacheSize {
				cache = append(cache, r)
			}
			if len(cache) == writeCacheSize {
				err := batchInsert(db, cache)
				if err != nil {
					log.Printf("error when insert records, error is %v", err)
				}
				cache = make([]Record, 0, writeCacheSize)
			}
		}
		if len(cache) > 0 {
			err := batchInsert(db, cache)
			if err != nil {
				log.Printf("error when insert records, error is %v", err)
			}
		}
		*chanFinish <- 0
	}()
}

func batchInsert(db *bolt.DB, records []Record) error {
	err := db.Update(func(tx *bolt.Tx) error {
		for _, cr := range records {
			bucket := tx.Bucket([]byte(cr.Bucket))
			if bucket == nil {
				return errors.New(fmt.Sprintf("bucket %s is nil", cr.Bucket))
			}
			err := bucket.Put([]byte(cr.Path), []byte(cr.Checksum))
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}
