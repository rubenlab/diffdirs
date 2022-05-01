package main

import (
	"errors"
	"fmt"
	"log"
	"os/exec"
	"strings"

	"github.com/gammazero/workerpool"
	bolt "go.etcd.io/bbolt"
)

type TransferTask struct {
	Bucket  string
	Path    string // file path relative to Bucket root dir
	AbsPath string // absolute file path on local disk
}

// diffResult is path to the diff result file, for example, diffresult.csv.
// ct means continue.
func Compensate(config *Config, diffResult string, compensation string, ct bool) {
	db, err := bolt.Open("compensation.db", 0600, nil)
	if err != nil {
		log.Printf("can't open db %s with error: %v", config.Db, err)
		return
	}
	defer db.Close()
	if !ct {
		err = generateTransferTasks(config, diffResult, db)
		if err != nil {
			log.Fatal(err)
		}
	}
	startCompensate(db, compensation, config.Workers)
	fmt.Println("compensate complete")
}

func startCompensate(db *bolt.DB, compensation string, workers int) {
	cacheSize := 10000
	fmt.Println("start compensate")
	for {
		taskList := make([]TransferTask, 0, cacheSize)
		db.View(func(tx *bolt.Tx) error {
			bc := tx.Cursor()
			for b, _ := bc.First(); b != nil; b, _ = bc.Next() {
				bucketName := string(b)
				bucket := tx.Bucket(b)
				c := bucket.Cursor()
				for k, v := c.First(); k != nil; k, v = c.Next() {
					path := string(k)
					if bucketName == "" || path == "" {
						continue
					}
					task := TransferTask{
						Bucket:  bucketName,
						Path:    path,
						AbsPath: string(v),
					}
					taskList = append(taskList, task)
					if len(taskList) >= cacheSize {
						return nil
					}
				}
			}
			return nil
		})
		if len(taskList) == 0 {
			break
		}
		executeTasks(taskList, compensation, workers)
		db.Update(func(tx *bolt.Tx) error {
			for _, task := range taskList {
				bucket := task.Bucket
				path := task.Path
				b := tx.Bucket([]byte(bucket))
				if b == nil {
					continue
				}
				b.Delete([]byte(path))
			}
			return nil
		})
	}
}

func executeTasks(tasks []TransferTask, compensation string, workers int) {
	chanIn := make(chan TransferTask, workers)
	chanFinish := make(chan int)
	go func(chanIn *chan TransferTask, chanFinish *chan int) {
		wp := workerpool.New(workers)
		for r := range *chanIn {
			task := r
			wp.Submit(func() {
				bucket := task.Bucket
				path := task.Path
				abspath := task.AbsPath
				command := strings.Replace(compensation, "{BUCKET}", bucket, 1)
				command = strings.Replace(command, "{PATH}", path, 1)
				command = strings.Replace(command, "{ABSPATH}", abspath, 1)
				arr := strings.Fields(command)
				cmd := exec.Command(arr[0], arr[1:]...)
				err := cmd.Run()
				if err != nil {
					log.Printf("error executing command %s, error is : %v", command, err)
				}
			})
		}
		wp.StopWait()
		*chanFinish <- 0
	}(&chanIn, &chanFinish)
	for _, task := range tasks {
		chanIn <- task
	}
	close(chanIn)
	<-chanFinish
}

func generateTransferTasks(config *Config, diffResult string, db *bolt.DB) error {
	fmt.Println("start generate tasks")
	taskCache := make([]TransferTask, 0, writeCacheSize)
	loopMismatchFiles(diffResult, config, func(bucket string, path string, absolutePath string) error {
		solid, err := isSolidFile(absolutePath)
		if err != nil {
			return err
		}
		if !solid {
			return nil
		}
		task := TransferTask{
			Bucket:  bucket,
			Path:    path,
			AbsPath: absolutePath,
		}
		if len(taskCache) < writeCacheSize {
			taskCache = append(taskCache, task)
		}
		if len(taskCache) == writeCacheSize {
			err := batchInsertTasks(db, taskCache)
			if err != nil {
				return err
			}
			taskCache = make([]TransferTask, 0, writeCacheSize)
		}
		return nil
	})
	if len(taskCache) > 0 {
		err := batchInsertTasks(db, taskCache)
		if err != nil {
			return err
		}
	}
	return nil
}

func batchInsertTasks(db *bolt.DB, tasks []TransferTask) error {
	var err error
	err = db.Update(func(tx *bolt.Tx) error {
		for _, task := range tasks {
			bucket, err := tx.CreateBucketIfNotExists([]byte(task.Bucket))
			if err != nil {
				return errors.New(fmt.Sprintf("error in creating bucket %s: %v", task.Bucket, err))
			}
			err = bucket.Put([]byte(task.Path), []byte(task.AbsPath))
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func main2() {
	command := "scp -i /home/ytm/apps/ytm_rsa   /home/ytm/install/antlr/antlr-4.7.2-complete.jar yi1@transfer-mdc.hpc.gwdg.de:/scratch1/projects/rubsak/owner/yi1/test/antlr-4.7.2-complete.jar"
	arr := strings.Fields(command)
	cmd := exec.Command(arr[0], arr[1:]...)
	output, err := cmd.Output()
	if err != nil {
		fmt.Printf("error: %v\n", err)
	} else {
		fmt.Printf("finished, output is: %s\n", string(output))
	}
}
