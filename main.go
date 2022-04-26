package main

import (
	"flag"
	"log"

	"github.com/sevlyar/go-daemon"
)

type Record struct {
	Bucket   string
	Path     string // file path relative to Bucket root dir
	AbsPath  string // absolute file path on local disk
	Checksum string
}

var (
	asDaemon = flag.Bool("d", false, "run as daemon")
)

func main() {
	flag.Parse()
	command := flag.Arg(0)

	config, err := loadConfig("./config.yml")
	if err != nil {
		log.Printf("can't load config file with error: %v", err)
		return
	}

	if *asDaemon {
		cntxt := &daemon.Context{
			PidFileName: "diffdir.pid",
			PidFilePerm: 0644,
			LogFileName: "diffdir.log",
			LogFilePerm: 0640,
			WorkDir:     "./",
			Umask:       027,
		}

		d, err := cntxt.Reborn()
		if err != nil {
			log.Fatal("Unable to run: ", err)
		}
		if d != nil {
			return
		}
		defer cntxt.Release()

		log.Print("- - - - - - - - - - - - - - -")
	}

	if command == "diff" {
		diffFrom2 := flag.Arg(1)
		if diffFrom2 == "" {
			log.Fatalf("please input a filepath to diff, for example, diffdirs diff source.db")
			return
		}
		diff(config, diffFrom2)
	} else if command == "missize" {
		diffResult := flag.Arg(1)
		if diffResult == "" {
			log.Fatalf("please input the filepath to diff result, for example, diffdirs missize diffresult.csv")
			return
		}
		CountMismatchSize(diffResult, config)
	} else if command == "" {
		generate(config)
	} else {
		log.Fatalf("unknown command %s", command)
	}

	log.Print("program finished")
}
