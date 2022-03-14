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
	diffFrom = flag.String("diff", "", "diff with this db")
	asDaemon = flag.Bool("d", false, "run as daemon")
)

func main() {
	flag.Parse()

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
		log.Print("daemon started")
	} else {
		log.Print("program started")
	}

	if *diffFrom == "" {
		generate(config)
	} else {
		diff(config, *diffFrom)
	}
	log.Print("program finished")
}
