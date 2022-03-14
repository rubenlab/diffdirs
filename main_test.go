package main

import "testing"

func TestLoadConfig(t *testing.T) {
	config, err := loadConfig("./config-test.yml")
	if err != nil {
		t.Error(err)
	}
	failed := config.Dirs["pool-ruben-goett"] != "/mnt/pool1/pool-ruben-goett"
	failed = failed || config.Dirs["pool-sakata-goett"] != "/mnt/pool1/pool-sakata-goett"
	failed = failed || !config.FirstRun || config.Db != "storage.db"
	if failed {
		t.Errorf("config value not correct: \n%v", config)
	}
}
