package main

import (
	"io/ioutil"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Db       string
	Dirs     map[string]string
	FirstRun bool `yaml:"first-run"`
	Workers  int
	Checksum bool
	Logsize  int64
}

func defaultConfig() *Config {
	return &Config{
		"", nil, true, 2, true, 10,
	}
}

func loadConfig(path string) (*Config, error) {
	config := defaultConfig()
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Wrap(err, "can not open config file")
	}
	err = yaml.Unmarshal(data, config)
	if err != nil {
		return nil, errors.Wrap(err, "can not unmarshal config data")
	}
	return config, nil
}
