package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/spf13/viper"
)

//Init is a config intializer function
func Init(apiCall bool) {
	var bodyBytes []byte
	var err error
	var jsonPath string
	if apiCall {
		jsonPath, _ = filepath.Abs("config/config.json")
	} else {
		jsonPath, _ = filepath.Abs("../config/config.json")
	}
	bodyBytes, err = ioutil.ReadFile(jsonPath)
	if err != nil {
		fmt.Println("Couldn't read local configuration json file.", err)
	}

	parseConfiguration(bodyBytes)
}

func parseConfiguration(body []byte) {
	var localConfig config
	err := json.Unmarshal(body, &localConfig)
	if err != nil {
		fmt.Println("Cannot parse configuration from json file, message: " + err.Error())
	}
	for key, value := range localConfig.ProducerAPI[0].Source {
		viper.Set(key, value)
	}
	for key, value := range localConfig.Consumer[0].Source {
		viper.Set(key, value)
	}
}

type config struct {
	ProducerAPI []producerAPI `json:"producerAPI"`
	Consumer    []consumer    `json:"consumer"`
}

type producerAPI struct {
	Source map[string]interface{} `json:"source"`
}

type consumer struct {
	Source map[string]interface{} `json:"source"`
}
