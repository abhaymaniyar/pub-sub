package main

import (
	"encoding/json"
	"fmt"

	"github.com/jaswanth05rongali/pub-sub/client"
	"github.com/jaswanth05rongali/pub-sub/config"
	"github.com/jaswanth05rongali/pub-sub/worker"
	"go.uber.org/zap"

	"github.com/spf13/viper"
)

var consumer *worker.ConsumerObject
var logger *zap.Logger
var err error

func main() {
	rawJSON := []byte(`{
		"level": "debug",
		"encoding": "json",
		"outputPaths": ["stdout", "./logConsumer/log"],
		"errorOutputPaths": ["stderr"],
		"initialFields": {"foo": "bar"},
		"encoderConfig": {
		  "messageKey": "message",
		  "levelKey": "level",
		  "levelEncoder": "lowercase"
		}
	  }`)

	var cfg zap.Config
	if err := json.Unmarshal(rawJSON, &cfg); err != nil {
		panic(err)
	}
	logger, err = cfg.Build()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	logger.Info("logger construction succeeded")
	defer logger.Sync()
	logger.Info("failed to fetch URL")
	config.Init(false)
	broker := viper.GetString("broker")
	group := viper.GetString("group")
	topics := viper.GetString("topic")

	client := client.Object{}
	consumer = &worker.ConsumerObject{ClientInterface: client}
	logger.Error("Created consumer...")
	consumer.Init(broker, group)

	err = consumer.GetConsumer().Subscribe(topics, nil)
	if err != nil {
		fmt.Printf("Error:%v while subscribing to topic:%v", err, topics)
		// logger.Info("Error:%v while subscribing to topic:%v", zap.String(err), topics)
	}
	// logger.Info("Successfully subscribed to topic:%v", topics)
	consumer.Consume(false)
}

func GetLogger() *zap.Logger {
	return logger
}
