package main

import (
	"fmt"
	"pub-sub/worker"

	"github.com/jaswanth05rongali/pub-sub/client"
	"github.com/jaswanth05rongali/pub-sub/config"
	"go.uber.org/zap"

	"github.com/spf13/viper"
)

var consumer *worker.ConsumerObject
var err error
var logger *zap.Logger

func main() {
	config.Init(false)
	broker := viper.GetString("broker")
	group := viper.GetString("group")
	topics := viper.GetString("topic")

	client := client.Object{}
	consumer = &worker.ConsumerObject{ClientInterface: client}
	consumer.Init(broker, group)
	logger = consumer.GetLogger()
	logger.Error("Created consumer...")

	err = consumer.GetConsumer().Subscribe(topics, nil)
	if err != nil {
		fmt.Printf("Error:%v while subscribing to topic:%v", err, topics)
		// logger.Info("Error:%v while subscribing to topic:%v", zap.String(err), topics)
	}
	// logger.Info("Successfully subscribed to topic:%v", topics)
	consumer.Consume(false)
}
