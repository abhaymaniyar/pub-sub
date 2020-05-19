package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/jaswanth05rongali/pub-sub/config"
	"github.com/jaswanth05rongali/pub-sub/producer"

	"github.com/gin-gonic/gin"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var logger = log.With().Str("pkg", "main").Logger()

var (
	listenAddrAPI       string
	kafkaBrokerURL      string
	kafkaTopic          string
	kafkaPubMessageType string
	pubPartition        string
)

func main() {

	config.Init(true)

	listenAddrAPI = viper.GetString("listenAddrAPI")
	kafkaBrokerURL = viper.GetString("kafkaBrokerURL")
	kafkaTopic = viper.GetString("kafkaTopic")
	kafkaPubMessageType = viper.GetString("kafkaPubMessageType")
	pubPartition = viper.GetString("pubPartition")

	producer.Init(kafkaBrokerURL)
	defer producer.P.Close()

	errChan := make(chan error, 1)

	go func() {
		log.Info().Msgf("starting server at %s", listenAddrAPI)
		errChan <- server(listenAddrAPI)
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	select {
	case <-signalChan:
		logger.Info().Msg("got an interrupt, exiting...")
	case err := <-errChan:
		if err != nil {
			logger.Error().Err(err).Msg("error while running api, exiting...")
		}
	}
}

func server(listenAddr string) error {
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.POST("/api/v1/data", postDataToKafka)

	for _, routeInfo := range router.Routes() {
		logger.Debug().
			Str("path", routeInfo.Path).
			Str("handler", routeInfo.Handler).
			Str("method", routeInfo.Method).
			Msg("registered routes")
	}

	return router.Run(listenAddr)
}

func postDataToKafka(ctx *gin.Context) {
	parent := context.Background()
	defer parent.Done()

	form := &struct {
		Requestid     string `json:"request_id"`
		Topicname     string `json:"topic_name"`
		Messagebody   string `json:"message_body"`
		Transactionid string `json:"transaction_id"`
		Email         string `json:"email"`
		Phone         string `json:"phone"`
		Customerid    string `json:"customer_id"`
		Key           string `json:"key"`
	}{}

	ctx.Bind(form)
	formInBytes, err := json.Marshal(form)
	if err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"error": map[string]interface{}{
				"message": fmt.Sprintf("error while marshalling json: %s", err.Error()),
			},
		})

		ctx.Abort()
		return
	}

	deliveryChan := make(chan kafka.Event)

	value := string(formInBytes)
	kafkaTopic = form.Topicname
	var message kafka.Message
	if kafkaPubMessageType == "0" {
		message = kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
			Value:          []byte(value),
			Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
		}
	} else if kafkaPubMessageType == "1" {
		key := form.Key
		message = kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
			Key:            []byte(key),
			Value:          []byte(value),
			Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
		}
	} else {
		par, er := strconv.Atoi(pubPartition)
		part := int32(par)
		if er != nil {
			logger.Error().Err(err).Msg("error while converting partitionToPublish to int, exiting...")
		}
		message = kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: part},
			Value:          []byte(value),
			Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
		}
	}

	err = producer.P.Produce(&message, deliveryChan)

	if err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"error": map[string]interface{}{
				"message": fmt.Sprintf("error while push message into kafka: %s", err.Error()),
			},
		})

		ctx.Abort()
		return
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		ctx.JSON(http.StatusOK, map[string]interface{}{
			"success": false,
			"message": "Message push failed",
		})
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)

		ctx.JSON(http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "success push data into kafka",
			"data":    form,
		})
	}

	close(deliveryChan)
}
