package factory

import (
	"strconv"

	"github.com/rs/zerolog/log"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var logger = log.With().Str("pkg", "main").Logger()

//GetMessage  returns the message format according to publishing type
func GetMessage(kafkaPubMessageType string, key string, pubPartition string, kafkaTopic string, value string) kafka.Message {
	var message kafka.Message
	switch kafkaPubMessageType {
	case "0":
		message = kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
			Value:          []byte(value),
			Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
		}

	case "1":
		key := key
		message = kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
			Key:            []byte(key),
			Value:          []byte(value),
			Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
		}

	default:
		par, er := strconv.Atoi(pubPartition)
		part := int32(par)
		if er != nil {
			logger.Error().Err(er).Msg("error while converting partitionToPublish to int, exiting...")
		}
		message = kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: part},
			Value:          []byte(value),
			Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
		}

	}
	return message
}
