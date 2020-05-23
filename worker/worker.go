package worker

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"pub-sub/client"
	"syscall"

	"go.uber.org/zap"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// C stores the created producer instance
var C *kafka.Consumer
var logger *zap.Logger

//ConsumerObject defines a struct for entire consumer along with few methodsC
type ConsumerObject struct {
	ClientInterface client.Interface
}

//Init will initialize the consumer function
func (cons *ConsumerObject) Init(broker string, group string) {
	logger = createLogger()
	logger.Info("Inside worker >>>>>>>> ")
	var err error
	C, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     broker,
		"broker.address.family": "v4",
		"group.id":              group,
		"session.timeout.ms":    6000,
		"enable.auto.commit":    false,
		"auto.offset.reset":     "latest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", C)
	logger.Info("Hello world >>>>>>>>>> ")
}

//GetConsumer returns the consumer variable
func (cons *ConsumerObject) GetConsumer() *kafka.Consumer {
	return C
}

//Consume will help consuming messages from the cluster and also in sending them to the clients
func (cons *ConsumerObject) Consume(testCall bool) {

	cons.ClientInterface.Init()
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	iterations := 0
	run := true
	for run {
		if testCall && iterations == 10 {
			break
		}
		iterations++
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := C.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				message := string(e.Value)
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, message)
				sentStatus := cons.ClientInterface.SendMessage(message)
				if !sentStatus {
					checkRetry := cons.ClientInterface.RetrySendingMessage(message)
					if !checkRetry {
						err := cons.ClientInterface.SaveToFile(message)
						if err != nil {
							//log.Error().Err(err).Msgf("Error while saving failed message to log file.")
						}
					}
				}
				C.Commit()
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	C.Close()
}

func (cons *ConsumerObject) GetLogger() *zap.Logger {
	return logger
}

func createLogger() *zap.Logger {
	rawJSON := []byte(`{
		"level": "debug",
		"encoding": "json",
		"outputPaths": ["stdout", "./logConsumer/log"],
		"errorOutputPaths": ["stderr"],
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
	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()
	return logger
}
