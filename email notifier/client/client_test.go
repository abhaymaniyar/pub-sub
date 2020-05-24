package client

import (
	"fmt"
	"testing"

	"github.com/spf13/viper"
)

var cli Object

func TestSendMessage(t *testing.T) {
	if cli.getServerStatus() == cli.SendMessage("{\"request_id\":\"1\",\"topic_name\":\"foo\",\"message_body\":\"Transaction Successful\",\"transaction_id\":\"987456321\",\"email\":\"kafka@gopostman.com\",\"phone\":\"9876543210\",\"customer_id\":\"1\",\"key\":\"1254\",\"pubMessageType\":\"0\",\"pubPartition\":\"3\"}") {
		t.Logf("Send Message working fine.\n")
	} else {
		t.Errorf("Expected true from SendMessage but not successful.\n")
	}
}

func TestRetrySendingMessage(t *testing.T) {
	viper.Set("numberOfRetries", 1)
	cli.Init()
	if cli.getServerStatus() {
		fmt.Println(cli.getServerStatus())
		if cli.RetrySendingMessage("{\"request_id\":\"1\",\"topic_name\":\"foo\",\"message_body\":\"Transaction Successful\",\"transaction_id\":\"987456321\",\"email\":\"kafka@gopostman.com\",\"phone\":\"9876543210\",\"customer_id\":\"1\",\"key\":\"1254\",\"pubMessageType\":\"0\",\"pubPartition\":\"3\"}") {
			t.Logf("Retry Sending Message working fine.\n")
		} else {

			t.Errorf("Expected success from RetrySendingMessage but failed.\n")
		}
	} else {
		if cli.RetrySendingMessage("{\"request_id\":\"1\",\"topic_name\":\"foo\",\"message_body\":\"Transaction Successful\",\"transaction_id\":\"987456321\",\"email\":\"kafka@gopostman.com\",\"phone\":\"9876543210\",\"customer_id\":\"1\",\"key\":\"1254\",\"pubMessageType\":\"0\",\"pubPartition\":\"3\"}") {
			t.Errorf("Expected failure from RetrySendingMessage but succeded.\n")
		} else {
			t.Logf("Retry Sending Message working fine.\n")
		}
	}
}

func TestSaveToFile(t *testing.T) {
	err := cli.SaveToFile("{\"request_id\":\"1\",\"topic_name\":\"foo\",\"message_body\":\"Transaction Successful\",\"transaction_id\":\"987456321\",\"email\":\"kafka@gopostman.com\",\"phone\":\"9876543210\",\"customer_id\":\"1\",\"key\":\"1254\",\"pubMessageType\":\"0\",\"pubPartition\":\"3\"}")
	if err != nil {
		t.Errorf("Expected Success but got error:%v.\n", err)
	}
}
