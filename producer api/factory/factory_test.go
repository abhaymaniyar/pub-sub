package factory

import (
	"testing"
)

type TestDataItems struct {
	inputs []string
	result string
}

func TestFactoryGetMessage(t *testing.T) {
	dataItems := []TestDataItems{
		{[]string{"0", "123", "0", "foo", "message"}, ""},
		{[]string{"1", "123", "0", "foo", "message"}, "123"},
		{[]string{"2", "123", "0", "foo", "message"}, ""},
	}

	for _, item := range dataItems {
		message := GetMessage(item.inputs[0], item.inputs[1], item.inputs[2], item.inputs[3], item.inputs[4])
		if item.inputs[0] == "0" {
			if string(message.Key) == "" {
				t.Logf("GetMessage() PASSED,expected  got %v", string(message.Key))
			} else {
				t.Errorf("GetMessage() FAILED,expected \"\" got %v", string(message.Key))
			}
		} else if item.inputs[1] == "1" {
			if string(message.Key) == "123" {
				t.Logf("GetMessage() PASSED,expected \"123\" got %v", string(message.Key))
			} else {
				t.Errorf("GetMessage() FAILED,expected \"\" got %v", string(message.Key))
			}
		} else if item.inputs[1] == "2" {
			if message.TopicPartition.Partition == 0 {
				t.Logf("GetMessage() PASSED,expected 0 got %v", message.TopicPartition.Partition)
			} else {
				t.Errorf("GetMessage() FAILED,expected 0 got %v", message.TopicPartition.Partition)
			}
		}
	}
}
