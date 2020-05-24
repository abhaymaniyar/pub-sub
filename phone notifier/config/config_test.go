package config

import (
	"testing"

	"github.com/spf13/viper"
)

func TestConfigInit(t *testing.T) {

	Init(false)
	broker := viper.GetString("broker")
	group := viper.GetString("group")
	topic := viper.GetString("topic")
	serverRunTime := viper.GetInt("serverRunTime")
	serverDownTime := viper.GetInt("serverDownTime")
	waitTime := viper.GetInt("waitTime")

	if broker == "localhost:19092" {
		t.Logf("Init(false) PASSED, expected \"localhost:19092\" got %v", broker)
	} else {
		t.Errorf("Init(false) PASSED, expected \"localhost:19092\" got %v", broker)
	}

	if group == "my-Group" {
		t.Logf("Init(false) PASSED, expected \"my-Group\" got %v", group)
	} else {
		t.Errorf("Init(false) FAILED, expected \"my-Group\" got %v", group)
	}

	if topic == "foo" {
		t.Logf("Init(false) PASSED, expected \"foo\" got %v", topic)
	} else {
		t.Errorf("Init(false) FAILED, expected \"foo\" got %v", topic)
	}

	if serverRunTime == 120 {
		t.Logf("Init(false) PASSED, expected 120 got %v", serverRunTime)
	} else {
		t.Errorf("Init(false) FAILED, expected 120 got %v", serverRunTime)
	}

	if serverDownTime == 30 {
		t.Logf("Init(false) PASSED, expected 30 got %v", serverDownTime)
	} else {
		t.Errorf("Init(false) FAILED, expected 30 got %v", serverDownTime)
	}

	if waitTime == 10 {
		t.Logf("Init(false) PASSED, expected 10 got %v", waitTime)
	} else {
		t.Errorf("Init(false) FAILED, expected 10 got %v", waitTime)
	}

}
