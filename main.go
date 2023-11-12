package main

import (
	"github.com/LeeZXin/zsf/logger"
	"github.com/LeeZXin/zsf/property/static"
	"github.com/LeeZXin/zsf/zsf"
	"zstash/consumer"
)

func main() {
	consumerType := static.GetString("consumer.type")
	switch consumerType {
	case "kafka":
		consumer.InitKafka()
	case "nsq":
		consumer.InitNsq()
	default:
		logger.Logger.Panic("unsupported consumer type")
	}
	zsf.Run()
}
