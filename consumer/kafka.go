package consumer

import (
	"context"
	"encoding/json"
	"github.com/LeeZXin/zsf/logger"
	"github.com/LeeZXin/zsf/mq/kafkamq"
	"github.com/LeeZXin/zsf/property/static"
	"strings"
	"zstash/model"
	"zstash/sink"
)

func InitKafka() {
	executorsNum := static.GetInt("kafka.consumer.executorsNum")
	if executorsNum <= 0 {
		logger.Logger.Panic("kafka executorsNum should greater than 0")
	}
	consumer, err := kafkamq.NewConsumer(kafkamq.Config{
		Brokers:       strings.Split(static.GetString("kafka.consumer.brokers"), ";"),
		Topic:         static.GetString("kafka.consumer.topic"),
		GroupId:       static.GetString("kafka.consumer.groupId"),
		Username:      static.GetString("kafka.consumer.username"),
		Password:      static.GetString("kafka.consumer.password"),
		SaslMechanism: static.GetString("kafka.consumer.saslMechanism"),
	})
	if err != nil {
		logger.Logger.Panic(err)
	}
	consumer.Consume(func(ctx context.Context, _ int64, msgBody []byte) error {
		var log model.LogContent
		// 不符合格式
		err := json.Unmarshal(msgBody, &log)
		if err != nil {
			return nil
		}
		sink.DoSink(log)
		return nil
	}, kafkamq.WithAutoCommit(true), kafkamq.WithExecutorsNum(executorsNum))
}
